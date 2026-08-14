import math
import time
import json
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import IngestionBenchmarkResult

lines = "-" * 50
print(lines)

MONGODB_DATA_DIR = Path("data/mongodb_data")
DATASET_SIZES_CSV = Path("data/row_data/dataset_sizes.csv")


@dataclass
class DatasetMeta:
    dataset: str
    batch_id: int
    record_count: int
    data_size_bytes: int
    data_size_kb: float
    data_size_mb: float


def load_dataset_sizes(csv_path: Path) -> dict[int, DatasetMeta]:
    df = pd.read_csv(csv_path)
    return {
        row.batch_id: DatasetMeta(
            dataset=row.dataset,
            batch_id=row.batch_id,
            record_count=row.record_count,
            data_size_bytes=row.json_data_bytes,
            data_size_kb=row.json_data_kb,
            data_size_mb=row.json_data_mb,
        )
        for row in df.itertuples()
    }


def parse_doc_time(doc: dict) -> dict:
    doc["time"] = datetime.fromisoformat(doc["time"].replace("Z", "+00:00"))
    return doc


def load_mongodb_files(data_dir: Path) -> list[tuple[int, list[dict]]]:
    files = sorted(data_dir.glob("*.json"), key=lambda f: int(f.stem.split("_")[3]))
    result = []
    for file in files:
        batch_id = int(file.stem.split("_")[3])
        with open(file) as f:
            docs = [parse_doc_time(doc) for doc in json.load(f)]
        result.append((batch_id, docs))
    return result


sizes = load_dataset_sizes(DATASET_SIZES_CSV)
files = load_mongodb_files(MONGODB_DATA_DIR)

print(f"Loaded {len(files)} dataset files")
print(lines)

mongodb_config_file_path = "./configs/config-mgdb.yml"
mongodb_config_dict = load_config(mongodb_config_file_path)
logger = get_logger("mongodb_write_benchmark", mongodb_config_dict["general"]["log_file"])
mongodb_config = get_mongodb_config(mongodb_config_dict["database"])
mongodb_repo = MongoDBRepository(mongodb_config, logger)

mongodb_repo.connect_and_cache()
if mongodb_repo.ping():
    print("MongoDB connection successful.")
    mongodb_repo.create_db_and_collection()
    mongodb_repo.delete_by_query({})
print(lines)

benchmarkdb_config_dict = load_config("./configs/config-benchmarkdb.yml")
bm_logger = get_logger("benchmark_db", benchmarkdb_config_dict["general"]["log_file"])
benchmarkdb_repo = BenchmarkDB(get_postgres_config(benchmarkdb_config_dict["database"]), bm_logger)

try:
    benchmarkdb_repo.connect()
    benchmarkdb_repo.create_tables()
    benchmarkdb_repo.ping()
    print("BenchmarkDB is connected and ready.\n")
except Exception as e:
    logger.error("BenchmarkDB setup failed: %s", e)
finally:
    benchmarkdb_repo.disconnect()

print(lines)

# BATCH_SIZE here is doc batch size (docs * 5 = obs-equivalent, matching Timescale/Influx units)
BATCH_SIZES = [20, 50, 100, 200]  # obs-equivalent: [100, 250, 500, 1000]
number_of_full_runs = 10

try:
    st = time.perf_counter()

    for BATCH_SIZE in BATCH_SIZES:
        print(f"=== BATCH_SIZE (docs) = {BATCH_SIZE} (obs equivalent = {BATCH_SIZE * 5}) ===")
        print(lines)

        for i in range(number_of_full_runs):
            for batch_id, docs in files:
                print("batch_id is ", batch_id)

                dataset_meta = sizes[batch_id]
                print("Dataset metadata:", dataset_meta)

                docs_len = len(docs)
                start_batch_index = int(0)
                end_batch_index = int(docs_len if docs_len <= BATCH_SIZE else BATCH_SIZE)
                num_iterations = math.ceil(docs_len / end_batch_index)
                print(num_iterations)

                total_insert_time_ns = 0

                try:
                    docs_seg = [doc for doc in docs[start_batch_index:end_batch_index]]
                    for doc in docs_seg:
                        doc.pop("_id", None)
                        doc["time"] = doc["time"] - timedelta(days=365 * 5)
                    mongodb_repo.insert_many(docs_seg)
                    for doc in docs_seg:
                        doc.pop("_id", None)
                        doc["time"] = doc["time"] + timedelta(days=365 * 6)
                    mongodb_repo.insert_many(docs_seg)
                    for doc in docs_seg:
                        doc.pop("_id", None)
                        doc["time"] = doc["time"] + timedelta(days=365 * 7)
                    mongodb_repo.insert_many(docs_seg)
                except Exception as e:
                    logger.error("An error occurred during initial caching of docs: %s", e)
                    raise Exception("Initial caching of docs failed") from e
                finally:
                    mongodb_repo.delete_by_query({})

                for run_num in range(1, num_iterations + 1):
                    docs_seg = docs[start_batch_index:end_batch_index]
                    for doc in docs_seg:
                        doc.pop("_id", None)
                    try:
                        inserted, elapsed_time_ns = mongodb_repo.insert_many(docs_seg)
                        start_batch_index = end_batch_index
                        end_batch_index = int(end_batch_index + BATCH_SIZE if end_batch_index + BATCH_SIZE <= docs_len else docs_len)
                        total_insert_time_ns += elapsed_time_ns
                    except Exception as e:
                        logger.error("An error occurred during insert_many: %s", e)
                        inserted = False
                        raise Exception("Insert operation failed") from e
                    if inserted:
                        print(f"Run {run_num}: Inserted {len(docs_seg)} docs in {elapsed_time_ns} ns")

                try:
                    mongodb_repo.delete_by_query({})
                except Exception as e:
                    logger.error("An error occurred while clearing MongoDB collection: %s", e)
                    raise Exception("Failed to clear MongoDB collection") from e

                print(f"Total insert time for batch {batch_id}: {total_insert_time_ns} ns or {total_insert_time_ns / 1_000_000_000} seconds")
                print(lines)

                try:
                    total_insert_time_seconds = total_insert_time_ns / 1_000_000_000
                    throughput_obs_per_sec = dataset_meta.record_count / total_insert_time_seconds
                    throughput_bytes_per_sec = dataset_meta.data_size_bytes / total_insert_time_seconds
                    throughput_kb_per_sec = dataset_meta.data_size_kb / total_insert_time_seconds
                    throughput_mb_per_sec = dataset_meta.data_size_mb / total_insert_time_seconds

                    result = IngestionBenchmarkResult(
                        benchmark_name="ingestion.insert_many",
                        database_system="mongodb",
                        database_version="8.0",
                        database_location="container",
                        insert_batch_size=BATCH_SIZE * 5,
                        batch_id=batch_id,
                        dataset_name=dataset_meta.dataset,
                        record_count=dataset_meta.record_count,
                        data_size_bytes=dataset_meta.data_size_bytes,
                        data_size_kb=dataset_meta.data_size_kb,
                        data_size_mb=dataset_meta.data_size_mb,
                        elapsed_time_seconds=total_insert_time_seconds,
                        elapsed_time_ns=total_insert_time_ns,
                        throughput_obs_per_sec=throughput_obs_per_sec,
                        throughput_bytes_per_sec=throughput_bytes_per_sec,
                        throughput_kb_per_sec=throughput_kb_per_sec,
                        throughput_mb_per_sec=throughput_mb_per_sec,
                        insert_operation_count=num_iterations,
                    )

                    result = benchmarkdb_repo.insert_result(result)
                    print("Inserted IngestionBenchmarkResult:", result.id)
                    print()
                except Exception as e:
                    logger.error("An error occurred while saving benchmark results: %s", e)

    er = time.perf_counter()
    tt = er - st
    print(lines)
    print(f"Total time for batch-size sweep: {tt} seconds or {tt/60} minutes")

except Exception as e:
    logger.error("An error occurred during the benchmark: %s", e)
    raise Exception("Benchmark execution failed") from e
finally:
    mongodb_repo.disconnect()
    benchmarkdb_repo.disconnect()