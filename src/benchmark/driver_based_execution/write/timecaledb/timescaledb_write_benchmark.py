import math
import time
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.timescaledb.config import get_timescaledb_config
from src.databases.timescaledb.timescaledb_repo import TimescaleDBRepo
from src.databases.timescaledb.models.observation import Observation
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import IngestionBenchmarkResult

lines = ""
for i in range(50):
    lines += "-"
print(lines)


TIMESCALEDB_DATA_DIR = Path("data/timescaledb_data")
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


def load_timescaledb_files(data_dir: Path) -> list[tuple[int, list[Observation]]]:
    files = sorted(
        data_dir.glob("*.csv"),
        key=lambda f: int(f.stem.split("_")[3])
    )
    result = []
    for file in files:
        batch_id = int(file.stem.split("_")[3])
        df = pd.read_csv(file)
        observations = [
            Observation(
                time=datetime.fromisoformat(row.time),
                node_source=row.node_source,
                node_source_id=row.node_source_id,
                latitude=float(row.latitude),
                longitude=float(row.longitude),
                sensor_source=row.sensor_source,
                sensor_source_id=row.sensor_source_id,
                parameter=row.parameter,
                value=float(row.value),
                unit=row.unit,
                quality_codes=[int(row.quality_codes)],
            )
            for row in df.itertuples()
        ]
        result.append((batch_id, observations))
    return result


# TODO 1: LOAD DATA
# - get all TimescaleDB data files from data/timescaledb_data/ (csv files)
# - Parse each csv into a list of Observation objects
# - Load dataset metadata from dataset_sizes.csv (data_size_bytes, kb, mb, record_count)

sizes = load_dataset_sizes(DATASET_SIZES_CSV)
files = load_timescaledb_files(TIMESCALEDB_DATA_DIR)

print(f"Loaded {len(files)} dataset files")
print(f"Loaded {len(sizes)} dataset metadata entries")
print(lines)


# TODO 2: SETUP TIMESCALEDB CLIENT
# - Load config from configs/config-timescaledb.yml
# - Instantiate TimescaleDBRepo
# - Ping to verify connection

timescale_config = load_config("./configs/config-timescaledb.yml")
logger = get_logger("timescaledb_write_benchmark", timescale_config["general"]["log_file"])
timescaledb_repo = TimescaleDBRepo(get_timescaledb_config(timescale_config["database"]), logger)

if timescaledb_repo.ping():
    print("TimescaleDB connection successful.")
print(lines)


# TODO 3: SETUP BENCHMARK DB CLIENT
# - Load config from configs/config-benchmarkdb.yml
# - Instantiate BenchmarkDB
# - Call connect() + create_tables()
# - Ping to verify connection

bmdb_config = load_config("./configs/config-benchmarkdb.yml")
bm_logger = get_logger("benchmark_db", bmdb_config["general"]["log_file"])
bm_db = BenchmarkDB(get_postgres_config(bmdb_config["database"]), bm_logger)

try:
    bm_db.connect()
    bm_db.create_tables()
    bm_db.ping()
    print("BenchmarkDB is connected and ready.\n")
except Exception as e:
    logger.error("BenchmarkDB setup failed: %s", e)
finally:
    bm_db.disconnect()

print(lines)


# TODO 4: RUN INGESTION BENCHMARK
# For each file (batch_1 → batch_len(files)):
#   - Get matching metadata from dataset_sizes (batch_id, record_count, bytes, kb, mb)
#   - Convert insert_batch_size (100 obs) into chunk_size
#   - For run_number in range(1, number_of_full_runs + 1):
#       - Warmup: insert a few chunks to warm up connection and caches
#       - Clear the observations table (fresh start for each run)
#       - Start timer (time.perf_counter_ns())
#       - Loop: chunk observations into chunk_size groups, call insert_many() per chunk
#         keeping count of insert_operation_count
#       - Stop timer
#       - Calculate metrics:
#           elapsed_time_seconds
#           throughput_obs_per_sec   = record_count / elapsed
#           throughput_bytes_per_sec = data_size_bytes / elapsed
#           throughput_kb_per_sec    = data_size_kb / elapsed
#           throughput_mb_per_sec    = data_size_mb / elapsed

BATCH_SIZE = 100

# 25 tar 33 minutter
number_of_full_runs = 25

try:
    
    st = time.perf_counter()
    bm_db.connect()
    for i in range(number_of_full_runs):
        for batch_id, observations in files:
            print("batch_id is ", batch_id)

            dataset_meta = sizes[batch_id]
            print("Dataset metadata:", dataset_meta)

            obs_len = len(observations)
            chunk_size = int(BATCH_SIZE)
            num_iterations = math.ceil(obs_len / chunk_size)
            print(num_iterations)

            total_insert_time_ns = 0

            # warmup
            try:
                warmup_chunk = observations[:chunk_size]
                timescaledb_repo.insert_many(warmup_chunk)
                timescaledb_repo.insert_many(warmup_chunk)
                timescaledb_repo.insert_many(warmup_chunk)
            except Exception as e:
                logger.error("Warmup failed: %s", e)
                raise Exception("Warmup failed") from e

            start_batch_index = int(0)
            end_batch_index = int(obs_len if obs_len <= chunk_size else chunk_size)

            elapsed_time_ns = 0
            for run_num in range(1, num_iterations + 1):
                obs_seg = observations[start_batch_index:end_batch_index]
                try:
                    elapsed_time_ns = timescaledb_repo.insert_many(obs_seg, page_size=BATCH_SIZE)
                    start_batch_index = end_batch_index
                    end_batch_index = int(end_batch_index + chunk_size if end_batch_index + chunk_size <= obs_len else obs_len)
                    total_insert_time_ns += elapsed_time_ns
                    print(f"Run {run_num}: Inserted {len(obs_seg)} obs in {elapsed_time_ns} ns")
                except Exception as e:
                    logger.error("insert_many failed on run %d: %s", run_num, e)
                    raise Exception("Insert operation failed") from e

            try:
                timescaledb_repo.delete_all()
            except Exception as e:
                logger.error("Failed to clear observations table: %s", e)
                raise Exception("Clear failed") from e

            print(f"Total insert time for batch {batch_id}: {total_insert_time_ns} ns or {total_insert_time_ns / 1_000_000_000} seconds")
            print(lines)

            # TODO 5: SAVE BENCHMARK RESULTS
            #   - Build IngestionBenchmarkResult object with all metrics
            #   - Call bm_db.insert_result(result)

            try:
                total_insert_time_seconds = total_insert_time_ns / 1_000_000_000
                throughput_obs_per_sec = dataset_meta.record_count / total_insert_time_seconds
                throughput_bytes_per_sec = dataset_meta.data_size_bytes / total_insert_time_seconds
                throughput_kb_per_sec = dataset_meta.data_size_kb / total_insert_time_seconds
                throughput_mb_per_sec = dataset_meta.data_size_mb / total_insert_time_seconds

                result = IngestionBenchmarkResult(
                    benchmark_name="ingestion.insert_many.fixed_batch",
                    database_system="timescaledb",
                    database_version="latest-pg17",
                    database_location="container",
                    insert_batch_size=BATCH_SIZE,
                    
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

                
                result = bm_db.insert_result(result)
                print("Inserted IngestionBenchmarkResult:", result.id)
                print()
            except Exception as e:
                logger.error("Failed to save benchmark result: %s", e)
                raise Exception("Save benchmark result failed") from e

    er = time.perf_counter()
    tt = er - st
    print(lines)
    print(f"Total time for inserting {number_of_full_runs} full data runs: {tt} seconds or {tt/60} minutes")

# TODO 6: TEARDOWN
# - Disconnect TimescaleDBRepo
# - Disconnect BenchmarkDB

finally:
    timescaledb_repo.close()
    bm_db.disconnect()