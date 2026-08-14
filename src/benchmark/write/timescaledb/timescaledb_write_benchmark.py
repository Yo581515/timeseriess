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

lines = "-" * 50
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
    files = sorted(data_dir.glob("*.csv"), key=lambda f: int(f.stem.split("_")[3]))
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


sizes = load_dataset_sizes(DATASET_SIZES_CSV)
files = load_timescaledb_files(TIMESCALEDB_DATA_DIR)

print(f"Loaded {len(files)} dataset files")
print(f"Loaded {len(sizes)} dataset metadata entries")
print(lines)

timescale_config = load_config("./configs/config-timescaledb.yml")
logger = get_logger("timescaledb_write_benchmark", timescale_config["general"]["log_file"])
timescaledb_repo = TimescaleDBRepo(get_timescaledb_config(timescale_config["database"]), logger)

if timescaledb_repo.ping():
    print("TimescaleDB connection successful.")
    timescaledb_repo.delete_all()
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

BATCH_SIZES = [100, 250, 500, 1000]
number_of_full_runs = 10

try:
    st = time.perf_counter()
    benchmarkdb_repo.connect()

    for BATCH_SIZE in BATCH_SIZES:
        print(f"=== BATCH_SIZE = {BATCH_SIZE} ===")
        print(lines)

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

                try:
                    warmup_chunk = observations[:chunk_size]
                    timescaledb_repo.insert_many(warmup_chunk)
                    timescaledb_repo.insert_many(warmup_chunk)
                    timescaledb_repo.insert_many(warmup_chunk)
                    timescaledb_repo.delete_all()
                except Exception as e:
                    logger.error("Warmup failed: %s", e)
                    raise Exception("Warmup failed") from e

                start_batch_index = int(0)
                end_batch_index = int(obs_len if obs_len <= chunk_size else chunk_size)

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

                try:
                    total_insert_time_seconds = total_insert_time_ns / 1_000_000_000
                    throughput_obs_per_sec = dataset_meta.record_count / total_insert_time_seconds
                    throughput_bytes_per_sec = dataset_meta.data_size_bytes / total_insert_time_seconds
                    throughput_kb_per_sec = dataset_meta.data_size_kb / total_insert_time_seconds
                    throughput_mb_per_sec = dataset_meta.data_size_mb / total_insert_time_seconds

                    result = IngestionBenchmarkResult(
                        benchmark_name="ingestion.insert_many",
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

                    result = benchmarkdb_repo.insert_result(result)
                    print("Inserted IngestionBenchmarkResult:", result.id)
                    print()
                except Exception as e:
                    logger.error("Failed to save benchmark result: %s", e)
                    raise Exception("Save benchmark result failed") from e

    er = time.perf_counter()
    tt = er - st
    print(lines)
    print(f"Total time for batch-size sweep: {tt} seconds or {tt/60} minutes")

finally:
    timescaledb_repo.close()
    benchmarkdb_repo.disconnect()
    