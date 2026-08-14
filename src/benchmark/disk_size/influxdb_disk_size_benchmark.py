import subprocess
import time
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.influxdb.config import get_influxdb_config
from src.databases.influxdb.influx_repo import InfluxRepo
from src.databases.influxdb.models.observation import Observation
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import DiskSizeBenchmarkResult

lines = "-" * 50
print(lines)

INFLUXDB_DATA_DIR = Path("data/influxdb_data")
DATASET_SIZES_CSV = Path("data/row_data/dataset_sizes.csv")
DISKTEST_CONTAINER_NAME = "timeseries-thesis-disk_influxdb-1"
DISKTEST_DATA_PATH_IN_CONTAINER = "/var/lib/influxdb2"

# InfluxDB's TSM engine compacts asynchronously — no synchronous flush command exists.
# This wait is a compromise, not a guarantee like CHECKPOINT/fsync give the other two DBs.
FLUSH_WAIT_SECONDS = 5


@dataclass
class DatasetMeta:
    dataset: str
    batch_id: int
    record_count: int


def load_dataset_sizes(csv_path: Path) -> dict[int, DatasetMeta]:
    df = pd.read_csv(csv_path)
    return {
        row.batch_id: DatasetMeta(dataset=row.dataset, batch_id=row.batch_id, record_count=row.record_count)
        for row in df.itertuples()
    }


def load_influxdb_files(data_dir: Path) -> list[tuple[int, list[Observation]]]:
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


def get_container_folder_size_bytes(container_name: str, path_in_container: str) -> int:
    result = subprocess.run(
        ["docker", "exec", container_name, "du", "-sb", path_in_container],
        capture_output=True, text=True, check=True,
    )
    return int(result.stdout.split()[0])


sizes = load_dataset_sizes(DATASET_SIZES_CSV)
files = load_influxdb_files(INFLUXDB_DATA_DIR)
print(f"Loaded {len(files)} dataset files")
print(lines)

# Connect to the DISKTEST influxdb instance
disktest_config = load_config("./configs/config-disktest-influxdb.yml")
logger = get_logger("influxdb_disk_size_benchmark", disktest_config["general"]["log_file"])
influxdb_repo = InfluxRepo(get_influxdb_config(disktest_config["database"]), logger)

if influxdb_repo.ping():
    print("Disktest InfluxDB connection successful.")
print(lines)

# Connect to your regular benchmark_db to store results
benchmarkdb_config_dict = load_config("./configs/config-benchmarkdb.yml")
bm_logger = get_logger("benchmark_db", benchmarkdb_config_dict["general"]["log_file"])
benchmarkdb_repo = BenchmarkDB(get_postgres_config(benchmarkdb_config_dict["database"]), bm_logger)

try:
    benchmarkdb_repo.connect()
    benchmarkdb_repo.create_tables()
    print("BenchmarkDB is connected and ready.\n")
finally:
    benchmarkdb_repo.disconnect()

print(lines)

# Baseline measurement (empty DB, 0 records)
time.sleep(FLUSH_WAIT_SECONDS)
baseline_size = get_container_folder_size_bytes(DISKTEST_CONTAINER_NAME, DISKTEST_DATA_PATH_IN_CONTAINER)
print(f"Baseline folder size (0 records): {baseline_size} bytes")

benchmarkdb_repo.connect()
baseline_result = DiskSizeBenchmarkResult(
    benchmark_name="disk_size.per_batch_delta",
    database_system="influxdb",
    database_version="2.7",
    batch_id=0,
    dataset_name="baseline_empty",
    record_count=0,
    cumulative_record_count=0,
    disk_size_before_bytes=baseline_size,
    disk_size_after_bytes=baseline_size,
    disk_size_delta_bytes=0,
)
benchmarkdb_repo.insert_result(baseline_result)
benchmarkdb_repo.disconnect()
print(lines)

# Cumulative insert + measure loop
cumulative_record_count = 0
BATCH_SIZE = 500

for batch_id, observations in files:
    dataset_meta = sizes[batch_id]
    print(f"Batch {batch_id} ({dataset_meta.record_count} obs)...")

    size_before = get_container_folder_size_bytes(DISKTEST_CONTAINER_NAME, DISKTEST_DATA_PATH_IN_CONTAINER)

    obs_len = len(observations)
    start = 0
    while start < obs_len:
        end = min(start + BATCH_SIZE, obs_len)
        chunk = observations[start:end]
        influxdb_repo.insert_many(chunk)
        start = end

    time.sleep(FLUSH_WAIT_SECONDS)

    size_after = get_container_folder_size_bytes(DISKTEST_CONTAINER_NAME, DISKTEST_DATA_PATH_IN_CONTAINER)
    size_delta = size_after - size_before

    cumulative_record_count += dataset_meta.record_count

    print(f"  before={size_before} after={size_after} delta={size_delta} bytes for {dataset_meta.record_count} obs")

    benchmarkdb_repo.connect()
    result = DiskSizeBenchmarkResult(
        benchmark_name="disk_size.per_batch_delta",
        database_system="influxdb",
        database_version="2.7",
        batch_id=batch_id,
        dataset_name=dataset_meta.dataset,
        record_count=dataset_meta.record_count,
        cumulative_record_count=cumulative_record_count,
        disk_size_before_bytes=size_before,
        disk_size_after_bytes=size_after,
        disk_size_delta_bytes=size_delta,
    )
    benchmarkdb_repo.insert_result(result)
    benchmarkdb_repo.disconnect()
    print(lines)

# clear all data after the benchmark, matching TimescaleDB/MongoDB pattern
try:
    influxdb_repo.delete_all()
    print("Bucket cleared after benchmark.")
except Exception as e:
    logger.warning("Final clear failed: %s", e)

print("InfluxDB disk size benchmark complete.")
influxdb_repo.close()