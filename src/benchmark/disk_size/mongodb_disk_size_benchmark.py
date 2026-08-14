import json
import subprocess
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import DiskSizeBenchmarkResult

lines = "-" * 50
print(lines)

MONGODB_DATA_DIR = Path("data/mongodb_data")
DATASET_SIZES_CSV = Path("data/row_data/dataset_sizes.csv")
DISKTEST_CONTAINER_NAME = "timeseries-thesis-disk_mongodb-1"
DISKTEST_DATA_PATH_IN_CONTAINER = "/data/db"


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


def get_container_folder_size_bytes(container_name: str, path_in_container: str) -> int:
    result = subprocess.run(
        ["docker", "exec", container_name, "du", "-sb", path_in_container],
        capture_output=True, text=True, check=True,
    )
    return int(result.stdout.split()[0])


sizes = load_dataset_sizes(DATASET_SIZES_CSV)
files = load_mongodb_files(MONGODB_DATA_DIR)
print(f"Loaded {len(files)} dataset files")
print(lines)

# Connect to the DISKTEST mongodb instance
disktest_config = load_config("./configs/config-disktest-mongodb.yml")
logger = get_logger("mongodb_disk_size_benchmark", disktest_config["general"]["log_file"])
mongodb_config = get_mongodb_config(disktest_config["database"])
mongodb_repo = MongoDBRepository(mongodb_config, logger)

mongodb_repo.connect_and_cache()
if mongodb_repo.ping():
    print("Disktest MongoDB connection successful.")
    mongodb_repo.create_db_and_collection()
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


def force_flush():
    """fsync forces MongoDB to flush pending writes to disk before measuring."""
    try:
        mongodb_repo.client.admin.command("fsync", lock=False)
    except Exception as e:
        logger.warning("fsync failed: %s", e)


# Baseline measurement (empty DB, 0 records)
force_flush()
baseline_size = get_container_folder_size_bytes(DISKTEST_CONTAINER_NAME, DISKTEST_DATA_PATH_IN_CONTAINER)
print(f"Baseline folder size (0 records): {baseline_size} bytes")

benchmarkdb_repo.connect()
baseline_result = DiskSizeBenchmarkResult(
    benchmark_name="disk_size.per_batch_delta",
    database_system="mongodb",
    database_version="8.0",
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

for batch_id, docs in files:
    dataset_meta = sizes[batch_id]
    print(f"Batch {batch_id} ({dataset_meta.record_count} obs)...")

    size_before = get_container_folder_size_bytes(DISKTEST_CONTAINER_NAME, DISKTEST_DATA_PATH_IN_CONTAINER)

    docs_len = len(docs)
    start = 0
    while start < docs_len:
        end = min(start + BATCH_SIZE, docs_len)
        chunk = docs[start:end]
        mongodb_repo.insert_many(chunk)
        start = end

    force_flush()

    size_after = get_container_folder_size_bytes(DISKTEST_CONTAINER_NAME, DISKTEST_DATA_PATH_IN_CONTAINER)
    size_delta = size_after - size_before

    cumulative_record_count += dataset_meta.record_count

    print(f"  before={size_before} after={size_after} delta={size_delta} bytes for {dataset_meta.record_count} obs")

    benchmarkdb_repo.connect()
    result = DiskSizeBenchmarkResult(
        benchmark_name="disk_size.per_batch_delta",
        database_system="mongodb",
        database_version="8.0",
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

# clear the collection after the benchmark, matching TimescaleDB's pattern
try:
    mongodb_repo.delete_by_query({})
    print("Collection cleared after benchmark.")
except Exception as e:
    logger.warning("Final clear failed: %s", e)

print("MongoDB disk size benchmark complete.")
mongodb_repo.disconnect()