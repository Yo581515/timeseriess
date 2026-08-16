from datetime import datetime, timezone

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import ReadBucketedBenchmarkResult

lines = "-" * 50
print(lines)

PARAMETERS = [
    "sea_water_temperature",
    "sea_water_electrical_conductivity",
    "sea_water_salinity",
    "dissolved_oxygen",
    "turbidity",
]

RANGE_WINDOW = {
    "name": "1_year",
    "start": datetime(2023, 1, 1, tzinfo=timezone.utc),
    "end": datetime(2024, 1, 1, tzinfo=timezone.utc),
}

BUCKET_INTERVALS = {
    "1_minute": ("minute", 1),
    "15_minutes": ("minute", 15),
    "30_minutes": ("minute", 30),
    "hourly": ("hour", 1),
    "daily": ("day", 1),
    "weekly": ("week", 1),
}

NUMBER_OF_REPEATS = 3

readbench_config = load_config("./configs/config-readbench-mongodb.yml")
logger = get_logger("mongodb_timebucket_benchmark", readbench_config["general"]["log_file"])
mongodb_config = get_mongodb_config(readbench_config["database"])
mongodb_repo = MongoDBRepository(mongodb_config, logger)

mongodb_repo.connect_and_cache()
if mongodb_repo.ping():
    print("Readbench MongoDB connection successful.")
print(lines)

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

benchmarkdb_repo.connect()

for parameter in PARAMETERS:
    for bucket_name, bucket_interval in BUCKET_INTERVALS.items():
        for run_num in range(1, NUMBER_OF_REPEATS + 1):
            buckets, elapsed_ns = mongodb_repo.query_bucketed(
                parameter, RANGE_WINDOW["start"], RANGE_WINDOW["end"], bucket_interval
            )
            elapsed_seconds = elapsed_ns / 1_000_000_000
            bucket_count = len(buckets)

            print(f"[{parameter}][{bucket_name}] run {run_num}: buckets={bucket_count} elapsed={elapsed_ns} ns ({elapsed_seconds:.6f} s)")

            result = ReadBucketedBenchmarkResult(
                benchmark_name="read.timebucket_aggregation",
                database_system="mongodb",
                database_version="8.0",
                parameter=parameter,
                bucket_interval=bucket_name,
                range_window=RANGE_WINDOW["name"],
                start_time=RANGE_WINDOW["start"],
                end_time=RANGE_WINDOW["end"],
                bucket_count=bucket_count,
                elapsed_time_seconds=elapsed_seconds,
                elapsed_time_ns=elapsed_ns,
            )
            benchmarkdb_repo.insert_result(result)

        print(lines)

benchmarkdb_repo.disconnect()

print("MongoDB time-bucket benchmark complete.")
mongodb_repo.disconnect()