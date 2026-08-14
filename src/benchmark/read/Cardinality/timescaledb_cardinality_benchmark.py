from datetime import datetime, timezone

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.timescaledb.config import get_timescaledb_config
from src.databases.timescaledb.timescaledb_repo import TimescaleDBRepo
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import ReadCardinalityBenchmarkResult

lines = "-" * 50
print(lines)

# Fixed 1-month window — same range used across all three databases
RANGE_START = datetime(2020, 1, 1, tzinfo=timezone.utc)
RANGE_END = datetime(2020, 1, 31, tzinfo=timezone.utc)
RANGE_WINDOW_NAME = "1_month"

TARGET_PARAMETER = "sea_water_temperature"
TARGET_NODE = "sfi_smart_ocean;demo;d1;1"

CARDINALITY_LEVELS = [
    {"level": "single_sensor", "parameter": TARGET_PARAMETER, "node_source_id": TARGET_NODE},
    {"level": "single_parameter_all_nodes", "parameter": TARGET_PARAMETER, "node_source_id": None},
    {"level": "all", "parameter": None, "node_source_id": None},
]

NUMBER_OF_REPEATS = 3

readbench_config = load_config("./configs/config-readbench-timescaledb.yml")
logger = get_logger("timescaledb_cardinality_benchmark", readbench_config["general"]["log_file"])
timescaledb_repo = TimescaleDBRepo(get_timescaledb_config(readbench_config["database"]), logger)

if timescaledb_repo.ping():
    print("Readbench TimescaleDB connection successful.")
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

for cfg in CARDINALITY_LEVELS:
    for run_num in range(1, NUMBER_OF_REPEATS + 1):
        observations, elapsed_ns = timescaledb_repo.query_cardinality(
            start_time=RANGE_START,
            end_time=RANGE_END,
            parameter=cfg["parameter"],
            node_source_id=cfg["node_source_id"],
        )
        elapsed_seconds = elapsed_ns / 1_000_000_000
        row_count = len(observations)

        print(f"[{cfg['level']}] run {run_num}: rows={row_count} elapsed={elapsed_ns} ns ({elapsed_seconds:.6f} s)")

        result = ReadCardinalityBenchmarkResult(
            benchmark_name="read.cardinality",
            database_system="timescaledb",
            database_version="latest-pg17",
            cardinality_level=cfg["level"],
            parameter=cfg["parameter"],
            node_source_id=cfg["node_source_id"],
            range_window=RANGE_WINDOW_NAME,
            start_time=RANGE_START,
            end_time=RANGE_END,
            result_row_count=row_count,
            elapsed_time_seconds=elapsed_seconds,
            elapsed_time_ns=elapsed_ns,
        )
        benchmarkdb_repo.insert_result(result)

    print(lines)

benchmarkdb_repo.disconnect()

print("TimescaleDB cardinality benchmark complete.")
timescaledb_repo.close()