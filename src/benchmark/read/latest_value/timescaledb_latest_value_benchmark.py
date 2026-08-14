from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.timescaledb.config import get_timescaledb_config
from src.databases.timescaledb.timescaledb_repo import TimescaleDBRepo
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import ReadLatestBenchmarkResult

lines = "-" * 50
print(lines)

PARAMETERS = [
    "sea_water_temperature",
    "sea_water_electrical_conductivity",
    "sea_water_salinity",
    "dissolved_oxygen",
    "turbidity",
]

NUMBER_OF_REPEATS = 10

# SETUP TIMESCALEDB CLIENT (readbench instance)
readbench_config = load_config("./configs/config-readbench-timescaledb.yml")
logger = get_logger("timescaledb_latest_value_benchmark", readbench_config["general"]["log_file"])
timescaledb_repo = TimescaleDBRepo(get_timescaledb_config(readbench_config["database"]), logger)

if timescaledb_repo.ping():
    print("Readbench TimescaleDB connection successful.")
print(lines)

# SETUP BENCHMARK DB CLIENT
benchmarkdb_config_dict = load_config("./configs/config-benchmarkdb.yml")
bm_logger = get_logger("benchmark_db", benchmarkdb_config_dict["general"]["log_file"])
benchmarkdb_repo = BenchmarkDB(get_postgres_config(benchmarkdb_config_dict["database"]), bm_logger)

benchmarkdb_repo.connect()
benchmarkdb_repo.create_tables()
print("BenchmarkDB is connected and ready.")
print(lines)

# RUN LATEST VALUE QUERY FOR EACH PARAMETER
for parameter in PARAMETERS:
    for run_num in range(1, NUMBER_OF_REPEATS + 1):
        obs, elapsed_ns = timescaledb_repo.query_latest(parameter)
        elapsed_seconds = elapsed_ns / 1_000_000_000
        result_found = obs is not None

        print(f"[{parameter}] run {run_num}: found={result_found} elapsed={elapsed_ns} ns ({elapsed_seconds:.6f} s)")

        result = ReadLatestBenchmarkResult(
            benchmark_name="read.latest_value",
            database_system="timescaledb",
            database_version="latest-pg17",
            parameter=parameter,
            result_found=result_found,
            elapsed_time_seconds=elapsed_seconds,
            elapsed_time_ns=elapsed_ns,
        )
        result = benchmarkdb_repo.insert_result(result)
        print(f"  -> saved as id={result.id}")

    print(lines)

benchmarkdb_repo.disconnect()

print("TimescaleDB latest-value benchmark complete.")
timescaledb_repo.close()