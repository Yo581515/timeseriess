from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.influxdb.config import get_influxdb_config
from src.databases.influxdb.influx_repo import InfluxRepo
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

# SETUP INFLUXDB CLIENT (readbench instance)
readbench_config = load_config("./configs/config-readbench-influxdb.yml")
logger = get_logger("influxdb_latest_value_benchmark", readbench_config["general"]["log_file"])
influxdb_repo = InfluxRepo(get_influxdb_config(readbench_config["database"]), logger)

if influxdb_repo.ping():
    print("Readbench InfluxDB connection successful.")
print(lines)

# SETUP BENCHMARK DB CLIENT
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

NUMBER_OF_REPEATS = 10

for parameter in PARAMETERS:
    for run_num in range(1, NUMBER_OF_REPEATS + 1):
        obs, elapsed_ns = influxdb_repo.query_latest(parameter)
        elapsed_seconds = elapsed_ns / 1_000_000_000
        result_found = obs is not None

        print(f"[{parameter}] run {run_num}: found={result_found} elapsed={elapsed_ns} ns ({elapsed_seconds:.6f} s)")

        benchmarkdb_repo.connect()
        result = ReadLatestBenchmarkResult(
            benchmark_name="read.latest_value",
            database_system="influxdb",
            database_version="2.7",
            parameter=parameter,
            result_found=result_found,
            elapsed_time_seconds=elapsed_seconds,
            elapsed_time_ns=elapsed_ns,
        )
        benchmarkdb_repo.insert_result(result)
        benchmarkdb_repo.disconnect()

    print(lines)

print("InfluxDB latest-value benchmark complete.")
influxdb_repo.close()