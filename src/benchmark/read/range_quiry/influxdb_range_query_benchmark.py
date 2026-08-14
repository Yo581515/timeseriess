from datetime import datetime, timezone

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.influxdb.config import get_influxdb_config
from src.databases.influxdb.influx_repo import InfluxRepo
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB
from src.databases.benchmark_db.models import ReadRangeBenchmarkResult

lines = "-" * 50
print(lines)

PARAMETERS = [
    "sea_water_temperature",
    "sea_water_electrical_conductivity",
    "sea_water_salinity",
    "dissolved_oxygen",
    "turbidity",
]

RANGE_WINDOWS = {
    "1_day": {
        "start": datetime(2015, 3, 10, tzinfo=timezone.utc),
        "end":   datetime(2015, 3, 11, tzinfo=timezone.utc),
    },
    "1_week": {
        "start": datetime(2017, 6, 1, tzinfo=timezone.utc),
        "end":   datetime(2017, 6, 8, tzinfo=timezone.utc),
    },
    "1_month": {
        "start": datetime(2020, 1, 1, tzinfo=timezone.utc),
        "end":   datetime(2020, 1, 31, tzinfo=timezone.utc),
    },
    "1_year": {
        "start": datetime(2023, 1, 1, tzinfo=timezone.utc),
        "end":   datetime(2024, 1, 1, tzinfo=timezone.utc),
    },
    "3_years": {
        "start": datetime(2018, 1, 1, tzinfo=timezone.utc),
        "end":   datetime(2021, 1, 1, tzinfo=timezone.utc),
    },
    # "5_years": {
    #     "start": datetime(2013, 1, 1, tzinfo=timezone.utc),
    #     "end":   datetime(2018, 1, 1, tzinfo=timezone.utc),
    # },
    # "10_years": {
    #     "start": datetime(2012, 1, 1, tzinfo=timezone.utc),
    #     "end":   datetime(2022, 1, 1, tzinfo=timezone.utc),
    # },
    # "15_years_full": {
    #     "start": datetime(2011, 8, 13, tzinfo=timezone.utc),
    #     "end":   datetime(2026, 8, 13, tzinfo=timezone.utc),
    # },
}

NUMBER_OF_REPEATS = 3

readbench_config = load_config("./configs/config-readbench-influxdb.yml")
logger = get_logger("influxdb_range_query_benchmark", readbench_config["general"]["log_file"])
influxdb_repo = InfluxRepo(get_influxdb_config(readbench_config["database"]), logger)

if influxdb_repo.ping():
    print("Readbench InfluxDB connection successful.")
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
    for window_name, window in RANGE_WINDOWS.items():
        start_time = window["start"]
        end_time = window["end"]

        for run_num in range(1, NUMBER_OF_REPEATS + 1):
            observations, elapsed_ns = influxdb_repo.query_range(parameter, start_time, end_time)
            elapsed_seconds = elapsed_ns / 1_000_000_000
            row_count = len(observations)

            print(f"[{parameter}][{window_name}] run {run_num}: rows={row_count} elapsed={elapsed_ns} ns ({elapsed_seconds:.6f} s)")

            result = ReadRangeBenchmarkResult(
                benchmark_name="read.range_query",
                database_system="influxdb",
                database_version="2.7",
                parameter=parameter,
                range_window=window_name,
                start_time=start_time,
                end_time=end_time,
                result_row_count=row_count,
                elapsed_time_seconds=elapsed_seconds,
                elapsed_time_ns=elapsed_ns,
            )
            benchmarkdb_repo.insert_result(result)

        print(lines)

benchmarkdb_repo.disconnect()

print("InfluxDB range-query benchmark complete.")
influxdb_repo.close()