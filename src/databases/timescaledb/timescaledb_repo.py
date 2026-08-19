import time
import logging
from datetime import datetime, timedelta, timezone

from psycopg2.extras import execute_values

from src.databases.timescaledb.client import TimeScaleDBClient
from src.databases.timescaledb.config import TimeScaleDBConfig
from src.databases.timescaledb.models.observation import Observation


class TimescaleDBRepo(TimeScaleDBClient):

    def __init__(self, timescaledb_config: TimeScaleDBConfig, logger: logging.Logger):
        super().__init__(timescaledb_config, logger)

    def ping(self) -> bool:
        with self.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result and result[0] == 1:
                self.logger.info("TimescaleDB ping successful")
                return True
            self.logger.error("TimescaleDB ping failed")
            return False

    def insert_one(self, observation: Observation) -> None:
        query = """
            INSERT INTO observations (
                time, node_source, node_source_id, latitude, longitude,
                sensor_source, sensor_source_id, parameter, value, unit, quality_codes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        with self.cursor() as cur:
            t0 = time.perf_counter()
            cur.execute(query, observation.to_tuple())
            elapsed = time.perf_counter() - t0
            self.logger.info(f"Inserted 1 observation in {elapsed:.6f} s")

    def insert_many(self, observations: list[Observation], page_size: int = 100) -> float:
        query = """
            INSERT INTO observations (
                time, node_source, node_source_id, latitude, longitude,
                sensor_source, sensor_source_id, parameter, value, unit, quality_codes
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """
        tuples = [o.to_tuple() for o in observations]
        try:
            with self.cursor() as cur:
                t0 = time.perf_counter_ns()
                execute_values(cur, query, tuples, page_size=page_size)
                t1 = time.perf_counter_ns()
                elapsed_time = t1 - t0 if t1 > t0 else 0
                return elapsed_time
        except Exception as e:
            self.logger.error(f"Error inserting batch: {e}")
            raise Exception(f"Batch insert failed: {e}")

    def delete_all(self) -> None:

        try:
            with self.cursor() as cur:
                cur.execute("TRUNCATE TABLE observations")
                self.logger.info("Deleted all observations from the table")
        except Exception as e:
            self.logger.error(f"Error deleting observations: {e}")
            raise Exception(f"Delete operation failed: {e}")

    def query_by_parameter(
            self,
            parameter: str,
            start_time: datetime,
            end_time: datetime,
    ) -> list[Observation]:
        query = """
            SELECT
                time, node_source, node_source_id, latitude, longitude,
                sensor_source, sensor_source_id, parameter, value, unit, quality_codes
            FROM observations
            WHERE parameter = %s
              AND time >= %s
              AND time <= %s
            ORDER BY time DESC
        """
        with self.cursor() as cur:
            t0 = time.perf_counter()
            cur.execute(query, (parameter, start_time, end_time))
            rows = cur.fetchall()
            elapsed = time.perf_counter() - t0
            self.logger.info(f"Queried {len(rows)} observations in {elapsed:.6f} s")
            return [Observation(*row) for row in rows]

    def query_latest(self, parameter: str) -> tuple[Observation | None, int]:
        query = """
            SELECT
                time, node_source, node_source_id, latitude, longitude,
                sensor_source, sensor_source_id, parameter, value, unit, quality_codes
            FROM observations
            WHERE parameter = %s
            ORDER BY time DESC
            LIMIT 1
        """
        with self.cursor() as cur:
            t0 = time.perf_counter_ns()
            cur.execute(query, (parameter,))
            row = cur.fetchone()
            elapsed_ns = time.perf_counter_ns() - t0
            obs = Observation(*row) if row else None
            return obs, elapsed_ns

    def query_range(
            self,
            parameter: str,
            start_time: datetime,
            end_time: datetime,
    ) -> tuple[list[Observation], int]:
        query = """
            SELECT
                time, node_source, node_source_id, latitude, longitude,
                sensor_source, sensor_source_id, parameter, value, unit, quality_codes
            FROM observations
            WHERE parameter = %s
            AND time >= %s
            AND time <= %s
            ORDER BY time DESC
        """
        with self.cursor() as cur:
            t0 = time.perf_counter_ns()
            cur.execute(query, (parameter, start_time, end_time))
            rows = cur.fetchall()
            elapsed_ns = time.perf_counter_ns() - t0
            observations = [Observation(*row) for row in rows]
            return observations, elapsed_ns

    def query_cardinality(
            self,
            start_time: datetime,
            end_time: datetime,
            parameter: str | None = None,
            node_source_id: str | None = None,
    ) -> tuple[list[Observation], int]:
        conditions = ["time >= %s", "time <= %s"]
        params = [start_time, end_time]

        if parameter is not None:
            conditions.append("parameter = %s")
            params.append(parameter)

        if node_source_id is not None:
            conditions.append("node_source_id = %s")
            params.append(node_source_id)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT
                time, node_source, node_source_id, latitude, longitude,
                sensor_source, sensor_source_id, parameter, value, unit, quality_codes
            FROM observations
            WHERE {where_clause}
            ORDER BY time DESC
        """
        with self.cursor() as cur:
            t0 = time.perf_counter_ns()
            cur.execute(query, params)
            rows = cur.fetchall()
            elapsed_ns = time.perf_counter_ns() - t0
            observations = [Observation(*row) for row in rows]
            return observations, elapsed_ns

    def query_aggregate(
            self,
            parameter: str,
            start_time: datetime,
            end_time: datetime,
    ) -> tuple[dict, int]:
        query = """
            SELECT
                AVG(value) AS avg_value,
                MIN(value) AS min_value,
                MAX(value) AS max_value,
                COUNT(*) AS row_count
            FROM observations
            WHERE parameter = %s
            AND time >= %s
            AND time <= %s
        """
        with self.cursor() as cur:
            t0 = time.perf_counter_ns()
            cur.execute(query, (parameter, start_time, end_time))
            row = cur.fetchone()
            elapsed_ns = time.perf_counter_ns() - t0

            result = {
                "avg_value": float(row[0]) if row[0] is not None else None,
                "min_value": float(row[1]) if row[1] is not None else None,
                "max_value": float(row[2]) if row[2] is not None else None,
                "row_count": row[3],
            }
            return result, elapsed_ns

    def query_bucketed(
            self,
            parameter: str,
            start_time: datetime,
            end_time: datetime,
            bucket_interval: str,
    ) -> tuple[list[dict], int]:
        query = """
            SELECT
                time_bucket(%s, time) AS bucket,
                AVG(value) AS avg_value,
                COUNT(*) AS row_count
            FROM observations
            WHERE parameter = %s
            AND time >= %s
            AND time <= %s
            GROUP BY bucket
            ORDER BY bucket
        """
        with self.cursor() as cur:
            t0 = time.perf_counter_ns()
            cur.execute(query, (bucket_interval, parameter, start_time, end_time))
            rows = cur.fetchall()
            elapsed_ns = time.perf_counter_ns() - t0

            buckets = [
                {"bucket_time": row[0], "avg_value": float(row[1]) if row[1] is not None else None, "row_count": row[2]}
                for row in rows
            ]
            return buckets, elapsed_ns


if __name__ == "__main__":
    from pprint import pprint
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.timescaledb.config import get_timescaledb_config


    def utc_now() -> datetime:
        return datetime.now(timezone.utc)


    timescale_config = load_config("./configs/config-timescaledb.yml")
    logger = get_logger("timescaledb_repo", timescale_config["general"]["log_file"])
    timescaledb = TimescaleDBRepo(get_timescaledb_config(timescale_config["database"]), logger)

    print("ping:", timescaledb.ping())

    try:
        obs = Observation(
            time=utc_now(),
            node_source="test_node",
            node_source_id="test_node_id",
            latitude=60.090717,
            longitude=5.263733,
            sensor_source="test_sensor",
            sensor_source_id="test_sensor_id",
            parameter="sea_water_temperature",
            value=16.71,
            unit="degrees_C",
            quality_codes=[0],
        )
        timescaledb.insert_one(obs)
        print("insert_one ok")
    except Exception as e:
        logger.error(f"insert_one failed: {e}")

    try:
        end_dt = utc_now()
        start_dt = end_dt - timedelta(hours=1)
        results = timescaledb.query_by_parameter("sea_water_temperature", start_dt, end_dt)
        logger.info(f"query returned {len(results)} rows")
        if results:
            print(results[0])
    except Exception as e:
        logger.error(f"query failed: {e}")

    print("done")
