import time
import logging
from datetime import datetime, timezone

from influxdb_client import Point, WritePrecision

from src.databases.influxdb.client import InfluxDB_Client
from src.databases.influxdb.config import InfluxDBConfig
from src.databases.influxdb.models.observation import Observation

MEASUREMENT = "ocean_observations"


class InfluxRepo(InfluxDB_Client):

    def __init__(self, influxdb_config: InfluxDBConfig, logger: logging.Logger):
        super().__init__(influxdb_config, logger)

    def ping(self) -> bool:
        try:
            result = self.write_client.ping()
            if result:
                self.logger.info("InfluxDB ping successful")
                return True
            self.logger.error("InfluxDB ping failed")
            return False
        except Exception as e:
            self.logger.error(f"Error pinging InfluxDB: {e}")
            raise Exception(f"Error pinging InfluxDB: {e}")

    def _to_point(self, obs: Observation) -> Point:
        return (
            Point(MEASUREMENT)
            .tag("node_source", obs.node_source)
            .tag("node_source_id", obs.node_source_id)
            .tag("sensor_source", obs.sensor_source)
            .tag("sensor_source_id", obs.sensor_source_id)
            .tag("parameter", obs.parameter)
            .tag("unit", obs.unit)
            .field("value", float(obs.value))
            .field("latitude", float(obs.latitude))
            .field("longitude", float(obs.longitude))
            .field("quality_codes", obs.quality_codes_str())
            .time(obs.time, WritePrecision.NS)
        )

    def insert_one(self, obs: Observation) -> int:
        try:
            point = self._to_point(obs)
            with self.write_api() as write_api:
                t0 = time.perf_counter_ns()
                write_api.write(bucket=self.bucket, record=point)
                elapsed_ns = time.perf_counter_ns() - t0
            self.logger.info(f"Inserted 1 observation in {elapsed_ns} ns")
            return elapsed_ns
        except Exception as e:
            self.logger.error(f"Error inserting observation: {e}")
            raise Exception(f"Insert failed: {e}")

    def insert_many(self, observations: list[Observation]) -> int:
        try:
            points = [self._to_point(obs) for obs in observations]
            with self.write_api() as write_api:
                t0 = time.perf_counter_ns()
                write_api.write(bucket=self.bucket, record=points)
                elapsed_ns = time.perf_counter_ns() - t0
            self.logger.info(f"Inserted {len(observations)} observations in {elapsed_ns} ns")
            return elapsed_ns
        except Exception as e:
            self.logger.error(f"Error inserting observations: {e}")
            raise Exception(f"Batch insert failed: {e}")

    def query_by_parameter(
        self,
        parameter: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Observation]:
        try:
            start = start_time.astimezone(timezone.utc).isoformat()
            stop = end_time.astimezone(timezone.utc).isoformat()

            query = f"""
                from(bucket: "{self.bucket}")
                |> range(start: time(v: "{start}"), stop: time(v: "{stop}"))
                |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
                |> filter(fn: (r) => r.parameter == "{parameter}")
                |> pivot(
                    rowKey: ["_time", "node_source_id", "sensor_source_id"],
                    columnKey: ["_field"],
                    valueColumn: "_value"
                )
            """

            t0 = time.perf_counter_ns()
            tables = self.query_api().query(query, org=self.org)
            elapsed_ns = time.perf_counter_ns() - t0
            self.logger.info(f"Query returned in {elapsed_ns} ns")

            observations = []
            for table in tables:
                for record in table.records:
                    v = record.values
                    qc_raw = v.get("quality_codes", "[]")
                    qcs = [int(q) for q in qc_raw.strip("[]").split(",") if q]
                    observations.append(Observation(
                        time=v.get("_time"),
                        node_source=v.get("node_source"),
                        node_source_id=v.get("node_source_id"),
                        latitude=float(v.get("latitude", 0)),
                        longitude=float(v.get("longitude", 0)),
                        sensor_source=v.get("sensor_source"),
                        sensor_source_id=v.get("sensor_source_id"),
                        parameter=v.get("parameter"),
                        value=float(v.get("value", 0)),
                        unit=v.get("unit", ""),
                        quality_codes=qcs,
                    ))
            return observations

        except Exception as e:
            self.logger.error(f"Error querying observations: {e}")
            return []

    def delete_all(self) -> None:
        try:
            delete_api = self.write_client.delete_api()
            start = "2000-01-01T00:00:00Z"
            stop = datetime.now(timezone.utc).isoformat()
            delete_api.delete(
                start, stop,
                f'_measurement="{MEASUREMENT}"',
                bucket=self.bucket,
                org=self.org,
            )
            self.logger.info("Deleted all observations from InfluxDB")
        except Exception as e:
            self.logger.error(f"Error deleting observations: {e}")
            raise Exception(f"Delete failed: {e}")


if __name__ == "__main__":
    from datetime import timedelta
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.influxdb.config import get_influxdb_config

    def utc_now() -> datetime:
        return datetime.now(timezone.utc)

    influxdb_config = load_config("./configs/config-influxdb.yml")
    logger = get_logger("influx_repo", influxdb_config["general"]["log_file"])
    repo = InfluxRepo(get_influxdb_config(influxdb_config["database"]), logger)

    print("ping:", repo.ping())

    repo.delete_all()

    try:
        obs = Observation(
            time=utc_now(),
            node_source="Node 1",
            node_source_id="sfi_smart_ocean;demo;d1;1",
            latitude=60.090717,
            longitude=5.263733,
            sensor_source="Aanderaa Temperature PROBE",
            sensor_source_id="sfi_smart_ocean;demo;d1;1;AANDERAA_TEMPERATURE",
            parameter="sea_water_temperature",
            value=16.71,
            unit="degrees_C",
            quality_codes=[0],
        )
        repo.insert_one(obs)
        print("insert_one ok")
    except Exception as e:
        logger.error(f"insert_one failed: {e}")

    try:
        end_dt = utc_now()
        start_dt = end_dt - timedelta(hours=1)
        results = repo.query_by_parameter("sea_water_temperature", start_dt, end_dt)
        print(f"query returned {len(results)} rows")
        if results:
            print(results[0])
    except Exception as e:
        logger.error(f"query failed: {e}")
    finally:
        repo.close()

    print("done")