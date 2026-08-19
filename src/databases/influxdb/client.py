from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import WriteApi

import logging

from src.databases.influxdb.config import InfluxDBConfig


class InfluxDB_Client:
    def __init__(self, influxdb_config: InfluxDBConfig, logger: logging.Logger):
        self.logger = logger

        self.username = influxdb_config.INFLUXDB_USERNAME
        self.password = influxdb_config.INFLUXDB_PASSWORD
        self.host = influxdb_config.INFLUXDB_HOST
        self.port = influxdb_config.INFLUXDB_PORT
        self.org = influxdb_config.INFLUXDB_ORG
        self.bucket = influxdb_config.INFLUXDB_BUCKET
        self.token = influxdb_config.INFLUXDB_TOKEN

        self.url = f"http://{self.host}:{self.port}"

        self.write_client = InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org,
            username=self.username,
            password=self.password
        )

    def write_api(self) -> WriteApi:
        try:
            self.logger.info("Creating InfluxDB write API...")
            # SYNCHRONOUS so writes block until they're actually committed,
            # needed for accurate timing when benchmarking write throughput
            return self.write_client.write_api(write_options=SYNCHRONOUS)
        except Exception as e:
            self.logger.error(f"Error creating InfluxDB write API: {e}")
            return None

    def query_api(self) -> QueryApi:
        try:
            self.logger.info("Creating InfluxDB query API...")
            return self.write_client.query_api()
        except Exception as e:
            self.logger.error(f"Error creating InfluxDB query API: {e}")
            return None

    def close(self) -> None:
        try:
            self.logger.info("Closing InfluxDB client...")
            if self.write_client:
                self.write_client.close()
                self.logger.info("InfluxDB client closed")
        except Exception as e:
            self.logger.error(f"Error closing InfluxDB client: {e}")


if __name__ == "__main__":
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.influxdb.config import get_influxdb_config

    influxdb_config_path = "./configs/config-influxdb.yml"
    influxdb_config_dict = load_config(influxdb_config_path)  # dict
    logger = get_logger("client.py", influxdb_config_dict["general"]["log_file"])
    influxdb_config = get_influxdb_config(influxdb_config_dict["database"])
    print(influxdb_config)
    client = InfluxDB_Client(influxdb_config, logger)

    try:
        client.write_client()
    finally:
        client.close()
