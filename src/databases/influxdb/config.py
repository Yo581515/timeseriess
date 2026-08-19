from src.common.config import load_config


class InfluxDBConfigurationException(Exception):
    pass


class InfluxDBConfig:
    def __init__(self, username, password, org, bucket, host, port, token):
        self.INFLUXDB_USERNAME = username
        self.INFLUXDB_PASSWORD = password
        self.INFLUXDB_ORG = org
        self.INFLUXDB_BUCKET = bucket
        self.INFLUXDB_HOST = host
        self.INFLUXDB_PORT = port
        self.INFLUXDB_TOKEN = token

    def __str__(self):
        return (
            "InfluxDB Configuration:\n"
            f"Host: {self.INFLUXDB_HOST}\n"
            f"Port: {self.INFLUXDB_PORT}\n"
            f"Org: {self.INFLUXDB_ORG}\n"
            f"Bucket: {self.INFLUXDB_BUCKET}\n"
        )


def get_influxdb_config(config_dict: dict) -> InfluxDBConfig:
    try:
        return InfluxDBConfig(
            username=config_dict["INFLUXDB_USERNAME"],
            password=config_dict["INFLUXDB_PASSWORD"],
            org=config_dict["INFLUXDB_ORG"],
            bucket=config_dict["INFLUXDB_BUCKET"],
            host=config_dict["INFLUXDB_HOST"],
            port=config_dict["INFLUXDB_PORT"],
            token=config_dict["INFLUXDB_TOKEN"],
        )
    except KeyError as e:
        raise InfluxDBConfigurationException(f"Missing config key: {e}")


if __name__ == "__main__":
    config_file_path = "configs/config-influxdb.yml"
    config_file_dict = load_config(config_file_path)
    influxdb_config = get_influxdb_config(config_file_dict["database"])
    print(influxdb_config)
