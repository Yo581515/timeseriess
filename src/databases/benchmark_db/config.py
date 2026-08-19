class BenchmarkDBConfigurationException(Exception):
    pass


class BenchmarkDBConfig:
    def __init__(self, username, password, host, port, database, schema):
        self.BENCHMARKDB_USER = username
        self.BENCHMARKDB_PASSWORD = password
        self.BENCHMARKDB_HOST = host
        self.BENCHMARKDB_PORT = port
        self.BENCHMARKDB_DATABASE = database
        self.BENCHMARKDB_SCHEMA = schema

    def __str__(self):
        return (
            "BenchmarkDB Configuration:\n"
            f"Host: {self.BENCHMARKDB_HOST}\n"
            f"Port: {self.BENCHMARKDB_PORT}\n"
            f"Database: {self.BENCHMARKDB_DATABASE}\n"
            f"Schema: {self.BENCHMARKDB_SCHEMA}\n"
        )


def get_postgres_config(config_dict: dict) -> BenchmarkDBConfig:
    try:
        return BenchmarkDBConfig(
            username=config_dict["BENCHMARKDB_USER"],
            password=config_dict["BENCHMARKDB_PASSWORD"],
            host=config_dict["BENCHMARKDB_HOST"],
            port=config_dict["BENCHMARKDB_PORT"],
            database=config_dict["BENCHMARKDB_DATABASE"],
            schema=config_dict["BENCHMARKDB_SCHEMA"],
        )
    except KeyError as e:
        raise BenchmarkDBConfigurationException(f"Missing config key: {e}")
