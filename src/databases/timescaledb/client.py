import logging
from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool

from src.databases.timescaledb.config import TimeScaleDBConfig


class TimeScaleDBClient:
    def __init__(self, timescaledb_config: TimeScaleDBConfig, logger: logging.Logger):
        self.logger = logger

        self.user = timescaledb_config.TIMESCALEDB_USER
        self.password = timescaledb_config.TIMESCALEDB_PASSWORD
        self.host = timescaledb_config.TIMESCALEDB_HOST
        self.port = timescaledb_config.TIMESCALEDB_PORT
        self.database = timescaledb_config.TIMESCALEDB_DATABASE
        self.schema = timescaledb_config.TIMESCALEDB_SCHEMA

        self.url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

        self.pool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=self.url)

        self.logger.info("TimescaleDB connection pool created")

    @contextmanager
    def cursor(self):
        conn = self.pool.getconn()
        try:
            if self.schema:
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {self.schema}")

            with conn.cursor() as cur:
                yield cur

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    def close(self):
        self.pool.closeall()
        self.logger.info("TimescaleDB connection pool closed")


if __name__ == "__main__":
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.timescaledb.config import get_timescaledb_config

    timescale_config_path = "./configs/config-timescaledb.yml"
    timescale_config = load_config(timescale_config_path)  # dict
    logger = get_logger("timescaledb", timescale_config["general"]["log_file"])
    timescale_db_config = get_timescaledb_config(timescale_config["database"])
    timescaledb = TimeScaleDBClient(timescale_db_config, logger)

    timescaledb.ping()
