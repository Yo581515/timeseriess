import logging
from contextlib import contextmanager
from typing import List

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.databases.benchmark_db.client import BenchmarkDBClient
from src.databases.benchmark_db.config import BenchmarkDBConfig
from src.databases.benchmark_db.models import IngestionBenchmarkResult


class BenchmarkDB(BenchmarkDBClient):
    def __init__(self, benchmark_db_config: BenchmarkDBConfig, logger: logging.Logger):
        super().__init__(benchmark_db_config, logger)

    @contextmanager
    def session_scope(self):
        if not self.SessionLocal:
            raise RuntimeError("SessionLocal is not initialized. Call connect() first.")

        db: Session = self.SessionLocal()
        try:
            yield db
            db.commit()
        except Exception as e:
            db.rollback()
            self.logger.exception("DB transaction failed; rolled back. Error=%s", e)
            raise
        finally:
            db.close()

    def ping(self) -> bool:
        try:
            with self.session_scope() as db:
                self.logger.info("Pinging benchmark database...")
                db.execute(text("SELECT 1"))
            self.logger.info("Ping OK.")
            return True
        except Exception as e:
            self.logger.error("Ping error: %s", e)
            return False

    def insert_result(self, result: IngestionBenchmarkResult) -> IngestionBenchmarkResult:
        with self.session_scope() as db:
            db.add(result)
            db.flush()
            db.refresh(result)
        self.logger.info("Inserted IngestionBenchmarkResult id=%s", result.id)
        return result

    def get_all_results(self) -> List[IngestionBenchmarkResult]:
        with self.session_scope() as db:
            return db.query(IngestionBenchmarkResult).all()

    def count_results(self) -> int:
        with self.session_scope() as db:
            return db.query(IngestionBenchmarkResult).count()

    def clear_results(self) -> int:
        with self.session_scope() as db:
            deleted = db.query(IngestionBenchmarkResult).delete()
        self.logger.info("Deleted %d rows from ingestion_benchmark_results", deleted)
        return deleted


if __name__ == "__main__":

    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.benchmark_db.config import get_postgres_config

    bmdb_config_file_path = "./configs/config-benchmarkdb.yml"
    bmdb_config_dict = load_config(bmdb_config_file_path)

    logger = get_logger(
        "benchmark_db_test",
        bmdb_config_dict["general"]["log_file"],
    )

    benchmark_db_config = get_postgres_config(bmdb_config_dict["database"])

    bm_db = BenchmarkDB(benchmark_db_config, logger)

    try:
        bm_db.connect()
        bm_db.create_tables()
        bm_db.ping()

        print("BenchmarkDB is connected and ready.\n")

        result = IngestionBenchmarkResult(
            benchmark_name="ingestion.insert_many.fixed_batch",
            database_system="mongodb",
            database_version="7.0",
            database_location="container",
            insert_batch_size=1000,
            batch_id=7,
            dataset_name="batch_7_5000_obs.json",
            record_count=5000,
            data_size_bytes=920000,
            data_size_kb=898.44,
            data_size_mb=0.878,
            elapsed_time_seconds=0.85,
            elapsed_time_ns=850000000,
            throughput_obs_per_sec=5882.35,
            throughput_bytes_per_sec=1082352.94,
            throughput_kb_per_sec=1056.99,
            throughput_mb_per_sec=1.03,
            insert_operation_count=5,
        )

        result = bm_db.insert_result(result)
        print("Inserted IngestionBenchmarkResult:")
        print(result)
        print()

    except Exception as e:
        logger.error("An error occurred: %s", e)

    finally:
        bm_db.disconnect()

    print("\nNow testing querying benchmark entries...\n")

    try:
        bm_db.connect()

        results = bm_db.get_all_results()
        print(f"Total ingestion results: {len(results)}\n")

        if results:
            print("First IngestionBenchmarkResult:")
            print(results[0])

    except Exception as e:
        logger.error("An error occurred during querying: %s", e)

    finally:
        bm_db.disconnect()