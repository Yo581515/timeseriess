from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    BigInteger,
)

from src.databases.benchmark_db.base import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class IngestionBenchmarkResult(Base):
    __tablename__ = "ingestion_benchmark_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    benchmark_name = Column(String, nullable=False)
    database_system = Column(String, nullable=False)
    database_version = Column(String, nullable=True)
    database_location = Column(String, nullable=True)

    insert_batch_size = Column(Integer, nullable=False)
    batch_id = Column(Integer, nullable=False)
    dataset_name = Column(String, nullable=False)
    record_count = Column(Integer, nullable=False)
    data_size_bytes = Column(BigInteger, nullable=False)
    data_size_kb = Column(Float, nullable=False)
    data_size_mb = Column(Float, nullable=False)

    elapsed_time_seconds = Column(Float, nullable=False)
    elapsed_time_ns = Column(BigInteger, nullable=False)
    throughput_obs_per_sec = Column(Float, nullable=False)
    throughput_bytes_per_sec = Column(Float, nullable=False)
    throughput_kb_per_sec = Column(Float, nullable=False)
    throughput_mb_per_sec = Column(Float, nullable=False)
    insert_operation_count = Column(Integer, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    def __str__(self) -> str:
        return (
            f"IngestionBenchmarkResult {self.id}\n"
            f"  benchmark:          {self.benchmark_name}\n"
            f"  database system:    {self.database_system}\n"
            f"  database version:   {self.database_version or 'N/A'}\n"
            f"  database location:  {self.database_location or 'N/A'}\n"
            f"  insert batch size:  {self.insert_batch_size}\n"
            f"  batch id:           {self.batch_id}\n"
            f"  dataset:            {self.dataset_name}\n"
            f"  record count:       {self.record_count}\n"
            f"  data size bytes:    {self.data_size_bytes}\n"
            f"  data size kb:       {self.data_size_kb:.2f}\n"
            f"  data size mb:       {self.data_size_mb:.3f}\n"
            f"  elapsed time:       {self.elapsed_time_seconds:.6f} s\n"
            f"  elapsed time ns:    {self.elapsed_time_ns} ns\n"
            f"  throughput obs/s:   {self.throughput_obs_per_sec:.2f}\n"
            f"  throughput bytes/s: {self.throughput_bytes_per_sec:.2f}\n"
            f"  throughput kb/s:    {self.throughput_kb_per_sec:.2f}\n"
            f"  throughput mb/s:    {self.throughput_mb_per_sec:.4f}\n"
            f"  insert operations:  {self.insert_operation_count}\n"
            f"  created at:         {self.created_at.isoformat()}"
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_system": self.database_system,
            "database_version": self.database_version,
            "database_location": self.database_location,
            "insert_batch_size": self.insert_batch_size,
            "batch_id": self.batch_id,
            "dataset_name": self.dataset_name,
            "record_count": self.record_count,
            "data_size_bytes": self.data_size_bytes,
            "data_size_kb": self.data_size_kb,
            "data_size_mb": self.data_size_mb,
            "elapsed_time_seconds": self.elapsed_time_seconds,
            "elapsed_time_ns": self.elapsed_time_ns,
            "throughput_obs_per_sec": self.throughput_obs_per_sec,
            "throughput_bytes_per_sec": self.throughput_bytes_per_sec,
            "throughput_kb_per_sec": self.throughput_kb_per_sec,
            "throughput_mb_per_sec": self.throughput_mb_per_sec,
            "insert_operation_count": self.insert_operation_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }