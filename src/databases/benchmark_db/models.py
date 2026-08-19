from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
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


# one row per write-batch: how long it took to insert a batch and at what throughput
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


# one row per batch: disk size before/after inserting that batch, to track storage growth over time   
class DiskSizeBenchmarkResult(Base):
    __tablename__ = "disk_size_benchmark_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    benchmark_name = Column(String, nullable=False)
    database_system = Column(String, nullable=False)
    database_version = Column(String, nullable=True)

    batch_id = Column(Integer, nullable=False)
    dataset_name = Column(String, nullable=False)
    record_count = Column(Integer, nullable=False)

    cumulative_record_count = Column(BigInteger, nullable=False)

    disk_size_before_bytes = Column(BigInteger, nullable=False)
    disk_size_after_bytes = Column(BigInteger, nullable=False)
    disk_size_delta_bytes = Column(BigInteger, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    def __str__(self) -> str:
        return (
            f"DiskSizeBenchmarkResult {self.id}\n"
            f"  benchmark:           {self.benchmark_name}\n"
            f"  database system:     {self.database_system}\n"
            f"  database version:    {self.database_version or 'N/A'}\n"
            f"  batch id:            {self.batch_id}\n"
            f"  dataset:             {self.dataset_name}\n"
            f"  record count:        {self.record_count}\n"
            f"  cumulative records:  {self.cumulative_record_count}\n"
            f"  size before:         {self.disk_size_before_bytes:,} bytes\n"
            f"  size after:          {self.disk_size_after_bytes:,} bytes\n"
            f"  size delta:          {self.disk_size_delta_bytes:,} bytes\n"
            f"  created at:          {self.created_at.isoformat()}"
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_system": self.database_system,
            "database_version": self.database_version,
            "batch_id": self.batch_id,
            "dataset_name": self.dataset_name,
            "record_count": self.record_count,
            "cumulative_record_count": self.cumulative_record_count,
            "disk_size_before_bytes": self.disk_size_before_bytes,
            "disk_size_after_bytes": self.disk_size_after_bytes,
            "disk_size_delta_bytes": self.disk_size_delta_bytes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# one row per lookup: time to fetch the most recent value for a given parameter
class ReadLatestBenchmarkResult(Base):
    __tablename__ = "read_latest_benchmark_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    benchmark_name = Column(String, nullable=False)
    database_system = Column(String, nullable=False)
    database_version = Column(String, nullable=True)

    parameter = Column(String, nullable=False)
    result_found = Column(Boolean, nullable=False)

    elapsed_time_seconds = Column(Float, nullable=False)
    elapsed_time_ns = Column(BigInteger, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    def __str__(self) -> str:
        return (
            f"ReadLatestBenchmarkResult {self.id}\n"
            f"  benchmark:          {self.benchmark_name}\n"
            f"  database system:    {self.database_system}\n"
            f"  database version:   {self.database_version or 'N/A'}\n"
            f"  parameter:          {self.parameter}\n"
            f"  result found:       {self.result_found}\n"
            f"  elapsed time:       {self.elapsed_time_seconds:.6f} s\n"
            f"  elapsed time ns:    {self.elapsed_time_ns} ns\n"
            f"  created at:         {self.created_at.isoformat()}"
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_system": self.database_system,
            "database_version": self.database_version,
            "parameter": self.parameter,
            "result_found": self.result_found,
            "elapsed_time_seconds": self.elapsed_time_seconds,
            "elapsed_time_ns": self.elapsed_time_ns,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# one row per query: time to fetch all values for a parameter within a time range
class ReadRangeBenchmarkResult(Base):
    __tablename__ = "read_range_benchmark_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    benchmark_name = Column(String, nullable=False)
    database_system = Column(String, nullable=False)
    database_version = Column(String, nullable=True)

    parameter = Column(String, nullable=False)
    range_window = Column(String, nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)

    result_row_count = Column(Integer, nullable=False)

    elapsed_time_seconds = Column(Float, nullable=False)
    elapsed_time_ns = Column(BigInteger, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_system": self.database_system,
            "database_version": self.database_version,
            "parameter": self.parameter,
            "range_window": self.range_window,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "result_row_count": self.result_row_count,
            "elapsed_time_seconds": self.elapsed_time_seconds,
            "elapsed_time_ns": self.elapsed_time_ns,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# one row per query: time to count distinct series at different granularities
# (single sensor, single parameter across all nodes, or everything
class ReadCardinalityBenchmarkResult(Base):
    __tablename__ = "read_cardinality_benchmark_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    benchmark_name = Column(String, nullable=False)
    database_system = Column(String, nullable=False)
    database_version = Column(String, nullable=True)

    cardinality_level = Column(String, nullable=False)  # "single_sensor" | "single_parameter_all_nodes" | "all"
    parameter = Column(String, nullable=True)  # NULL when level = "all"
    node_source_id = Column(String, nullable=True)  # NULL when level != "single_sensor"

    range_window = Column(String, nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)

    result_row_count = Column(Integer, nullable=False)

    elapsed_time_seconds = Column(Float, nullable=False)
    elapsed_time_ns = Column(BigInteger, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_system": self.database_system,
            "database_version": self.database_version,
            "cardinality_level": self.cardinality_level,
            "parameter": self.parameter,
            "node_source_id": self.node_source_id,
            "range_window": self.range_window,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "result_row_count": self.result_row_count,
            "elapsed_time_seconds": self.elapsed_time_seconds,
            "elapsed_time_ns": self.elapsed_time_ns,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# one row per query: time to compute avg/min/max over a parameter within a time range
class ReadAggregateBenchmarkResult(Base):
    __tablename__ = "read_aggregate_benchmark_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    benchmark_name = Column(String, nullable=False)
    database_system = Column(String, nullable=False)
    database_version = Column(String, nullable=True)

    parameter = Column(String, nullable=False)
    range_window = Column(String, nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)

    avg_value = Column(Float, nullable=True)
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    row_count = Column(Integer, nullable=True)

    elapsed_time_seconds = Column(Float, nullable=False)
    elapsed_time_ns = Column(BigInteger, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_system": self.database_system,
            "database_version": self.database_version,
            "parameter": self.parameter,
            "range_window": self.range_window,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "avg_value": self.avg_value,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "row_count": self.row_count,
            "elapsed_time_seconds": self.elapsed_time_seconds,
            "elapsed_time_ns": self.elapsed_time_ns,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# one row per query: time to bucket a parameter into fixed intervals (e.g. hourly averages)
class ReadBucketedBenchmarkResult(Base):
    __tablename__ = "read_bucketed_benchmark_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    benchmark_name = Column(String, nullable=False)
    database_system = Column(String, nullable=False)
    database_version = Column(String, nullable=True)

    parameter = Column(String, nullable=False)
    bucket_interval = Column(String, nullable=False)
    range_window = Column(String, nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)

    bucket_count = Column(Integer, nullable=False)

    elapsed_time_seconds = Column(Float, nullable=False)
    elapsed_time_ns = Column(BigInteger, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_system": self.database_system,
            "database_version": self.database_version,
            "parameter": self.parameter,
            "bucket_interval": self.bucket_interval,
            "range_window": self.range_window,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "bucket_count": self.bucket_count,
            "elapsed_time_seconds": self.elapsed_time_seconds,
            "elapsed_time_ns": self.elapsed_time_ns,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
