# TODO 1: LOAD DATA
# - get all MongoDB data files from data/mongodb_data/ (exclude setup/ and utils/ subdirs)
# - Parse into a list of docs
# - Load dataset metadata from dataset_sizes.csv (data_size_bytes, kb, mb, record_count)

import json
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

lines = ""
for i in range(50):
    lines += "-"
print(lines)





MONGODB_DATA_DIR = Path("data/mongodb_data")
DATASET_SIZES_CSV = Path("data/row_data/dataset_sizes.csv")

@dataclass
class DatasetMeta:
    dataset: str
    batch_id: int
    record_count: int
    data_size_bytes: int
    data_size_kb: float
    data_size_mb: float
    

def load_dataset_sizes(csv_path: Path) -> dict[int, DatasetMeta]:
    df = pd.read_csv(csv_path)
    return {
        row.batch_id: DatasetMeta(
            dataset=row.dataset,
            batch_id=row.batch_id,
            record_count=row.record_count,
            data_size_bytes=row.json_data_bytes,
            data_size_kb=row.json_data_kb,
            data_size_mb=row.json_data_mb,
        )
        for row in df.itertuples()
    }

def parse_doc_time(doc: dict) -> dict:
    doc["time"] = datetime.fromisoformat(doc["time"].replace("Z", "+00:00"))
    return doc


def load_mongodb_files(data_dir: Path) -> list[tuple[int, list[dict]]]:
    files = sorted(
        data_dir.glob("*.json"),
        key=lambda f: int(f.stem.split("_")[3])
    )
    result = []
    for file in files:
        batch_id = int(file.stem.split("_")[3])
        with open(file) as f:
            docs = [parse_doc_time(doc) for doc in json.load(f)]
        result.append((batch_id, docs))
    return result

sizes = load_dataset_sizes(DATASET_SIZES_CSV)
files = load_mongodb_files(MONGODB_DATA_DIR)

from pprint import pprint
# print(type(files))
for batch_id, docs in files:
    # pprint(batch_id)
    # pprint(docs)
    pass


print(lines)
# TODO 2: SETUP MONGODB CLIENT
# - Load config from configs/config-mgdb.yml
# - Instantiate MongoDBRepository
# - Call connect_and_cache()
# - Ping to verify connection
from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository

mongodb_config_file_path = "./configs/config-mgdb.yml"
mongodb_config_dict = load_config(mongodb_config_file_path)
logger = get_logger("mongodb_ingest_benchmark.py", mongodb_config_dict["general"]["log_file"])
mongodb_config = get_mongodb_config(mongodb_config_dict["database"])
mongodb_repo = MongoDBRepository(mongodb_config, logger)
mongodb_repo.connect_and_cache()
if mongodb_repo.ping():
    print("MongoDB connection successful.")
    mongodb_repo.create_db_and_collection()
    mongodb_repo.delete_by_query({})


print(lines)
# TODO 3: SETUP BENCHMARK DB CLIENT
# - Load config from configs/config-benchmarkdb.yml
# - Instantiate BenchmarkDB
# - Call connect() + create_tables()
# - Ping to verify connection
from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB

bmdb_config_file_path = "./configs/config-benchmarkdb.yml"
bmdb_config_dict = load_config(bmdb_config_file_path)
logger = get_logger("benchmark_db_test", bmdb_config_dict["general"]["log_file"],)
bmdb_config = get_postgres_config(bmdb_config_dict["database"])
bm_db = BenchmarkDB(bmdb_config, logger)

try:
    bm_db.connect()
    bm_db.create_tables()
    bm_db.ping()

    print("BenchmarkDB is connected and ready.\n")
except Exception as e:
    logger.error("An error occurred: %s", e)
finally:
    bm_db.disconnect()



print(lines)
# TODO 4: RUN INGESTION BENCHMARK
# For each file (batch_1 → batch_len(files)):
#   - Get matching metadata from dataset_sizes (batch_id, record_count, bytes, kb, mb)
#   - Convert insert_batch_size (1000 obs) → doc_batch_size (1000 / 5 = 200 docs)
#   - For run_number in range(1, 6):
#       - Clear the MongoDB collection (fresh start for each run)
#       - Start timer (time.perf_counter())
#       - Loop: chunk docs into doc_batch_size groups, call insert_many() per chunk
#         keeping count of insert_operation_count
#       - Stop timer
#       - Calculate metrics:
#           elapsed_time_seconds
#           throughput_obs_per_sec   = record_count / elapsed
#           throughput_bytes_per_sec = data_size_bytes / elapsed
#           throughput_kb_per_sec    = data_size_kb / elapsed
#           throughput_mb_per_sec    = data_size_mb / elapsed
import math
import time
from src.databases.benchmark_db.models import  IngestionBenchmarkResult

# 100 batch size, 50 full inserts took 8-9 mins
BATCH_SIZE = 100
BATCH_SIZE = BATCH_SIZE / 5 # one mongo db doc have 5 observations
# print("Calculated doc batch size:", BATCH_SIZE)

batch_number = 1
files = files[batch_number-1:batch_number]
print(type(files), len(files)) #list
print(type(sizes), len(sizes)) # doc
print(type(files[0]))

for batch_id, sataset_meta in sizes.items():
    # print(sataset_meta)
    pass
# sizes_meta = sizes[batch_number]
# print(sizes_meta)

# 50 tar 3 minutter
number_of_fill_inserted_data = 50

try:
    st = time.perf_counter()
    for i in range(number_of_fill_inserted_data):
        for batch_id, docs in files:
            print("batch_id is ", batch_id)
            
            dataset_meta = sizes[batch_id]
            print("Dataset metadata:", dataset_meta)
            
            docs_len = len(docs)
            print("Total docs to insert:", docs_len)
            start_batch_index = int(0)
            end_batch_index = int(docs_len if docs_len <= BATCH_SIZE else BATCH_SIZE)
            num_iterations = math.ceil(docs_len / end_batch_index)
            print(num_iterations)
            
            total_insert_time_ns = 0
            
            
            # cache docs in MongoDBRepository to speed up inserts
            try:
                docs_seg = docs[start_batch_index:end_batch_index]
                mongodb_repo.insert_many(docs_seg)
                mongodb_repo.insert_many(docs_seg)
                mongodb_repo.insert_many(docs_seg)
            except Exception as e:
                logger.error("An error occurred during initial caching of docs: %s", e)
                raise Exception("Initial caching of docs failed") from e
            
            time.sleep(60)            
            for run_num in range(1,num_iterations+1):
                docs_seg = docs[start_batch_index:end_batch_index]
                # print("docs_seg length", len(docs_seg))
                try:
                    inserted, elapsed_time_ns =  mongodb_repo.insert_many(docs_seg)
                    start_batch_index = end_batch_index
                    end_batch_index = int(end_batch_index + BATCH_SIZE if end_batch_index + BATCH_SIZE <= docs_len else docs_len)
                    total_insert_time_ns += elapsed_time_ns # elapsed time ns
                    time.sleep(60) # small sleep to avoid overwhelming the database with back-to-back inserts
                except Exception as e:
                    logger.error("An error occurred during insert_many: %s", e)
                    inserted = False
                    raise Exception("Insert operation failed") from e
                if inserted:
                    print(f"Run {run_num}: Inserted {len(docs_seg)} docs in {elapsed_time_ns} ns")
            
            # import time
            # time.sleep(60) # wait a bit to ensure all writes are flushed to disk before getting collection stats
            # collection = mongodb_repo.collection
            # stats = collection.database.command("collstats", collection.name)
            
            # storageSize = stats.get("storageSize", 0)
            # totalIndexSize = stats.get("totalIndexSize", 0)
            # totalSize = stats.get("totalSize", 0)   
            
            # print(storageSize, totalIndexSize, totalSize)
            
            
            

            
                    
            # Clear MongoDB collection after each full batch run   
            try:
                # pass
                # break
                mongodb_repo.delete_by_query({})
            except Exception as e:
                logger.error("An error occurred while clearing MongoDB collection: %s", e)
                raise Exception("Failed to clear MongoDB collection") from e
                
            print(f"Total insert time for batch {batch_id}: {total_insert_time_ns} ns or {total_insert_time_ns / 1_000_000_000} seconds")    
            
            print(lines)
        # TODO 5: SAVE BENCHMARK RESULTS
        #   For each run:
        #   - Build IngestionBenchmarkRun object
        #   - Build IngestionBenchmarkResult object
        #   - Call bm_db.insert_ingestion_benchmark(run, result)

        # TODO 6: TEARDOWN
        # - Disconnect MongoDBRepository
        # - Disconnect BenchmarkDB
            try:
                total_insert_time_seconds = total_insert_time_ns / 1_000_000_000
                throughput_obs_per_sec = dataset_meta.record_count / total_insert_time_seconds
                throughput_bytes_per_sec = dataset_meta.data_size_bytes / total_insert_time_seconds
                throughput_kb_per_sec = dataset_meta.data_size_kb / total_insert_time_seconds
                throughput_mb_per_sec = dataset_meta.data_size_mb / total_insert_time_seconds

                result = IngestionBenchmarkResult(
                    benchmark_name="ingestion.insert_many.fixed_batch",
                    database_system="mongodb",
                    database_version="8.0",
                    database_location="container",
                    
                    insert_batch_size=BATCH_SIZE*5,
                    batch_id=batch_id,
                    dataset_name=dataset_meta.dataset,
                    record_count=dataset_meta.record_count,
                    data_size_bytes=dataset_meta.data_size_bytes,
                    data_size_kb=dataset_meta.data_size_kb,
                    data_size_mb=dataset_meta.data_size_mb,
                    
                    elapsed_time_seconds=total_insert_time_seconds,
                    elapsed_time_ns=total_insert_time_ns,
                    throughput_obs_per_sec=throughput_obs_per_sec,
                    throughput_bytes_per_sec=throughput_bytes_per_sec,
                    throughput_kb_per_sec=throughput_kb_per_sec,
                    throughput_mb_per_sec=throughput_mb_per_sec,
                    insert_operation_count=num_iterations,
                )

                result = bm_db.insert_result(result)
                print("Inserted IngestionBenchmarkResult:", result.id)
                # print(result)
                print()
                
            except Exception as e:
                logger.error("An error occurred while saving benchmark results: %s", e)
    er = time.perf_counter()
    tt = er - st
    print(lines)
    print(f"Total time for inserting {number_of_fill_inserted_data} full data runs: {tt} seconds or {tt/60} minutes")
finally:
    mongodb_repo.disconnect()
    bm_db.disconnect()    


    












