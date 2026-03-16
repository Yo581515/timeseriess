import logging

from src.databases.mongodb.config import MongoDBConfig
from src.databases.mongodb.client import MongoDBClient

import time



class MongoDBRepository(MongoDBClient):
    """
    Repo = Client + CRUD.
    Benchmark tip: call connect_and_cache() once, then use repo.collection (or get_collection())
    inside the benchmark loop to minimize Python overhead in timed sections.
    """

    def __init__(self, mongodb_config : MongoDBConfig, logger: logging.Logger):
        super().__init__(mongodb_config, logger)
        self.db = None
        self.collection = None

  
    def connect_and_cache(self) -> bool:
        """
        Connect and cache db/collection objects for fast use in benchmarks.
        """
        if not self.connect():
            self.logger.error("connect_and_cache(): connect() failed")
            return False
        
        if self.database_name is None:
            self.logger.error("connect_and_cache(): database_name is None")
            raise ValueError("database_name is required")
        
        if self.collection_name is None:
            self.logger.error("connect_and_cache(): collection_name is None")
            raise ValueError("collection_name is required")
        
        try:
            self.db = self.client[self.database_name]
        except Exception as e:
            self.logger.exception("Error accessing database '%s': %s", self.database_name, e)
            raise RuntimeError(f"Failed to access database '{self.database_name}'") from e
        
        try:
            self.collection = self.db[self.collection_name]
        except Exception as e:
            self.logger.exception("Error accessing collection '%s': %s", self.collection_name, e)
            raise RuntimeError(f"Failed to access collection '{self.collection_name}'") from e
            
        
        

        self.logger.info("Cached db/collection handles (db=%s, collection=%s)", self.database_name, self.collection_name)
        return True
    
    
    def disconnect(self) -> bool:
        self.db = None
        self.collection = None
        return super().disconnect()

    def get_collection(self):
        """
        Use this to grab the collection once BEFORE starting your timer.
        """
        if self.collection is None:
            raise RuntimeError("Collection not cached. Call connect_and_cache() first.")
        return self.collection


    def ping(self) -> bool:
        if self.client is None:
            self.logger.error("ping(): not connected")
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")
        try:
            self.client.admin.command("ping")
            self.logger.info("Ping OK.")
            return True
        except Exception as e:
            self.logger.exception("Ping error: %s", e)
            raise RuntimeError("Ping failed.") from e

   
   
    def insert_one(self, doc: dict) -> bool:
        if not doc:
            self.logger.error("insert_one(): empty doc")
            raise ValueError("Document cannot be empty")

        if self.collection is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            self.collection.insert_one(doc)
            return True
        except Exception as e:
            self.logger.exception("insert_one() error: %s", e)
            raise RuntimeError("Failed to insert document.") from e
        
        
    def insert_many(self, docs: list[dict]) -> tuple[bool, float]:
        if not docs:
            self.logger.error("insert_many(): empty docs")
            raise ValueError("Documents cannot be empty")

        if self.collection is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            start_time = time.perf_counter_ns()
            self.collection.insert_many(docs)
            end_time = time.perf_counter_ns()
            elapsed_time_nanoseconds = end_time - start_time
            return True, elapsed_time_nanoseconds
        except Exception as e:
            self.logger.exception("insert_many() error: %s", e)
            raise RuntimeError("Failed to insert documents.") from e

  
  
    def find_by_query(self, query: dict) -> list[dict]:
        if query is None:
            self.logger.error("find_by_query(): query is None")
            raise ValueError("Query cannot be None")

        if self.collection is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            return list(self.collection.find(query))
        except Exception as e:
            self.logger.exception("find_by_query() error: %s", e)
            raise RuntimeError("Failed to find documents.") from e


    def aggregate(self, pipeline: list[dict]) -> list[dict]:
        if not pipeline:
            self.logger.error("aggregate(): empty pipeline")
            return []

        if self.collection is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            return list(self.collection.aggregate(pipeline))
        except Exception as e:
            self.logger.exception("aggregate() error: %s", e)
            raise RuntimeError("Failed to aggregate documents.") from e

        
    
    def delete_by_query(self, query: __dict__) -> bool:
        if query is None:
            self.logger.error("delete_by_query(): query is None")
            raise ValueError("Query cannot be None")

        if self.collection is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            self.collection.delete_many(query)
            return True
        except Exception as e:
            self.logger.exception("delete_by_query() error: %s", e)
            raise RuntimeError("Failed to delete documents.") from e