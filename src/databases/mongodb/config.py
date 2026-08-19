class MongoDBConfigurationException(Exception):
    pass


class MongoDBConfig:
    def __init__(self, username, password, cluster, host, port, database, collection, mode):
        self.MONGODB_USER = username
        self.MONGODB_PASSWORD = password

        # for MongoDB Atlas connections
        self.MONGODB_CLUSTER = cluster

        # for local/container connections
        self.MONGODB_HOST = host
        self.MONGODB_PORT = port

        self.MONGODB_DATABASE_NAME = database
        self.MONGODB_COLLECTION_NAME = collection

        self.MONGODB_MODE = mode  # localhost | container | atlas

    def __str__(self):
        return (
            "MongoDB Configuration:\n"
            f"Mode: {self.MONGODB_MODE}\n"

            f"Cluster: {self.MONGODB_CLUSTER}\n"

            f"Host: {self.MONGODB_HOST}\n"
            f"Port: {self.MONGODB_PORT}\n"

            f"Database: {self.MONGODB_DATABASE_NAME}\n"
            f"Collection: {self.MONGODB_COLLECTION_NAME}\n"
        )


def get_mongodb_config(config_dict: dict) -> MongoDBConfig:
    try:
        return MongoDBConfig(
            username=config_dict["MONGODB_USER"],
            password=config_dict["MONGODB_PASSWORD"],

            cluster=config_dict["MONGODB_CLUSTER"],

            host=config_dict["MONGODB_HOST"],
            port=config_dict["MONGODB_PORT"],

            database=config_dict["MONGODB_DATABASE_NAME"],
            collection=config_dict["MONGODB_COLLECTION_NAME"],

            mode=config_dict["MONGODB_MODE"],
        )
    except KeyError as e:
        raise MongoDBConfigurationException(f"Missing config key: {e}")
