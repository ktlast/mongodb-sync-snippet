from typing import Any

from pymongo import MongoClient


class MongoCluster:
    def __init__(self, connect_uri) -> None:
        self.connect_uri = connect_uri
        self.client = MongoClient(
            host=self.connect_uri,
            document_class=dict[str, Any],
            tz_aware=False,
            connect=True,
        )
        self.database = None

    def list_databases(self):
        return self.client.list_database_names()

    def use_database(self, database_name):
        return self.client.get_database(database_name)

    def use_collection(self, database_name, collection_name):
        self.database = self.use_database(database_name)
        return self.database.get_collection(collection_name)
