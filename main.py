from database import MongoCluster

EXAMPLE_TOKYO_USER = "tokyo_user"
EXAMPLE_TOKYO_PASSWORD = "tokyo_password"
EXAMPLE_TOKYO_CLUSTER = "tokyo_cluster"

EXAMPLE_SINGAPORE_USER = "singapore_user"
EXAMPLE_SINGAPORE_PASSWORD = "singapore_password"
EXAMPLE_SINGAPORE_CLUSTER = "singapore_cluster"

# Suppose you have 2 MongoDB clusters, one in Tokyo and another in Singapore.
tokyo_cluster = MongoCluster(
    connect_uri=f"mongodb+srv://{EXAMPLE_TOKYO_USER}:{EXAMPLE_TOKYO_PASSWORD}@{EXAMPLE_TOKYO_CLUSTER}/"
)

singapore_cluster = MongoCluster(
    connect_uri=f"mongodb+srv://{EXAMPLE_SINGAPORE_USER}:{EXAMPLE_SINGAPORE_PASSWORD}@{EXAMPLE_SINGAPORE_CLUSTER}/"
)


def sync_data(
    source_cluster: MongoCluster, dest_cluster: MongoCluster, drop_dest_collection=False
):
    for original_db_name in source_cluster.list_databases():
        if original_db_name in ["admin", "local", "config"]:
            print(f"Skipping | [Database] ({original_db_name})")
            continue

        source_db = source_cluster.use_database(original_db_name)
        dest_db = dest_cluster.use_database(original_db_name)
        print(f"Syncing  | [Database] ({original_db_name})")

        for collection_name in source_db.list_collection_names():
            source_collection = source_db.get_collection(collection_name)
            dest_collection = dest_db.get_collection(collection_name)
            print(f"Syncing  | [collection] ({original_db_name}):  {collection_name}")
            if drop_dest_collection:
                dest_collection.drop()

            if source_collection.count_documents({}) > 0:
                dest_collection.insert_many(source_collection.find(), ordered=False)
            else:
                dest_db.create_collection(collection_name)


sync_data(
    source_cluster=tokyo_cluster,
    dest_cluster=singapore_cluster,
    drop_dest_collection=False,  # If you want to have idempotence, set this to True
)
