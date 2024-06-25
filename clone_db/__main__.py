from os import environ

from .database import MongoCluster

print("Source: {environ['SOURCE_MONGODB_URI']}")
print("Source: {environ['SOURCE_DATABASE']}")
print("Dest: {environ['DEST_MONGODB_URI']}")
print("Dest: {environ['DEST_MONGODB_DATABASE']}")

source_db_cluster = MongoCluster(connect_uri=environ["SOURCE_MONGODB_URI"])
dest_db_cluster = MongoCluster(connect_uri=environ["DEST_MONGODB_URI"])


def sync_data(
    source_cluster: MongoCluster, dest_cluster: MongoCluster, drop_dest_collection=False
):
    source_db = source_cluster.use_database(environ["SOURCE_DATABASE"])
    dest_db = dest_cluster.use_database(environ["DEST_MONGODB_DATABASE"])
    print(f"Syncing  | [Database] ({source_db}) -> ({dest_db})")

    for collection_name in source_db.list_collection_names():
        source_collection = source_db.get_collection(collection_name)
        dest_collection = dest_db.get_collection(collection_name)
        print(f"Syncing  | [collection]: {collection_name}")
        if drop_dest_collection:
            dest_collection.drop()

        if source_collection.count_documents({}) > 0:
            dest_collection.insert_many(source_collection.find(), ordered=False)
        else:
            dest_db.create_collection(collection_name)


sync_data(
    source_cluster=source_db_cluster,
    dest_cluster=dest_db_cluster,
    drop_dest_collection=False,  # If you want to have idempotence, set this to True
)
