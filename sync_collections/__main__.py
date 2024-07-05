from os import environ

from .database import MongoCluster

print(f"Source: {environ['SOURCE_MONGODB_URI']}")
print(f"Source: {environ['SOURCE_DATABASE']}")
print(f"Dest: {environ['DEST_MONGODB_URI']}")
print(f"Dest: {environ['DEST_MONGODB_DATABASE']}")

source_db_cluster = MongoCluster(connect_uri=environ["SOURCE_MONGODB_URI"])
dest_db_cluster = MongoCluster(connect_uri=environ["DEST_MONGODB_URI"])

excluded_collections = ["users"]


def sync_data(
    source_cluster: MongoCluster, dest_cluster: MongoCluster, drop_dest_collection=False
):
    source_db = source_cluster.use_database(environ["SOURCE_DATABASE"])
    dest_db = dest_cluster.use_database(environ["DEST_MONGODB_DATABASE"])
    print(
        f"Syncing  | [Database] ({environ['SOURCE_DATABASE']}) -> ({environ['DEST_MONGODB_DATABASE']})"
    )

    for collection_name in source_db.list_collection_names():
        if collection_name in excluded_collections:
            continue
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
