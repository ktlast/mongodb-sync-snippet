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
    source_cluster: MongoCluster,
    dest_cluster: MongoCluster,
    source_coll_name: str,
    drop_dest_collection=False,
):
    source_db = source_cluster.use_database(environ["SOURCE_DATABASE"])
    dest_db = dest_cluster.use_database(environ["DEST_MONGODB_DATABASE"])
    print(
        f"Syncing  | [Database] ({environ['SOURCE_DATABASE']}) -> ({environ['DEST_MONGODB_DATABASE']})"
    )

    source_collection = source_db.get_collection(source_coll_name)
    for version in range(1, 10):
        counts_of_version = source_collection.count_documents({"v": version})
        print(f"Version {version} has {counts_of_version} documents")
        if counts_of_version > 0:
            dest_coll_name = f"{source_coll_name}_v{version}"
            dest_db.create_collection(dest_coll_name)
            dest_collection = dest_db.get_collection(dest_coll_name)
            if drop_dest_collection:
                print(f"Dropping  | [collection]: {source_coll_name}_v{version}")
                dest_collection.drop()
            print(f"Syncing  | [collection]: {source_coll_name}_v{version}")
            dest_collection.insert_many(source_collection.find(), ordered=False)


sync_data(
    source_cluster=source_db_cluster,
    dest_cluster=dest_db_cluster,
    source_coll_name="ledgers",
    drop_dest_collection=False,  # If you want to have idempotence, set this to True
)
