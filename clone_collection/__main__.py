from os import environ

from .database import MongoCluster

print(f"Source: {environ['SOURCE_MONGODB_URI']}")
print(f"Source: {environ['SOURCE_DATABASE']}")

source_db_cluster = MongoCluster(connect_uri=environ["SOURCE_MONGODB_URI"])


def clone(
    source_cluster: MongoCluster,
    source_coll_name: str,
    dest_coll_name: str,
    drop_dest_collection=False,
):
    source_db = source_cluster.use_database(environ["SOURCE_DATABASE"])
    print(
        f"Syncing  | [Database] ({environ['SOURCE_DATABASE']}) -> ({environ['DEST_MONGODB_DATABASE']})"
    )
    source_collection = source_db.get_collection(source_coll_name)
    dest_collection = source_db.get_collection(dest_coll_name)
    print(f"Syncing  | [collection]: {source_coll_name} -> {dest_coll_name}")
    if drop_dest_collection:
        dest_collection.drop()

    if source_collection.count_documents({}) > 0:
        dest_collection.insert_many(source_collection.find(), ordered=False)
    else:
        source_db.create_collection(dest_coll_name)


for coll in ["ledger_templates", "ledgers", "users"]:
    clone(
        source_cluster=source_db_cluster,
        source_coll_name=coll,
        dest_coll_name=f"{coll}_v3",
        drop_dest_collection=True,  # If you want to have idempotence, set this to True
    )
