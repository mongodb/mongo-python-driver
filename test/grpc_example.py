from pymongo import MongoClient

client = MongoClient("mongodb://host9.local.10gen.cc:9901", grpc=True, loadBalanced=True)

dbs = client.list_databases()
for db in dbs:
    print(db["name"], ": ", client[db["name"]].list_collection_names())
