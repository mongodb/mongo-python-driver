from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

client = MongoClient(document_class=RawBSONDocument)
coll = client.test.test
doc = {"my": "doc"}
coll.insert_one(doc)
retreived = coll.find_one({"_id": doc["_id"]})
assert retreived is not None
assert len(retreived.raw) > 0
retreived[
    "foo"
] = "bar"  # error: Unsupported target for indexed assignment ("RawBSONDocument")  [index]
