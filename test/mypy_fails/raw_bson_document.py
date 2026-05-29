from __future__ import annotations

from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

client = MongoClient(document_class=RawBSONDocument)
coll = client.test.test
doc = {"my": "doc"}
coll.insert_one(doc)
retrieved = coll.find_one({"_id": doc["_id"]})
assert retrieved is not None
assert len(retrieved.raw) > 0
retrieved[
    "foo"
] = "bar"  # error: Unsupported target for indexed assignment ("RawBSONDocument")  [index]
