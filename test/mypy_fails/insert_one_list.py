from __future__ import annotations

from pymongo import MongoClient

client: MongoClient = MongoClient()
# error: Argument 1 to "insert_one" of "Collection" has incompatible type
# "List[Dict[<nothing>, <nothing>]]"; expected "Mapping[str, Any]"
client.db.coll.insert_one([{}])
