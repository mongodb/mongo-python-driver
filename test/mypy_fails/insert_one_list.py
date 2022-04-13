from pymongo import MongoClient

client: MongoClient = MongoClient()
client.test.test.insert_one(
    [{}]
)  # error: Argument 1 to "insert_one" of "Collection" has incompatible type "List[Dict[<nothing>, <nothing>]]"; expected "Mapping[str, Any]"
