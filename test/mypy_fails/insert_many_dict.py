from pymongo import MongoClient

client: MongoClient = MongoClient()
client.test.test.insert_many(
    {"a": 1}
)  # error: Dict entry 0 has incompatible type "str": "int"; expected "Mapping[str, Any]": "int"
