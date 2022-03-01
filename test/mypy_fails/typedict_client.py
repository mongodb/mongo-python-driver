from typing import TypedDict

from pymongo import MongoClient


class Movie(TypedDict):
    name: str
    year: int


client: MongoClient[Movie] = MongoClient(document_class=Movie)
doc = client.test.test.find_one({})
if doc is not None:
    doc["name"] = "LOTR"
    doc[
        "year"
    ] = "2005"  # Value of "year" has incompatible type "str"; expected "int"  [typeddict-item]
