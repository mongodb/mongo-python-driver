from typing import TypedDict

from pymongo import MongoClient


class Movie(TypedDict):
    name: str
    year: int


client: MongoClient[Movie] = MongoClient()
coll = client.test.test
retreived = coll.find_one({"_id": "foo"})
assert retreived is not None
assert retreived["year"] == 1
assert (
    retreived["name"] == 2
)  # error: Non-overlapping equality check (left operand type: "str", right operand type: "Literal[2]")  [comparison-overlap]
