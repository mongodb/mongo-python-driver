from __future__ import annotations

from typing import TypedDict

from pymongo import MongoClient


class Movie(TypedDict):
    name: str
    year: int


client: MongoClient[Movie] = MongoClient()
coll = client.test.test
retrieved = coll.find_one({"_id": "foo"})
assert retrieved is not None
assert retrieved["year"] == 1
assert (
    retrieved["name"] == 2
)  # error: Non-overlapping equality check (left operand type: "str", right operand type: "Literal[2]")  [comparison-overlap]
