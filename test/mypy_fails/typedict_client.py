from typing import TypedDict

from pymongo import MongoClient


class Movie(TypedDict):
    name: str
    year: int


client = MongoClient(
    document_class=Movie
)  # Value of type variable "_DocumentType" of "MongoClient" cannot be "Movie"  [type-var]
