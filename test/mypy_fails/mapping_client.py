from typing import Any, Mapping

from pymongo import MongoClient


class MyMapping(Mapping[str, Any]):
    pass


client: MongoClient[MyMapping] = MongoClient(
    document_class=MyMapping
)  # Value of type variable "_DocumentType" of "MongoClient" cannot be "MyMapping"  [type-var]
