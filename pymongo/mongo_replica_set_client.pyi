from typing import Any

from pymongo import mongo_client


class MongoReplicaSetClient(mongo_client.MongoClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def __repr__(self) -> str: ...
