from typing import Tuple

from pymongo import MongoClient


class CursorManager(object):
    def __init__(self, client: MongoClient) -> None: ...
    def close(self, cursor_id: int, address: Tuple[str, int]) -> None: ...
