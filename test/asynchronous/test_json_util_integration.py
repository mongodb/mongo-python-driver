from __future__ import annotations

from test.asynchronous import AsyncIntegrationTest
from typing import Any, List, MutableMapping

from bson import Binary, Code, DBRef, ObjectId, json_util
from bson.binary import USER_DEFINED_SUBTYPE

_IS_SYNC = False


class TestJsonUtilRoundtrip(AsyncIntegrationTest):
    async def test_cursor(self):
        db = self.db

        await db.drop_collection("test")
        docs: List[MutableMapping[str, Any]] = [
            {"foo": [1, 2]},
            {"bar": {"hello": "world"}},
            {"code": Code("function x() { return 1; }")},
            {"bin": Binary(b"\x00\x01\x02\x03\x04", USER_DEFINED_SUBTYPE)},
            {"dbref": {"_ref": DBRef("simple", ObjectId("509b8db456c02c5ab7e63c34"))}},
        ]

        await db.test.insert_many(docs)
        reloaded_docs = json_util.loads(json_util.dumps(await (db.test.find()).to_list()))
        for doc in docs:
            self.assertIn(doc, reloaded_docs)
