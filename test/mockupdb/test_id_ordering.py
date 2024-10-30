from __future__ import annotations

from test import PyMongoTestCase

import pytest

from pymongo import InsertOne

try:
    from mockupdb import MockupDB, OpMsg, go, going

    _HAVE_MOCKUPDB = True
except ImportError:
    _HAVE_MOCKUPDB = False


from bson.objectid import ObjectId

pytestmark = pytest.mark.mockupdb


class TestIdOrdering(PyMongoTestCase):
    def test_id_ordering(self):
        server = MockupDB()
        server.autoresponds(
            "hello",
            isWritablePrimary=True,
            msg="isdbgrid",
            minWireVersion=0,
            maxWireVersion=25,
            helloOk=True,
            serviceId=ObjectId(),
        )
        server.run()
        self.addCleanup(server.stop)

        client = self.simple_client(server.uri, loadBalanced=True)
        collection = client.db.coll
        with going(collection.insert_one, {"x": 1}):
            request = server.receives()
            self.assertEqual("_id", next(iter(request["documents"][0])))
            request.reply({"ok": 1})

        with going(collection.bulk_write, [InsertOne({"x1": 1})]):
            request = server.receives()
            self.assertEqual("_id", next(iter(request["documents"][0])))
            request.reply({"ok": 1})

        with going(client.bulk_write, [InsertOne(namespace="db.coll", document={"x2": 1})]):
            request = server.receives()
            self.assertEqual("_id", next(iter(request["ops"][0]["document"])))
            request.reply({"ok": 1})

        # Re-ordering user-supplied _id fields is not required by the spec, but PyMongo does it for performance reasons
        with going(collection.insert_one, {"x": 1, "_id": 111}):
            request = server.receives()
            self.assertEqual("_id", next(iter(request["documents"][0])))
            request.reply({"ok": 1})

        with going(collection.bulk_write, [InsertOne({"x1": 1, "_id": 1111})]):
            request = server.receives()
            self.assertEqual("_id", next(iter(request["documents"][0])))
            request.reply({"ok": 1})

        with going(
            client.bulk_write, [InsertOne(namespace="db.coll", document={"x2": 1, "_id": 11111})]
        ):
            request = server.receives()
            self.assertEqual("_id", next(iter(request["ops"][0]["document"])))
            request.reply({"ok": 1})
