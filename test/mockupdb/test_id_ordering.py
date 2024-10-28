from __future__ import annotations

from test import PyMongoTestCase

import pytest

try:
    from mockupdb import MockupDB, OpMsg, going

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
            maxWireVersion=20,
            helloOk=True,
            serviceId=ObjectId(),
        )
        server.run()
        self.addCleanup(server.stop)

        client = self.simple_client(server.uri, loadBalanced=True)
        collection = client.db.coll
        with going(collection.insert_one, {"x": 1}):
            request = server.receives(OpMsg({"insert": "coll"}))
            self.assertEqual("_id", next(iter(request["documents"][0])))
            request.reply({"ok": 1})
