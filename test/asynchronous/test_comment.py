# Copyright 2022-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the keyword argument 'comment' in various helpers."""

from __future__ import annotations

import inspect
import sys

sys.path[0:0] = [""]
from asyncio import iscoroutinefunction
from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.utils import EventListener

from bson.dbref import DBRef
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.operations import IndexModel

_IS_SYNC = False


class Empty:
    def __getattr__(self, item):
        try:
            self.__dict__[item]
        except KeyError:
            return self.empty

    def empty(self, *args, **kwargs):
        return Empty()


class AsyncTestComment(AsyncIntegrationTest):
    async def _test_ops(
        self,
        helpers,
        already_supported,
        listener,
        db=Empty(),  # noqa: B008
        coll=Empty(),  # noqa: B008
    ):
        for h, args in helpers:
            c = "testing comment with " + h.__name__
            with self.subTest("collection-" + h.__name__ + "-comment"):
                for cc in [c, {"key": c}, ["any", 1]]:
                    listener.reset()
                    kwargs = {"comment": cc}
                    if h == coll.rename:
                        await db.get_collection("temp_temp_temp").drop()
                        destruct_coll = db.get_collection("test_temp")
                        await destruct_coll.insert_one({})
                        maybe_cursor = await destruct_coll.rename(*args, **kwargs)
                        await destruct_coll.drop()
                    elif h == db.validate_collection:
                        coll = db.get_collection("test")
                        await coll.insert_one({})
                        maybe_cursor = await db.validate_collection(*args, **kwargs)
                    else:
                        if not _IS_SYNC and isinstance(coll, Empty):
                            coll.create_index("a")
                        else:
                            await coll.create_index("a")
                        if not _IS_SYNC and iscoroutinefunction(h):
                            maybe_cursor = await h(*args, **kwargs)
                        else:
                            maybe_cursor = h(*args, **kwargs)
                    self.assertIn(
                        "comment",
                        inspect.signature(h).parameters,
                        msg="Could not find 'comment' in the "
                        "signature of function %s" % (h.__name__),
                    )
                    self.assertEqual(
                        inspect.signature(h).parameters["comment"].annotation, "Optional[Any]"
                    )
                    if isinstance(maybe_cursor, AsyncCommandCursor):
                        await maybe_cursor.close()
                    tested = False
                    # For some reason collection.list_indexes creates two commands and the first
                    # one doesn't contain 'comment'.
                    for i in listener.started_events:
                        if cc == i.command.get("comment", ""):
                            self.assertEqual(cc, i.command["comment"])
                            tested = True
                    self.assertTrue(tested)
                    if h not in [coll.aggregate_raw_batches]:
                        self.assertIn(
                            ":param comment:",
                            h.__doc__,
                        )
                        if h not in already_supported:
                            self.assertIn(
                                "Added ``comment`` parameter",
                                h.__doc__,
                            )
                        else:
                            self.assertNotIn(
                                "Added ``comment`` parameter",
                                h.__doc__,
                            )

        listener.reset()

    @async_client_context.require_version_min(4, 7, -1)
    @async_client_context.require_replica_set
    async def test_database_helpers(self):
        listener = EventListener()
        db = (await self.async_rs_or_single_client(event_listeners=[listener])).db
        helpers = [
            (db.watch, []),
            (db.command, ["hello"]),
            (db.list_collections, []),
            (db.list_collection_names, []),
            (db.drop_collection, ["hello"]),
            (db.validate_collection, ["test"]),
            (db.dereference, [DBRef("collection", 1)]),
        ]
        already_supported = [db.command, db.list_collections, db.list_collection_names]
        await self._test_ops(
            helpers, already_supported, listener, db=db, coll=db.get_collection("test")
        )

    @async_client_context.require_version_min(4, 7, -1)
    @async_client_context.require_replica_set
    async def test_client_helpers(self):
        listener = EventListener()
        cli = await self.async_rs_or_single_client(event_listeners=[listener])
        helpers = [
            (cli.watch, []),
            (cli.list_databases, []),
            (cli.list_database_names, []),
            (cli.drop_database, ["test"]),
        ]
        already_supported = [
            cli.list_databases,
        ]
        await self._test_ops(helpers, already_supported, listener)

    @async_client_context.require_version_min(4, 7, -1)
    async def test_collection_helpers(self):
        listener = EventListener()
        db = (await self.async_rs_or_single_client(event_listeners=[listener]))[self.db.name]
        coll = db.get_collection("test")

        helpers = [
            (coll.list_indexes, []),
            (coll.drop, []),
            (coll.index_information, []),
            (coll.options, []),
            (coll.aggregate, [[{"$set": {"x": 1}}]]),
            (coll.aggregate_raw_batches, [[{"$set": {"x": 1}}]]),
            (coll.rename, ["temp_temp_temp"]),
            (coll.distinct, ["_id"]),
            (coll.find_one_and_delete, [{}]),
            (coll.find_one_and_replace, [{}, {}]),
            (coll.find_one_and_update, [{}, {"$set": {"a": 1}}]),
            (coll.estimated_document_count, []),
            (coll.count_documents, [{}]),
            (coll.create_indexes, [[IndexModel("a")]]),
            (coll.create_index, ["a"]),
            (coll.drop_index, [[("a", 1)]]),
            (coll.drop_indexes, []),
        ]
        already_supported = [
            coll.estimated_document_count,
            coll.count_documents,
            coll.create_indexes,
            coll.drop_indexes,
            coll.options,
            coll.find_one_and_replace,
            coll.drop_index,
            coll.rename,
            coll.distinct,
            coll.find_one_and_delete,
            coll.find_one_and_update,
        ]
        await self._test_ops(helpers, already_supported, listener, coll=coll, db=db)


if __name__ == "__main__":
    unittest.main()
