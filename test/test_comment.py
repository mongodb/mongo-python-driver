import sys
import inspect
from typing import Any, Union
from collections import defaultdict
sys.path[0:0] = [""]

from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from pymongo.command_cursor import CommandCursor
from pymongo.operations import (IndexModel)
from bson.dbref import DBRef

from test import (client_context,
                  SkipTest,
                  unittest,
                  IntegrationTest)
from test.utils import (rs_or_single_client,
                        EventListener)

class Empty(object):
    def __getattr__(self, item):
        try:
            self.__dict__[item]
        except KeyError:
            return self.empty
    def empty(self, *args, **kwargs):
        return Empty()

class TestComment(IntegrationTest):
    def _test_ops(self, helpers, already_supported, listener, db=Empty(),
                  coll=Empty()):
        results = listener.results
        for h, args in helpers:
            c = "testing comment with " + h.__name__
            with self.subTest("collection-" + h.__name__ + "-comment"):
                for cc in [c, {"key": c}, ["any", 1]]:
                    results.clear()
                    kwargs = {"comment": cc}
                    if h == coll.rename:
                        tmp = db.get_collection("temp_temp_temp").drop()
                        destruct_coll = db.get_collection("test_temp")
                        destruct_coll.insert_one({})
                        maybe_cursor = destruct_coll.rename(*args,
                                                            **kwargs)
                        destruct_coll.drop()
                    elif h == db.validate_collection:
                        coll = db.get_collection("test")
                        coll.insert_one({})
                        maybe_cursor = db.validate_collection(*args,
                                                            **kwargs)
                    else:
                        coll.create_index('a')
                        maybe_cursor = h(*args, **kwargs)
                    self.assertIn("comment", inspect.signature(
                        h).parameters,
                                  msg="Could not find 'comment' in the "
                                      "signature of function %s"
                                      % (h.__name__))
                    self.assertEqual(inspect.signature(h).parameters[
                                         "comment"].annotation, Union[Any,
                                                                      None])
                    if isinstance(maybe_cursor, CommandCursor):
                        maybe_cursor.close()
                    tested = False
                    for i in results['started']:
                        if cc == i.command.get("comment", ""):
                            tested = True
                    self.assertTrue(tested, msg=
                    "Using the keyword argument \"comment\" did "
                    "not work for func: %s with comment "
                    "type: %s" % (h.__name__, type(cc)))
                    if h not in [coll.aggregate_raw_batches]:
                        self.assertIn("`comment` (optional):",
                                      h.__doc__,
                                      msg="Could not find 'comment' in the "
                                          "docstring of function %s"
                                          % (h.__name__))
                        if h not in already_supported:
                            self.assertIn("Added ``comment`` parameter",
                                          h.__doc__,
                                          msg="Could not find 'comment' "
                                              "versionchanged in "
                                              "the "
                                              "docstring of function %s"
                                              % (h.__name__))
                        else:
                            self.assertNotIn("Added ``comment`` parameter",
                                             h.__doc__,
                                             msg="Found 'comment' "
                                                 "versionchanged in "
                                                 "the "
                                                 "docstring of function that "
                                                 "should not have it %s"
                                                 % (h.__name__))

        results.clear()


    @client_context.require_version_min(4, 7, -1)
    @client_context.require_replica_set
    def test_database_helpers(self):
        listener = EventListener()
        db = rs_or_single_client(event_listeners=[listener]).db
        helpers = [
            (db.watch, []), (db.command, ["hello"]), (db.list_collections, []),
            (db.list_collection_names, []), (db.drop_collection, ["hello"]),
            (db.validate_collection, ["test"]),
            (db.dereference, [DBRef('collection', 1)])
        ]
        already_supported = [db.command, db.list_collections, db.list_collection_names]
        self._test_ops(helpers, already_supported, listener, db=db,
                       coll=db.get_collection("test"))

    @client_context.require_version_min(4, 7, -1)
    @client_context.require_replica_set
    def test_client_helpers(self):
        listener = EventListener()
        cli = rs_or_single_client(event_listeners=[listener])
        helpers = [
            (cli.watch, []), (cli.list_databases, []),
            (cli.list_database_names, []),
            (cli.drop_database, ["test"]),
        ]
        already_supported = [
            cli.list_databases,
        ]
        self._test_ops(helpers, already_supported, listener)

    @client_context.require_auth
    @client_context.require_version_min(4, 7, -1)
    def test_collection_helpers(self):
        listener = EventListener()
        db = rs_or_single_client(event_listeners=[listener])[self.db.name]
        coll = db.get_collection("test")

        helpers = [
            (coll.list_indexes, []), (coll.drop, []),
            (coll.index_information, []),
            (coll.options, []),
            (coll.aggregate, [[{"$set": {"x": 1}}]]),
            (coll.aggregate_raw_batches, [[{"$set": {"x": 1}}]]),
            (coll.rename, ["temp_temp_temp"]), (coll.distinct, ["_id"]),
            (coll.find_one_and_delete, [{}]),
            (coll.find_one_and_replace, [{}, {}]),
            (coll.find_one_and_update, [{}, {'$set': {'a': 1}}]),
            (coll.estimated_document_count, []), (coll.count_documents, [{}]),
            (coll.create_indexes, [[IndexModel("a")]]),
            (coll.create_index, ["a"]), (coll.drop_index, [[('a', 1)]]),
            (coll.drop_indexes, []),
        ]
        already_supported = [
            coll.estimated_document_count, coll.count_documents,
            coll.create_indexes, coll.drop_indexes, coll.options,
            coll.find_one_and_replace, coll.drop_index, coll.rename,
            coll.distinct, coll.find_one_and_delete, coll.find_one_and_update,
        ]
        self._test_ops(helpers, already_supported, listener, coll=coll, db=db)

if __name__ == "__main__":
    unittest.main()
