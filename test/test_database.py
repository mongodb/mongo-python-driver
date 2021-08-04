# Copyright 2009-present MongoDB, Inc.
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

"""Test the database module."""

import datetime
import re
import sys

sys.path[0:0] = [""]

from bson.codec_options import CodecOptions
from bson.int64 import Int64
from bson.regex import Regex
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.son import SON
from pymongo import (ALL,
                     auth,
                     OFF,
                     SLOW_ONLY,
                     helpers)
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (CollectionInvalid,
                            ConfigurationError,
                            ExecutionTimeout,
                            InvalidName,
                            OperationFailure,
                            WriteConcernError)
from pymongo.mongo_client import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from test import (client_context,
                  SkipTest,
                  unittest,
                  IntegrationTest)
from test.utils import (ignore_deprecations,
                        rs_or_single_client,
                        server_started_with_auth,
                        wait_until,
                        DeprecationFilter,
                        IMPOSSIBLE_WRITE_CONCERN,
                        OvertCommandListener)
from test.test_custom_types import DECIMAL_CODECOPTS


class TestDatabaseNoConnect(unittest.TestCase):
    """Test Database features on a client that does not connect.
    """

    @classmethod
    def setUpClass(cls):
        cls.client = MongoClient(connect=False)

    def test_name(self):
        self.assertRaises(TypeError, Database, self.client, 4)
        self.assertRaises(InvalidName, Database, self.client, "my db")
        self.assertRaises(InvalidName, Database, self.client, 'my"db')
        self.assertRaises(InvalidName, Database, self.client, "my\x00db")
        self.assertRaises(InvalidName, Database,
                          self.client, "my\u0000db")
        self.assertEqual("name", Database(self.client, "name").name)

    def test_get_collection(self):
        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        read_concern = ReadConcern('majority')
        coll = self.client.pymongo_test.get_collection(
            'foo', codec_options, ReadPreference.SECONDARY, write_concern,
            read_concern)
        self.assertEqual('foo', coll.name)
        self.assertEqual(codec_options, coll.codec_options)
        self.assertEqual(ReadPreference.SECONDARY, coll.read_preference)
        self.assertEqual(write_concern, coll.write_concern)
        self.assertEqual(read_concern, coll.read_concern)

    def test_getattr(self):
        db = self.client.pymongo_test
        self.assertTrue(isinstance(db['_does_not_exist'], Collection))

        with self.assertRaises(AttributeError) as context:
            db._does_not_exist

        # Message should be: "AttributeError: Database has no attribute
        # '_does_not_exist'. To access the _does_not_exist collection,
        # use database['_does_not_exist']".
        self.assertIn("has no attribute '_does_not_exist'",
                      str(context.exception))

    def test_iteration(self):
        self.assertRaises(TypeError, next, self.client.pymongo_test)


class TestDatabase(IntegrationTest):

    def test_equality(self):
        self.assertNotEqual(Database(self.client, "test"),
                            Database(self.client, "mike"))
        self.assertEqual(Database(self.client, "test"),
                         Database(self.client, "test"))

        # Explicitly test inequality
        self.assertFalse(Database(self.client, "test") !=
                         Database(self.client, "test"))

    def test_hashable(self):
        self.assertIn(self.client.test, {Database(self.client, "test")})

    def test_get_coll(self):
        db = Database(self.client, "pymongo_test")
        self.assertEqual(db.test, db["test"])
        self.assertEqual(db.test, Collection(db, "test"))
        self.assertNotEqual(db.test, Collection(db, "mike"))
        self.assertEqual(db.test.mike, db["test.mike"])

    def test_repr(self):
        self.assertEqual(repr(Database(self.client, "pymongo_test")),
                         "Database(%r, %s)" % (self.client,
                                               repr("pymongo_test")))

    def test_create_collection(self):
        db = Database(self.client, "pymongo_test")

        db.test.insert_one({"hello": "world"})
        self.assertRaises(CollectionInvalid, db.create_collection, "test")

        db.drop_collection("test")

        self.assertRaises(TypeError, db.create_collection, 5)
        self.assertRaises(TypeError, db.create_collection, None)
        self.assertRaises(InvalidName, db.create_collection, "coll..ection")

        test = db.create_collection("test")
        self.assertTrue("test" in db.list_collection_names())
        test.insert_one({"hello": "world"})
        self.assertEqual(db.test.find_one()["hello"], "world")

        db.drop_collection("test.foo")
        db.create_collection("test.foo")
        self.assertTrue("test.foo" in db.list_collection_names())
        self.assertRaises(CollectionInvalid, db.create_collection, "test.foo")

    def test_list_collection_names(self):
        db = Database(self.client, "pymongo_test")
        db.test.insert_one({"dummy": "object"})
        db.test.mike.insert_one({"dummy": "object"})

        colls = db.list_collection_names()
        self.assertTrue("test" in colls)
        self.assertTrue("test.mike" in colls)
        for coll in colls:
            self.assertTrue("$" not in coll)

        db.systemcoll.test.insert_one({})
        no_system_collections = db.list_collection_names(
                filter={"name": {"$regex": r"^(?!system\.)"}})
        for coll in no_system_collections:
            self.assertTrue(not coll.startswith("system."))
        self.assertIn("systemcoll.test", no_system_collections)

        # Force more than one batch.
        db = self.client.many_collections
        for i in range(101):
            db["coll" + str(i)].insert_one({})
        # No Error
        try:
            db.list_collection_names()
        finally:
            self.client.drop_database("many_collections")

    def test_list_collection_names_filter(self):
        listener = OvertCommandListener()
        results = listener.results
        client = rs_or_single_client(event_listeners=[listener])
        db = client[self.db.name]
        db.capped.drop()
        db.create_collection("capped", capped=True, size=4096)
        db.capped.insert_one({})
        db.non_capped.insert_one({})
        self.addCleanup(client.drop_database, db.name)

        # Should not send nameOnly.
        for filter in ({'options.capped': True},
                       {'options.capped': True, 'name': 'capped'}):
            results.clear()
            names = db.list_collection_names(filter=filter)
            self.assertEqual(names, ["capped"])
            self.assertNotIn("nameOnly", results["started"][0].command)

        # Should send nameOnly (except on 2.6).
        for filter in (None, {}, {'name': {'$in': ['capped', 'non_capped']}}):
            results.clear()
            names = db.list_collection_names(filter=filter)
            self.assertIn("capped", names)
            self.assertIn("non_capped", names)
            command = results["started"][0].command
            if client_context.version >= (3, 0):
                self.assertIn("nameOnly", command)
                self.assertTrue(command["nameOnly"])
            else:
                self.assertNotIn("nameOnly", command)

    def test_list_collections(self):
        self.client.drop_database("pymongo_test")
        db = Database(self.client, "pymongo_test")
        db.test.insert_one({"dummy": "object"})
        db.test.mike.insert_one({"dummy": "object"})

        results = db.list_collections()
        colls = [result["name"] for result in results]

        # All the collections present.
        self.assertTrue("test" in colls)
        self.assertTrue("test.mike" in colls)

        # No collection containing a '$'.
        for coll in colls:
            self.assertTrue("$" not in coll)

        # Duplicate check.
        coll_cnt = {}
        for coll in colls:
            try:
                # Found duplicate.
                coll_cnt[coll] += 1
                self.assertTrue(False)
            except KeyError:
                coll_cnt[coll] = 1
        coll_cnt = {}

        # Checking if is there any collection which don't exists.
        if (len(set(colls) - set(["test","test.mike"])) == 0 or
            len(set(colls) - set(["test","test.mike","system.indexes"])) == 0):
            self.assertTrue(True)
        else:
            self.assertTrue(False)

        colls = db.list_collections(filter={"name": {"$regex": "^test$"}})
        self.assertEqual(1, len(list(colls)))

        colls = db.list_collections(filter={"name": {"$regex": "^test.mike$"}})
        self.assertEqual(1, len(list(colls)))

        db.drop_collection("test")

        db.create_collection("test", capped=True, size=4096)
        results = db.list_collections(filter={'options.capped': True})
        colls = [result["name"] for result in results]

        # Checking only capped collections are present
        self.assertTrue("test" in colls)
        self.assertFalse("test.mike" in colls)

        # No collection containing a '$'.
        for coll in colls:
            self.assertTrue("$" not in coll)

        # Duplicate check.
        coll_cnt = {}
        for coll in colls:
            try:
                # Found duplicate.
                coll_cnt[coll] += 1
                self.assertTrue(False)
            except KeyError:
                coll_cnt[coll] = 1
        coll_cnt = {}

        # Checking if is there any collection which don't exists.
        if (len(set(colls) - set(["test"])) == 0 or
            len(set(colls) - set(["test","system.indexes"])) == 0):
            self.assertTrue(True)
        else:
            self.assertTrue(False)

        self.client.drop_database("pymongo_test")

    def test_list_collection_names_single_socket(self):
        client = rs_or_single_client(maxPoolSize=1)
        client.drop_database('test_collection_names_single_socket')
        db = client.test_collection_names_single_socket
        for i in range(200):
            db.create_collection(str(i))

        db.list_collection_names()  # Must not hang.
        client.drop_database('test_collection_names_single_socket')

    def test_drop_collection(self):
        db = Database(self.client, "pymongo_test")

        self.assertRaises(TypeError, db.drop_collection, 5)
        self.assertRaises(TypeError, db.drop_collection, None)

        db.test.insert_one({"dummy": "object"})
        self.assertTrue("test" in db.list_collection_names())
        db.drop_collection("test")
        self.assertFalse("test" in db.list_collection_names())

        db.test.insert_one({"dummy": "object"})
        self.assertTrue("test" in db.list_collection_names())
        db.drop_collection("test")
        self.assertFalse("test" in db.list_collection_names())

        db.test.insert_one({"dummy": "object"})
        self.assertTrue("test" in db.list_collection_names())
        db.drop_collection(db.test)
        self.assertFalse("test" in db.list_collection_names())

        db.test.insert_one({"dummy": "object"})
        self.assertTrue("test" in db.list_collection_names())
        db.test.drop()
        self.assertFalse("test" in db.list_collection_names())
        db.test.drop()

        db.drop_collection(db.test.doesnotexist)

        if client_context.version.at_least(3, 3, 9) and client_context.is_rs:
            db_wc = Database(self.client, 'pymongo_test',
                             write_concern=IMPOSSIBLE_WRITE_CONCERN)
            with self.assertRaises(WriteConcernError):
                db_wc.drop_collection('test')

    def test_validate_collection(self):
        db = self.client.pymongo_test

        self.assertRaises(TypeError, db.validate_collection, 5)
        self.assertRaises(TypeError, db.validate_collection, None)

        db.test.insert_one({"dummy": "object"})

        self.assertRaises(OperationFailure, db.validate_collection,
                          "test.doesnotexist")
        self.assertRaises(OperationFailure, db.validate_collection,
                          db.test.doesnotexist)

        self.assertTrue(db.validate_collection("test"))
        self.assertTrue(db.validate_collection(db.test))
        self.assertTrue(db.validate_collection(db.test, full=True))
        self.assertTrue(db.validate_collection(db.test, scandata=True))
        self.assertTrue(db.validate_collection(db.test, scandata=True, full=True))
        self.assertTrue(db.validate_collection(db.test, True, True))

    @client_context.require_version_min(4, 3, 3)
    def test_validate_collection_background(self):
        db = self.client.pymongo_test
        db.test.insert_one({"dummy": "object"})
        coll = db.test
        self.assertTrue(db.validate_collection(coll, background=False))
        # The inMemory storage engine does not support background=True.
        if client_context.storage_engine != 'inMemory':
            self.assertTrue(db.validate_collection(coll, background=True))
            self.assertTrue(
                db.validate_collection(coll, scandata=True, background=True))
            # The server does not support background=True with full=True.
            # Assert that we actually send the background option by checking
            # that this combination fails.
            with self.assertRaises(OperationFailure):
                db.validate_collection(coll, full=True, background=True)

    @client_context.require_no_mongos
    @ignore_deprecations
    def test_profiling_levels(self):
        db = self.client.pymongo_test
        self.assertEqual(db.profiling_level(), OFF)  # default

        self.assertRaises(ValueError, db.set_profiling_level, 5.5)
        self.assertRaises(ValueError, db.set_profiling_level, None)
        self.assertRaises(ValueError, db.set_profiling_level, -1)
        self.assertRaises(TypeError, db.set_profiling_level, SLOW_ONLY, 5.5)
        self.assertRaises(TypeError, db.set_profiling_level, SLOW_ONLY, '1')

        db.set_profiling_level(SLOW_ONLY)
        self.assertEqual(db.profiling_level(), SLOW_ONLY)

        db.set_profiling_level(ALL)
        self.assertEqual(db.profiling_level(), ALL)

        db.set_profiling_level(OFF)
        self.assertEqual(db.profiling_level(), OFF)

        db.set_profiling_level(SLOW_ONLY, 50)
        self.assertEqual(50, db.command("profile", -1)['slowms'])

        db.set_profiling_level(ALL, -1)
        self.assertEqual(-1, db.command("profile", -1)['slowms'])

        db.set_profiling_level(OFF, 100)  # back to default
        self.assertEqual(100, db.command("profile", -1)['slowms'])

    @client_context.require_no_mongos
    @client_context.require_version_min(3, 6)
    @ignore_deprecations
    def test_profiling_sample_rate(self):
        db = self.client.pymongo_test
        with self.assertRaises(TypeError):
            db.set_profiling_level(SLOW_ONLY, 50, sample_rate='1')
        with self.assertRaises(TypeError):
            db.set_profiling_level(SLOW_ONLY, 50, sample_rate=1)

        db.set_profiling_level(SLOW_ONLY, 50, sample_rate=0.0)
        db.set_profiling_level(SLOW_ONLY, 50, sample_rate=1.0)
        db.set_profiling_level(SLOW_ONLY, 50, sample_rate=0.5)
        profile = db.command("profile", -1)
        self.assertEqual(50, profile['slowms'])
        self.assertEqual(0.5, profile['sampleRate'])
        db.set_profiling_level(OFF, 100)  # back to default
        self.assertEqual(100, db.command("profile", -1)['slowms'])

    @client_context.require_no_mongos
    @client_context.require_version_min(4, 4, 2)
    @ignore_deprecations
    def test_profiling_filter(self):
        db = self.client.pymongo_test
        db.set_profiling_level(ALL, filter={'ns': {'$eq': 'test.test'}})
        profile = db.command("profile", -1)
        self.assertEqual({'ns': {'$eq': 'test.test'}}, profile['filter'])
        # filter='unset' resets the filter back to the default.
        db.set_profiling_level(OFF, 100, filter='unset')
        self.assertEqual(100, db.command("profile", -1)['slowms'])

    @client_context.require_no_mongos
    @ignore_deprecations
    def test_profiling_info(self):
        db = self.client.pymongo_test

        db.system.profile.drop()
        db.set_profiling_level(ALL)
        db.test.find_one()
        db.set_profiling_level(OFF)

        info = db.profiling_info()
        self.assertTrue(isinstance(info, list))

        # Check if we're going to fail because of SERVER-4754, in which
        # profiling info isn't collected if mongod was started with --auth
        if server_started_with_auth(self.client):
            raise SkipTest(
                "We need SERVER-4754 fixed for the rest of this test to pass"
            )

        self.assertTrue(len(info) >= 1)
        # These basically clue us in to server changes.
        self.assertTrue(isinstance(info[0]['responseLength'], int))
        self.assertTrue(isinstance(info[0]['millis'], int))
        self.assertTrue(isinstance(info[0]['client'], str))
        self.assertTrue(isinstance(info[0]['user'], str))
        self.assertTrue(isinstance(info[0]['ns'], str))
        self.assertTrue(isinstance(info[0]['op'], str))
        self.assertTrue(isinstance(info[0]["ts"], datetime.datetime))

    def test_profiling_helpers_deprecated(self):
        filter = DeprecationFilter('error')
        self.addCleanup(filter.stop)
        db = self.client.pymongo_test
        self.assertRaises(DeprecationWarning, db.profiling_level)
        self.assertRaises(DeprecationWarning, db.profiling_info)
        self.assertRaises(DeprecationWarning, db.set_profiling_level, OFF)

    def test_command(self):
        self.maxDiff = None
        db = self.client.admin
        first = db.command("buildinfo")
        second = db.command({"buildinfo": 1})
        third = db.command("buildinfo", 1)
        self.assertEqualReply(first, second)
        self.assertEqualReply(second, third)

    # We use 'aggregate' as our example command, since it's an easy way to
    # retrieve a BSON regex from a collection using a command. But until
    # MongoDB 2.3.2, aggregation turned regexes into strings: SERVER-6470.
    # Note: MongoDB 3.5.2 requires the 'cursor' or 'explain' option for
    # aggregate.
    @client_context.require_version_max(3, 5, 0)
    def test_command_with_regex(self):
        db = self.client.pymongo_test
        db.test.drop()
        db.test.insert_one({'r': re.compile('.*')})
        db.test.insert_one({'r': Regex('.*')})

        result = db.command('aggregate', 'test', pipeline=[])
        for doc in result['result']:
            self.assertTrue(isinstance(doc['r'], Regex))

    def test_password_digest(self):
        self.assertRaises(TypeError, auth._password_digest, 5)
        self.assertRaises(TypeError, auth._password_digest, True)
        self.assertRaises(TypeError, auth._password_digest, None)

        self.assertTrue(isinstance(auth._password_digest("mike", "password"),
                                   str))
        self.assertEqual(auth._password_digest("mike", "password"),
                         "cd7e45b3b2767dc2fa9b6b548457ed00")
        self.assertEqual(auth._password_digest("Gustave", "Dor\xe9"),
                         "81e0e2364499209f466e75926a162d73")

    def test_id_ordering(self):
        # PyMongo attempts to have _id show up first
        # when you iterate key/value pairs in a document.
        # This isn't reliable since python dicts don't
        # guarantee any particular order. This will never
        # work right in Jython or any Python or environment
        # with hash randomization enabled (e.g. tox).
        db = self.client.pymongo_test
        db.test.drop()
        db.test.insert_one(SON([("hello", "world"),
                                ("_id", 5)]))

        db = self.client.get_database(
            "pymongo_test", codec_options=CodecOptions(document_class=SON))
        cursor = db.test.find()
        for x in cursor:
            for (k, v) in x.items():
                self.assertEqual(k, "_id")
                break

    def test_deref(self):
        db = self.client.pymongo_test
        db.test.drop()

        self.assertRaises(TypeError, db.dereference, 5)
        self.assertRaises(TypeError, db.dereference, "hello")
        self.assertRaises(TypeError, db.dereference, None)

        self.assertEqual(None, db.dereference(DBRef("test", ObjectId())))
        obj = {"x": True}
        key = db.test.insert_one(obj).inserted_id
        self.assertEqual(obj, db.dereference(DBRef("test", key)))
        self.assertEqual(obj,
                         db.dereference(DBRef("test", key, "pymongo_test")))
        self.assertRaises(ValueError,
                          db.dereference, DBRef("test", key, "foo"))

        self.assertEqual(None, db.dereference(DBRef("test", 4)))
        obj = {"_id": 4}
        db.test.insert_one(obj)
        self.assertEqual(obj, db.dereference(DBRef("test", 4)))

    def test_deref_kwargs(self):
        db = self.client.pymongo_test
        db.test.drop()

        db.test.insert_one({"_id": 4, "foo": "bar"})
        db = self.client.get_database(
            "pymongo_test", codec_options=CodecOptions(document_class=SON))
        self.assertEqual(SON([("foo", "bar")]),
                         db.dereference(DBRef("test", 4),
                                        projection={"_id": False}))

    # TODO some of these tests belong in the collection level testing.
    def test_insert_find_one(self):
        db = self.client.pymongo_test
        db.test.drop()

        a_doc = SON({"hello": "world"})
        a_key = db.test.insert_one(a_doc).inserted_id
        self.assertTrue(isinstance(a_doc["_id"], ObjectId))
        self.assertEqual(a_doc["_id"], a_key)
        self.assertEqual(a_doc, db.test.find_one({"_id": a_doc["_id"]}))
        self.assertEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(None, db.test.find_one(ObjectId()))
        self.assertEqual(a_doc, db.test.find_one({"hello": "world"}))
        self.assertEqual(None, db.test.find_one({"hello": "test"}))

        b = db.test.find_one()
        b["hello"] = "mike"
        db.test.replace_one({"_id": b["_id"]}, b)

        self.assertNotEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one())

        count = 0
        for _ in db.test.find():
            count += 1
        self.assertEqual(count, 1)

    def test_long(self):
        db = self.client.pymongo_test
        db.test.drop()
        db.test.insert_one({"x": 9223372036854775807})
        retrieved = db.test.find_one()['x']
        self.assertEqual(Int64(9223372036854775807), retrieved)
        self.assertIsInstance(retrieved, Int64)
        db.test.delete_many({})
        db.test.insert_one({"x": Int64(1)})
        retrieved = db.test.find_one()['x']
        self.assertEqual(Int64(1), retrieved)
        self.assertIsInstance(retrieved, Int64)

    def test_delete(self):
        db = self.client.pymongo_test
        db.test.drop()

        db.test.insert_one({"x": 1})
        db.test.insert_one({"x": 2})
        db.test.insert_one({"x": 3})
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 3)

        db.test.delete_one({"x": 1})
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 2)

        db.test.delete_one(db.test.find_one())
        db.test.delete_one(db.test.find_one())
        self.assertEqual(db.test.find_one(), None)

        db.test.insert_one({"x": 1})
        db.test.insert_one({"x": 2})
        db.test.insert_one({"x": 3})

        self.assertTrue(db.test.find_one({"x": 2}))
        db.test.delete_one({"x": 2})
        self.assertFalse(db.test.find_one({"x": 2}))

        self.assertTrue(db.test.find_one())
        db.test.delete_many({})
        self.assertFalse(db.test.find_one())

    def test_command_response_without_ok(self):
        # Sometimes (SERVER-10891) the server's response to a badly-formatted
        # command document will have no 'ok' field. We should raise
        # OperationFailure instead of KeyError.
        self.assertRaises(OperationFailure,
                          helpers._check_command_response, {}, None)

        try:
            helpers._check_command_response({'$err': 'foo'}, None)
        except OperationFailure as e:
            self.assertEqual(e.args[0], "foo, full error: {'$err': 'foo'}")
        else:
            self.fail("_check_command_response didn't raise OperationFailure")

    def test_mongos_response(self):
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {'ok': 0, 'errmsg': 'inner'}}}

        with self.assertRaises(OperationFailure) as context:
            helpers._check_command_response(error_document, None)

        self.assertIn('inner', str(context.exception))

        # If a shard has no primary and you run a command like dbstats, which
        # cannot be run on a secondary, mongos's response includes empty "raw"
        # errors. See SERVER-15428.
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {}}}

        with self.assertRaises(OperationFailure) as context:
            helpers._check_command_response(error_document, None)

        self.assertIn('outer', str(context.exception))

        # Raw error has ok: 0 but no errmsg. Not a known case, but test it.
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {'ok': 0}}}

        with self.assertRaises(OperationFailure) as context:
            helpers._check_command_response(error_document, None)

        self.assertIn('outer', str(context.exception))

    @client_context.require_test_commands
    @client_context.require_no_mongos
    def test_command_max_time_ms(self):
        self.client.admin.command("configureFailPoint",
                                  "maxTimeAlwaysTimeOut",
                                  mode="alwaysOn")
        try:
            db = self.client.pymongo_test
            db.command('count', 'test')
            self.assertRaises(ExecutionTimeout, db.command,
                              'count', 'test', maxTimeMS=1)
            pipeline = [{'$project': {'name': 1, 'count': 1}}]
            # Database command helper.
            db.command('aggregate', 'test', pipeline=pipeline, cursor={})
            self.assertRaises(ExecutionTimeout, db.command,
                              'aggregate', 'test',
                              pipeline=pipeline, cursor={}, maxTimeMS=1)
            # Collection helper.
            db.test.aggregate(pipeline=pipeline)
            self.assertRaises(ExecutionTimeout,
                              db.test.aggregate, pipeline, maxTimeMS=1)
        finally:
            self.client.admin.command("configureFailPoint",
                                      "maxTimeAlwaysTimeOut",
                                      mode="off")

    def test_with_options(self):
        codec_options = DECIMAL_CODECOPTS
        read_preference = ReadPreference.SECONDARY_PREFERRED
        write_concern = WriteConcern(j=True)
        read_concern = ReadConcern(level="majority")

        # List of all options to compare.
        allopts = ['name', 'client', 'codec_options',
                   'read_preference', 'write_concern', 'read_concern']

        db1 = self.client.get_database(
            'with_options_test', codec_options=codec_options,
            read_preference=read_preference, write_concern=write_concern,
            read_concern=read_concern)

        # Case 1: swap no options
        db2 = db1.with_options()
        for opt in allopts:
            self.assertEqual(getattr(db1, opt), getattr(db2, opt))

        # Case 2: swap all options
        newopts = {'codec_options': CodecOptions(),
                   'read_preference': ReadPreference.PRIMARY,
                   'write_concern': WriteConcern(w=1),
                   'read_concern': ReadConcern(level="local")}
        db2 = db1.with_options(**newopts)
        for opt in newopts:
            self.assertEqual(
                getattr(db2, opt), newopts.get(opt, getattr(db1, opt)))


class TestDatabaseAggregation(IntegrationTest):
    def setUp(self):
        self.pipeline = [{"$listLocalSessions": {}},
                         {"$limit": 1},
                         {"$addFields": {"dummy": "dummy field"}},
                         {"$project": {"_id": 0, "dummy": 1}}]
        self.result = {"dummy": "dummy field"}
        self.admin = self.client.admin

    @client_context.require_version_min(3, 6, 0)
    def test_database_aggregation(self):
        with self.admin.aggregate(self.pipeline) as cursor:
            result = next(cursor)
            self.assertEqual(result, self.result)

    @client_context.require_version_min(3, 6, 0)
    @client_context.require_no_mongos
    def test_database_aggregation_fake_cursor(self):
        coll_name = "test_output"
        if client_context.version < (4, 3):
            db_name = "admin"
            write_stage = {"$out": coll_name}
        else:
            # SERVER-43287 disallows writing with $out to the admin db, use
            # $merge instead.
            db_name = "pymongo_test"
            write_stage = {
                "$merge": {"into": {"db": db_name, "coll": coll_name}}}
        output_coll = self.client[db_name][coll_name]
        output_coll.drop()
        self.addCleanup(output_coll.drop)

        admin = self.admin.with_options(write_concern=WriteConcern(w=0))
        pipeline = self.pipeline[:]
        pipeline.append(write_stage)
        with admin.aggregate(pipeline) as cursor:
            with self.assertRaises(StopIteration):
                next(cursor)

        result = wait_until(output_coll.find_one, "read unacknowledged write")
        self.assertEqual(result["dummy"], self.result["dummy"])

    @client_context.require_version_max(3, 6, 0, -1)
    def test_database_aggregation_unsupported(self):
        err_msg = r"Database.aggregate\(\) is only supported on MongoDB 3.6\+."
        with self.assertRaisesRegex(ConfigurationError, err_msg):
            with self.admin.aggregate(self.pipeline) as _:
                pass


if __name__ == "__main__":
    unittest.main()
