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
import warnings

sys.path[0:0] = [""]

from bson.code import Code
from bson.codec_options import CodecOptions
from bson.int64 import Int64
from bson.regex import Regex
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.py3compat import string_type, text_type, PY3
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
from pymongo.saslprep import HAVE_STRINGPREP
from pymongo.write_concern import WriteConcern
from test import (client_context,
                  SkipTest,
                  unittest,
                  IntegrationTest)
from test.utils import (EventListener,
                        ignore_deprecations,
                        remove_all_users,
                        rs_or_single_client_noauth,
                        rs_or_single_client,
                        server_started_with_auth,
                        wait_until,
                        IMPOSSIBLE_WRITE_CONCERN,
                        OvertCommandListener)
from test.test_custom_types import DECIMAL_CODECOPTS


if PY3:
    long = int


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
                          self.client, u"my\u0000db")
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

    def test_get_coll(self):
        db = Database(self.client, "pymongo_test")
        self.assertEqual(db.test, db["test"])
        self.assertEqual(db.test, Collection(db, "test"))
        self.assertNotEqual(db.test, Collection(db, "mike"))
        self.assertEqual(db.test.mike, db["test.mike"])

    def test_repr(self):
        self.assertEqual(repr(Database(self.client, "pymongo_test")),
                         "Database(%r, %s)" % (self.client,
                                               repr(u"pymongo_test")))

    def test_create_collection(self):
        db = Database(self.client, "pymongo_test")

        db.test.insert_one({"hello": "world"})
        self.assertRaises(CollectionInvalid, db.create_collection, "test")

        db.drop_collection("test")

        self.assertRaises(TypeError, db.create_collection, 5)
        self.assertRaises(TypeError, db.create_collection, None)
        self.assertRaises(InvalidName, db.create_collection, "coll..ection")

        test = db.create_collection("test")
        self.assertTrue(u"test" in db.list_collection_names())
        test.insert_one({"hello": u"world"})
        self.assertEqual(db.test.find_one()["hello"], "world")

        db.drop_collection("test.foo")
        db.create_collection("test.foo")
        self.assertTrue(u"test.foo" in db.list_collection_names())
        self.assertRaises(CollectionInvalid, db.create_collection, "test.foo")

    def _test_collection_names(self, meth, **no_system_kwargs):
        db = Database(self.client, "pymongo_test")
        db.test.insert_one({"dummy": u"object"})
        db.test.mike.insert_one({"dummy": u"object"})

        colls = getattr(db, meth)()
        self.assertTrue("test" in colls)
        self.assertTrue("test.mike" in colls)
        for coll in colls:
            self.assertTrue("$" not in coll)

        db.systemcoll.test.insert_one({})
        no_system_collections = getattr(db, meth)(**no_system_kwargs)
        for coll in no_system_collections:
            self.assertTrue(not coll.startswith("system."))
        self.assertIn("systemcoll.test", no_system_collections)

        # Force more than one batch.
        db = self.client.many_collections
        for i in range(101):
            db["coll" + str(i)].insert_one({})
        # No Error
        try:
            getattr(db, meth)()
        finally:
            self.client.drop_database("many_collections")

    def test_collection_names(self):
        self._test_collection_names(
            'collection_names', include_system_collections=False)

    def test_list_collection_names(self):
        self._test_collection_names(
            'list_collection_names', filter={
                "name": {"$regex": r"^(?!system\.)"}})

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
        db.test.insert_one({"dummy": u"object"})
        db.test.mike.insert_one({"dummy": u"object"})

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

    def test_collection_names_single_socket(self):
        # Test that Database.collection_names only requires one socket.
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

        db.test.insert_one({"dummy": u"object"})
        self.assertTrue("test" in db.list_collection_names())
        db.drop_collection("test")
        self.assertFalse("test" in db.list_collection_names())

        db.test.insert_one({"dummy": u"object"})
        self.assertTrue("test" in db.list_collection_names())
        db.drop_collection(u"test")
        self.assertFalse("test" in db.list_collection_names())

        db.test.insert_one({"dummy": u"object"})
        self.assertTrue("test" in db.list_collection_names())
        db.drop_collection(db.test)
        self.assertFalse("test" in db.list_collection_names())

        db.test.insert_one({"dummy": u"object"})
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

        db.test.insert_one({"dummy": u"object"})

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
        db.test.insert_one({"dummy": u"object"})
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
        self.assertTrue(isinstance(info[0]['client'], string_type))
        self.assertTrue(isinstance(info[0]['user'], string_type))
        self.assertTrue(isinstance(info[0]['ns'], string_type))
        self.assertTrue(isinstance(info[0]['op'], string_type))
        self.assertTrue(isinstance(info[0]["ts"], datetime.datetime))

    @client_context.require_no_mongos
    @ignore_deprecations
    def test_errors(self):
        # We must call getlasterror, etc. on same socket as last operation.
        db = rs_or_single_client(maxPoolSize=1).pymongo_test
        db.reset_error_history()
        self.assertEqual(None, db.error())
        if client_context.supports_getpreverror:
            self.assertEqual(None, db.previous_error())

        db.test.insert_one({"_id": 1})
        unacked = db.test.with_options(write_concern=WriteConcern(w=0))

        unacked.insert_one({"_id": 1})
        self.assertTrue(db.error())
        if client_context.supports_getpreverror:
            self.assertTrue(db.previous_error())

        unacked.insert_one({"_id": 1})
        self.assertTrue(db.error())

        if client_context.supports_getpreverror:
            prev_error = db.previous_error()
            self.assertEqual(prev_error["nPrev"], 1)
            del prev_error["nPrev"]
            prev_error.pop("lastOp", None)
            error = db.error()
            error.pop("lastOp", None)
            # getLastError includes "connectionId" in recent
            # server versions, getPrevError does not.
            error.pop("connectionId", None)
            self.assertEqualReply(error, prev_error)

        db.test.find_one()
        self.assertEqual(None, db.error())
        if client_context.supports_getpreverror:
            self.assertTrue(db.previous_error())
            self.assertEqual(db.previous_error()["nPrev"], 2)

        db.reset_error_history()
        self.assertEqual(None, db.error())
        if client_context.supports_getpreverror:
            self.assertEqual(None, db.previous_error())

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
                                   text_type))
        self.assertEqual(auth._password_digest("mike", "password"),
                         u"cd7e45b3b2767dc2fa9b6b548457ed00")
        self.assertEqual(auth._password_digest("mike", "password"),
                         auth._password_digest(u"mike", u"password"))
        self.assertEqual(auth._password_digest("Gustave", u"Dor\xe9"),
                         u"81e0e2364499209f466e75926a162d73")

    @client_context.require_auth
    def test_authenticate_add_remove_user(self):
        # "self.client" is logged in as root.
        auth_db = self.client.pymongo_test

        def check_auth(username, password):
            c = rs_or_single_client_noauth(
                username=username,
                password=password,
                authSource="pymongo_test")

            c.pymongo_test.collection.find_one()

        # Configuration errors
        self.assertRaises(ValueError, auth_db.add_user, "user", '')
        self.assertRaises(TypeError, auth_db.add_user, "user", 'password', 15)
        self.assertRaises(TypeError, auth_db.add_user,
                          "user", 'password', 'True')
        self.assertRaises(ConfigurationError, auth_db.add_user,
                          "user", 'password', True, roles=['read'])

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            self.assertRaises(DeprecationWarning, auth_db.add_user,
                              "user", "password")
            self.assertRaises(DeprecationWarning, auth_db.add_user,
                              "user", "password", True)

        with ignore_deprecations():
            self.assertRaises(ConfigurationError, auth_db.add_user,
                              "user", "password", digestPassword=True)

        # Add / authenticate / remove
        auth_db.add_user("mike", "password", roles=["read"])
        self.addCleanup(remove_all_users, auth_db)
        self.assertRaises(TypeError, check_auth, 5, "password")
        self.assertRaises(TypeError, check_auth, "mike", 5)
        self.assertRaises(OperationFailure,
                          check_auth, "mike", "not a real password")
        self.assertRaises(OperationFailure, check_auth, "faker", "password")
        check_auth("mike", "password")

        if not client_context.version.at_least(3, 7, 2) or HAVE_STRINGPREP:
            # Unicode name and password.
            check_auth(u"mike", u"password")

            auth_db.remove_user("mike")
            self.assertRaises(
                OperationFailure, check_auth, "mike", "password")

            # Add / authenticate / change password
            self.assertRaises(
                OperationFailure, check_auth, "Gustave", u"Dor\xe9")
            auth_db.add_user("Gustave", u"Dor\xe9", roles=["read"])
            check_auth("Gustave", u"Dor\xe9")

            # Change password.
            auth_db.add_user("Gustave", "password", roles=["read"])
            self.assertRaises(
                OperationFailure, check_auth, "Gustave", u"Dor\xe9")
            check_auth("Gustave", u"password")

    @client_context.require_auth
    @ignore_deprecations
    def test_make_user_readonly(self):
        # "self.client" is logged in as root.
        auth_db = self.client.pymongo_test

        # Make a read-write user.
        auth_db.add_user('jesse', 'pw')
        self.addCleanup(remove_all_users, auth_db)

        # Check that we're read-write by default.
        c = rs_or_single_client_noauth(username='jesse',
                                       password='pw',
                                       authSource='pymongo_test')

        c.pymongo_test.collection.insert_one({})

        # Make the user read-only.
        auth_db.add_user('jesse', 'pw', read_only=True)

        c = rs_or_single_client_noauth(username='jesse',
                                       password='pw',
                                       authSource='pymongo_test')

        self.assertRaises(OperationFailure,
                          c.pymongo_test.collection.insert_one,
                          {})

    @client_context.require_auth
    @ignore_deprecations
    def test_default_roles(self):
        # "self.client" is logged in as root.
        auth_admin = self.client.admin
        auth_admin.add_user('test_default_roles', 'pass')
        self.addCleanup(client_context.drop_user, 'admin', 'test_default_roles')
        info = auth_admin.command(
            'usersInfo', 'test_default_roles')['users'][0]

        self.assertEqual("root", info['roles'][0]['role'])

        # Read only "admin" user
        auth_admin.add_user('ro-admin', 'pass', read_only=True)
        self.addCleanup(client_context.drop_user, 'admin', 'ro-admin')
        info = auth_admin.command('usersInfo', 'ro-admin')['users'][0]
        self.assertEqual("readAnyDatabase", info['roles'][0]['role'])

        # "Non-admin" user
        auth_db = self.client.pymongo_test
        auth_db.add_user('user', 'pass')
        self.addCleanup(remove_all_users, auth_db)
        info = auth_db.command('usersInfo', 'user')['users'][0]
        self.assertEqual("dbOwner", info['roles'][0]['role'])

        # Read only "Non-admin" user
        auth_db.add_user('ro-user', 'pass', read_only=True)
        info = auth_db.command('usersInfo', 'ro-user')['users'][0]
        self.assertEqual("read", info['roles'][0]['role'])

    @client_context.require_auth
    @ignore_deprecations
    def test_new_user_cmds(self):
        # "self.client" is logged in as root.
        auth_db = self.client.pymongo_test
        auth_db.add_user("amalia", "password", roles=["userAdmin"])
        self.addCleanup(client_context.drop_user, "pymongo_test", "amalia")

        db = rs_or_single_client_noauth(username="amalia",
                                        password="password",
                                        authSource="pymongo_test").pymongo_test

        # This tests the ability to update user attributes.
        db.add_user("amalia", "new_password",
                    customData={"secret": "koalas"})

        user_info = db.command("usersInfo", "amalia")
        self.assertTrue(user_info["users"])
        amalia_user = user_info["users"][0]
        self.assertEqual(amalia_user["user"], "amalia")
        self.assertEqual(amalia_user["customData"], {"secret": "koalas"})

    @client_context.require_auth
    @ignore_deprecations
    def test_authenticate_multiple(self):
        # "self.client" is logged in as root.
        self.client.drop_database("pymongo_test")
        self.client.drop_database("pymongo_test1")
        admin_db_auth = self.client.admin
        users_db_auth = self.client.pymongo_test

        admin_db_auth.add_user(
            'ro-admin',
            'pass',
            roles=["userAdmin", "readAnyDatabase"])

        self.addCleanup(client_context.drop_user, 'admin', 'ro-admin')
        users_db_auth.add_user(
            'user', 'pass', roles=["userAdmin", "readWrite"])
        self.addCleanup(remove_all_users, users_db_auth)

        # Non-root client.
        listener = EventListener()
        client = rs_or_single_client_noauth(event_listeners=[listener])
        admin_db = client.admin
        users_db = client.pymongo_test
        other_db = client.pymongo_test1

        self.assertRaises(OperationFailure, users_db.test.find_one)
        self.assertEqual(listener.started_command_names(), ['find'])
        listener.reset()

        # Regular user should be able to query its own db, but
        # no other.
        users_db.authenticate('user', 'pass')
        if client_context.version.at_least(3, 0):
            self.assertEqual(listener.started_command_names()[0], 'saslStart')
        else:
            self.assertEqual(listener.started_command_names()[0], 'getnonce')

        self.assertEqual(0, users_db.test.count_documents({}))
        self.assertRaises(OperationFailure, other_db.test.find_one)

        listener.reset()
        # Admin read-only user should be able to query any db,
        # but not write.
        admin_db.authenticate('ro-admin', 'pass')
        if client_context.version.at_least(3, 0):
            self.assertEqual(listener.started_command_names()[0], 'saslStart')
        else:
            self.assertEqual(listener.started_command_names()[0], 'getnonce')
        self.assertEqual(None, other_db.test.find_one())
        self.assertRaises(OperationFailure,
                          other_db.test.insert_one, {})

        # Close all sockets.
        client.close()

        listener.reset()
        # We should still be able to write to the regular user's db.
        self.assertTrue(users_db.test.delete_many({}))
        names = listener.started_command_names()
        if client_context.version.at_least(4, 4, -1):
            # No speculation with multiple users (but we do skipEmptyExchange).
            self.assertEqual(
                names, ['saslStart', 'saslContinue', 'saslStart',
                        'saslContinue', 'delete'])
        elif client_context.version.at_least(3, 0):
            self.assertEqual(
                names, ['saslStart', 'saslContinue', 'saslContinue',
                        'saslStart', 'saslContinue', 'saslContinue', 'delete'])
        else:
            self.assertEqual(
                names, ['getnonce', 'authenticate',
                        'getnonce', 'authenticate', 'delete'])

        # And read from other dbs...
        self.assertEqual(0, other_db.test.count_documents({}))

        # But still not write to other dbs.
        self.assertRaises(OperationFailure,
                          other_db.test.insert_one, {})

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

    @client_context.require_no_auth
    @client_context.require_version_max(4, 1, 0)
    def test_eval(self):
        db = self.client.pymongo_test
        db.test.drop()

        with ignore_deprecations():
            self.assertRaises(TypeError, db.eval, None)
            self.assertRaises(TypeError, db.eval, 5)
            self.assertRaises(TypeError, db.eval, [])

            self.assertEqual(3, db.eval("function (x) {return x;}", 3))
            self.assertEqual(3, db.eval(u"function (x) {return x;}", 3))

            self.assertEqual(None,
                             db.eval("function (x) {db.test.save({y:x});}", 5))
            self.assertEqual(db.test.find_one()["y"], 5)

            self.assertEqual(5, db.eval("function (x, y) {return x + y;}", 2, 3))
            self.assertEqual(5, db.eval("function () {return 5;}"))
            self.assertEqual(5, db.eval("2 + 3;"))

            self.assertEqual(5, db.eval(Code("2 + 3;")))
            self.assertRaises(OperationFailure, db.eval, Code("return i;"))
            self.assertEqual(2, db.eval(Code("return i;", {"i": 2})))
            self.assertEqual(5, db.eval(Code("i + 3;", {"i": 2})))

            self.assertRaises(OperationFailure, db.eval, "5 ++ 5;")

    # TODO some of these tests belong in the collection level testing.
    def test_insert_find_one(self):
        db = self.client.pymongo_test
        db.test.drop()

        a_doc = SON({"hello": u"world"})
        a_key = db.test.insert_one(a_doc).inserted_id
        self.assertTrue(isinstance(a_doc["_id"], ObjectId))
        self.assertEqual(a_doc["_id"], a_key)
        self.assertEqual(a_doc, db.test.find_one({"_id": a_doc["_id"]}))
        self.assertEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(None, db.test.find_one(ObjectId()))
        self.assertEqual(a_doc, db.test.find_one({"hello": u"world"}))
        self.assertEqual(None, db.test.find_one({"hello": u"test"}))

        b = db.test.find_one()
        b["hello"] = u"mike"
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
        db.test.insert_one({"x": long(9223372036854775807)})
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

    @client_context.require_no_auth
    @client_context.require_version_max(4, 1, 0)
    def test_system_js(self):
        db = self.client.pymongo_test
        db.system.js.delete_many({})

        self.assertEqual(0, db.system.js.count_documents({}))
        db.system_js.add = "function(a, b) { return a + b; }"
        self.assertEqual('add', db.system.js.find_one()['_id'])
        self.assertEqual(1, db.system.js.count_documents({}))
        self.assertEqual(6, db.system_js.add(1, 5))
        del db.system_js.add
        self.assertEqual(0, db.system.js.count_documents({}))

        db.system_js['add'] = "function(a, b) { return a + b; }"
        self.assertEqual('add', db.system.js.find_one()['_id'])
        self.assertEqual(1, db.system.js.count_documents({}))
        self.assertEqual(6, db.system_js['add'](1, 5))
        del db.system_js['add']
        self.assertEqual(0, db.system.js.count_documents({}))
        self.assertRaises(OperationFailure, db.system_js.add, 1, 5)

        # TODO right now CodeWScope doesn't work w/ system js
        # db.system_js.scope = Code("return hello;", {"hello": 8})
        # self.assertEqual(8, db.system_js.scope())

        self.assertRaises(OperationFailure, db.system_js.non_existant)

    def test_system_js_list(self):
        db = self.client.pymongo_test
        db.system.js.delete_many({})
        self.assertEqual([], db.system_js.list())

        db.system_js.foo = "function() { return 'blah'; }"
        self.assertEqual(["foo"], db.system_js.list())

        db.system_js.bar = "function() { return 'baz'; }"
        self.assertEqual(set(["foo", "bar"]), set(db.system_js.list()))

        del db.system_js.foo
        self.assertEqual(["bar"], db.system_js.list())

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

    def test_current_op_codec_options(self):
        class MySON(SON):
            pass
        opts = CodecOptions(document_class=MySON)
        db = self.client.get_database("pymongo_test", codec_options=opts)
        current_op = db.current_op(True)
        self.assertTrue(current_op['inprog'])
        self.assertIsInstance(current_op, MySON)


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
