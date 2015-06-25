# Copyright 2009-2015 MongoDB, Inc.
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
from bson.py3compat import u, string_type, text_type, PY3
from bson.son import SON
from pymongo import (MongoClient,
                     ALL,
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
                            OperationFailure)
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from test import (client_context,
                  SkipTest,
                  unittest,
                  host,
                  port,
                  IntegrationTest)
from test.utils import (ignore_deprecations,
                        remove_all_users,
                        rs_or_single_client_noauth,
                        rs_or_single_client,
                        server_started_with_auth)


if PY3:
    long = int


class TestDatabaseNoConnect(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = MongoClient(host, port, connect=False)

    def test_name(self):
        self.assertRaises(TypeError, Database, self.client, 4)
        self.assertRaises(InvalidName, Database, self.client, "my db")
        self.assertRaises(InvalidName, Database, self.client, "my\x00db")
        self.assertRaises(InvalidName, Database,
                          self.client, u("my\u0000db"))
        self.assertEqual("name", Database(self.client, "name").name)

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

    def test_get_collection(self):
        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        coll = self.client.pymongo_test.get_collection(
            'foo', codec_options, ReadPreference.SECONDARY, write_concern)
        self.assertEqual('foo', coll.name)
        self.assertEqual(codec_options, coll.codec_options)
        self.assertEqual(ReadPreference.SECONDARY, coll.read_preference)
        self.assertEqual(write_concern, coll.write_concern)

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

    def test_repr(self):
        self.assertEqual(repr(Database(self.client, "pymongo_test")),
                         "Database(%r, %s)" % (self.client,
                                               repr(u("pymongo_test"))))

    def test_create_collection(self):
        db = Database(self.client, "pymongo_test")

        db.test.insert_one({"hello": "world"})
        self.assertRaises(CollectionInvalid, db.create_collection, "test")

        db.drop_collection("test")

        self.assertRaises(TypeError, db.create_collection, 5)
        self.assertRaises(TypeError, db.create_collection, None)
        self.assertRaises(InvalidName, db.create_collection, "coll..ection")

        test = db.create_collection("test")
        self.assertTrue(u("test") in db.collection_names())
        test.insert_one({"hello": u("world")})
        self.assertEqual(db.test.find_one()["hello"], "world")

        db.drop_collection("test.foo")
        db.create_collection("test.foo")
        self.assertTrue(u("test.foo") in db.collection_names())
        self.assertRaises(CollectionInvalid, db.create_collection, "test.foo")

    def test_collection_names(self):
        db = Database(self.client, "pymongo_test")
        db.test.insert_one({"dummy": u("object")})
        db.test.mike.insert_one({"dummy": u("object")})

        colls = db.collection_names()
        self.assertTrue("test" in colls)
        self.assertTrue("test.mike" in colls)
        for coll in colls:
            self.assertTrue("$" not in coll)

        colls_without_systems = db.collection_names(False)
        for coll in colls_without_systems:
            self.assertTrue(not coll.startswith("system."))

        # Force more than one batch.
        db = self.client.many_collections
        for i in range(101):
            db["coll" + str(i)].insert_one({})
        # No Error
        try:
            db.collection_names()
        finally:
            self.client.drop_database("many_collections")

    def test_drop_collection(self):
        db = Database(self.client, "pymongo_test")

        self.assertRaises(TypeError, db.drop_collection, 5)
        self.assertRaises(TypeError, db.drop_collection, None)

        db.test.insert_one({"dummy": u("object")})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection("test")
        self.assertFalse("test" in db.collection_names())

        db.test.insert_one({"dummy": u("object")})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection(u("test"))
        self.assertFalse("test" in db.collection_names())

        db.test.insert_one({"dummy": u("object")})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection(db.test)
        self.assertFalse("test" in db.collection_names())

        db.test.insert_one({"dummy": u("object")})
        self.assertTrue("test" in db.collection_names())
        db.test.drop()
        self.assertFalse("test" in db.collection_names())
        db.test.drop()

        db.drop_collection(db.test.doesnotexist)

    def test_validate_collection(self):
        db = self.client.pymongo_test

        self.assertRaises(TypeError, db.validate_collection, 5)
        self.assertRaises(TypeError, db.validate_collection, None)

        db.test.insert_one({"dummy": u("object")})

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
    def test_errors(self):
        with ignore_deprecations():
            # We must call getlasterror, etc. on same socket as last operation.
            db = rs_or_single_client(maxPoolSize=1).pymongo_test
            db.reset_error_history()
            self.assertEqual(None, db.error())
            self.assertEqual(None, db.previous_error())

            db.command("forceerror", check=False)
            self.assertTrue(db.error())
            self.assertTrue(db.previous_error())

            db.command("forceerror", check=False)
            self.assertTrue(db.error())
            prev_error = db.previous_error()
            self.assertEqual(prev_error["nPrev"], 1)
            del prev_error["nPrev"]
            prev_error.pop("lastOp", None)
            error = db.error()
            error.pop("lastOp", None)
            # getLastError includes "connectionId" in recent
            # server versions, getPrevError does not.
            error.pop("connectionId", None)
            self.assertEqual(error, prev_error)

            db.test.find_one()
            self.assertEqual(None, db.error())
            self.assertTrue(db.previous_error())
            self.assertEqual(db.previous_error()["nPrev"], 2)

            db.reset_error_history()
            self.assertEqual(None, db.error())
            self.assertEqual(None, db.previous_error())

    def test_command(self):
        db = self.client.admin

        self.assertEqual(db.command("buildinfo"), db.command({"buildinfo": 1}))

    # We use 'aggregate' as our example command, since it's an easy way to
    # retrieve a BSON regex from a collection using a command. But until
    # MongoDB 2.3.2, aggregation turned regexes into strings: SERVER-6470.
    @client_context.require_version_min(2, 3, 2)
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
                         u("cd7e45b3b2767dc2fa9b6b548457ed00"))
        self.assertEqual(auth._password_digest("mike", "password"),
                         auth._password_digest(u("mike"), u("password")))
        self.assertEqual(auth._password_digest("Gustave", u("Dor\xe9")),
                         u("81e0e2364499209f466e75926a162d73"))

    @client_context.require_auth
    def test_authenticate_add_remove_user(self):
        # "self.client" is logged in as root.
        auth_db = self.client.pymongo_test
        db = rs_or_single_client_noauth().pymongo_test

        # Configuration errors
        self.assertRaises(ValueError, auth_db.add_user, "user", '')
        self.assertRaises(TypeError, auth_db.add_user, "user", 'password', 15)
        self.assertRaises(TypeError, auth_db.add_user,
                          "user", 'password', 'True')
        self.assertRaises(ConfigurationError, auth_db.add_user,
                          "user", 'password', True, roles=['read'])

        if client_context.version.at_least(2, 5, 3, -1):
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
        auth_db.add_user("mike", "password", roles=["dbOwner"])
        self.addCleanup(remove_all_users, auth_db)
        self.assertRaises(TypeError, db.authenticate, 5, "password")
        self.assertRaises(TypeError, db.authenticate, "mike", 5)
        self.assertRaises(OperationFailure,
                          db.authenticate, "mike", "not a real password")
        self.assertRaises(OperationFailure,
                          db.authenticate, "faker", "password")
        db.authenticate("mike", "password")
        db.logout()

        # Unicode name and password.
        db.authenticate(u("mike"), u("password"))
        db.logout()

        auth_db.remove_user("mike")
        self.assertRaises(OperationFailure,
                          db.authenticate, "mike", "password")

        # Add / authenticate / change password
        self.assertRaises(OperationFailure,
                          db.authenticate, "Gustave", u("Dor\xe9"))
        auth_db.add_user("Gustave", u("Dor\xe9"), roles=["dbOwner"])
        db.authenticate("Gustave", u("Dor\xe9"))

        # Change password.
        auth_db.add_user("Gustave", "password", roles=["dbOwner"])
        db.logout()
        self.assertRaises(OperationFailure,
                          db.authenticate, "Gustave", u("Dor\xe9"))
        self.assertTrue(db.authenticate("Gustave", u("password")))

        if not client_context.version.at_least(2, 5, 3, -1):
            # Add a readOnly user
            with ignore_deprecations():
                auth_db.add_user("Ross", "password", read_only=True)

            db.logout()
            db.authenticate("Ross", u("password"))
            self.assertTrue(
                auth_db.system.users.find({"readOnly": True}).count())

    @client_context.require_auth
    def test_make_user_readonly(self):
        # "self.client" is logged in as root.
        auth_db = self.client.pymongo_test
        db = rs_or_single_client_noauth().pymongo_test

        # Make a read-write user.
        auth_db.add_user('jesse', 'pw')
        self.addCleanup(remove_all_users, auth_db)

        # Check that we're read-write by default.
        db.authenticate('jesse', 'pw')
        db.collection.insert_one({})
        db.logout()

        # Make the user read-only.
        auth_db.add_user('jesse', 'pw', read_only=True)

        db.authenticate('jesse', 'pw')
        self.assertRaises(OperationFailure, db.collection.insert_one, {})

    @client_context.require_version_min(2, 5, 3, -1)
    @client_context.require_auth
    def test_default_roles(self):
        # "self.client" is logged in as root.
        auth_admin = self.client.admin
        auth_admin.add_user('test_default_roles', 'pass')
        self.addCleanup(auth_admin.remove_user, 'test_default_roles')
        info = auth_admin.command(
            'usersInfo', 'test_default_roles')['users'][0]

        self.assertEqual("root", info['roles'][0]['role'])

        # Read only "admin" user
        auth_admin.add_user('ro-admin', 'pass', read_only=True)
        self.addCleanup(auth_admin.remove_user, 'ro-admin')
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

    @client_context.require_version_min(2, 5, 3, -1)
    @client_context.require_auth
    def test_new_user_cmds(self):
        # "self.client" is logged in as root.
        auth_db = self.client.pymongo_test
        auth_db.add_user("amalia", "password", roles=["userAdmin"])
        self.addCleanup(auth_db.remove_user, "amalia")

        db = rs_or_single_client_noauth().pymongo_test
        db.authenticate("amalia", "password")

        # This tests the ability to update user attributes.
        db.add_user("amalia", "new_password",
                    customData={"secret": "koalas"})

        user_info = db.command("usersInfo", "amalia")
        self.assertTrue(user_info["users"])
        amalia_user = user_info["users"][0]
        self.assertEqual(amalia_user["user"], "amalia")
        self.assertEqual(amalia_user["customData"], {"secret": "koalas"})

    @client_context.require_auth
    def test_authenticate_multiple(self):
        # "self.client" is logged in as root.
        self.client.drop_database("pymongo_test")
        self.client.drop_database("pymongo_test1")
        admin_db_auth = self.client.admin
        users_db_auth = self.client.pymongo_test

        # Non-root client.
        client = rs_or_single_client_noauth()
        admin_db = client.admin
        users_db = client.pymongo_test
        other_db = client.pymongo_test1

        self.assertRaises(OperationFailure, users_db.test.find_one)

        if client_context.version.at_least(2, 5, 3, -1):
            admin_db_auth.add_user('ro-admin', 'pass',
                                   roles=["userAdmin", "readAnyDatabase"])
        else:
            admin_db_auth.add_user('ro-admin', 'pass', read_only=True)

        self.addCleanup(admin_db_auth.remove_user, 'ro-admin')
        users_db_auth.add_user('user', 'pass',
                               roles=["userAdmin", "readWrite"])
        self.addCleanup(remove_all_users, users_db_auth)

        # Regular user should be able to query its own db, but
        # no other.
        users_db.authenticate('user', 'pass')
        self.assertEqual(0, users_db.test.count())
        self.assertRaises(OperationFailure, other_db.test.find_one)

        # Admin read-only user should be able to query any db,
        # but not write.
        admin_db.authenticate('ro-admin', 'pass')
        self.assertEqual(None, other_db.test.find_one())
        self.assertRaises(OperationFailure,
                          other_db.test.insert_one, {})

        # Close all sockets.
        client.close()

        # We should still be able to write to the regular user's db.
        self.assertTrue(users_db.test.delete_many({}))

        # And read from other dbs...
        self.assertEqual(0, other_db.test.count())

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
    def test_eval(self):
        db = self.client.pymongo_test
        db.test.drop()

        self.assertRaises(TypeError, db.eval, None)
        self.assertRaises(TypeError, db.eval, 5)
        self.assertRaises(TypeError, db.eval, [])

        self.assertEqual(3, db.eval("function (x) {return x;}", 3))
        self.assertEqual(3, db.eval(u("function (x) {return x;}"), 3))

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

        a_doc = SON({"hello": u("world")})
        a_key = db.test.insert_one(a_doc).inserted_id
        self.assertTrue(isinstance(a_doc["_id"], ObjectId))
        self.assertEqual(a_doc["_id"], a_key)
        self.assertEqual(a_doc, db.test.find_one({"_id": a_doc["_id"]}))
        self.assertEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(None, db.test.find_one(ObjectId()))
        self.assertEqual(a_doc, db.test.find_one({"hello": u("world")}))
        self.assertEqual(None, db.test.find_one({"hello": u("test")}))

        b = db.test.find_one()
        b["hello"] = u("mike")
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
    def test_system_js(self):
        db = self.client.pymongo_test
        db.system.js.delete_many({})

        self.assertEqual(0, db.system.js.count())
        db.system_js.add = "function(a, b) { return a + b; }"
        self.assertEqual('add', db.system.js.find_one()['_id'])
        self.assertEqual(1, db.system.js.count())
        self.assertEqual(6, db.system_js.add(1, 5))
        del db.system_js.add
        self.assertEqual(0, db.system.js.count())

        db.system_js['add'] = "function(a, b) { return a + b; }"
        self.assertEqual('add', db.system.js.find_one()['_id'])
        self.assertEqual(1, db.system.js.count())
        self.assertEqual(6, db.system_js['add'](1, 5))
        del db.system_js['add']
        self.assertEqual(0, db.system.js.count())
        self.assertRaises(OperationFailure, db.system_js.add, 1, 5)

        # TODO right now CodeWScope doesn't work w/ system js
        # db.system_js.scope = Code("return hello;", {"hello": 8})
        # self.assertEqual(8, db.system_js.scope())

        self.assertRaises(OperationFailure, db.system_js.non_existant)

        # XXX: Broken in V8, works in SpiderMonkey
        if not client_context.version.at_least(2, 3, 0):
            db.system_js.no_param = Code("return 5;")
            self.assertEqual(5, db.system_js.no_param())

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
                          helpers._check_command_response, {})

        try:
            helpers._check_command_response({'$err': 'foo'})
        except OperationFailure as e:
            self.assertEqual(e.args[0], 'foo')
        else:
            self.fail("_check_command_response didn't raise OperationFailure")

    def test_mongos_response(self):
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {'ok': 0, 'errmsg': 'inner'}}}

        with self.assertRaises(OperationFailure) as context:
            helpers._check_command_response(error_document)

        self.assertEqual('inner', str(context.exception))

        # If a shard has no primary and you run a command like dbstats, which
        # cannot be run on a secondary, mongos's response includes empty "raw"
        # errors. See SERVER-15428.
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {}}}

        with self.assertRaises(OperationFailure) as context:
            helpers._check_command_response(error_document)

        self.assertEqual('outer', str(context.exception))

        # Raw error has ok: 0 but no errmsg. Not a known case, but test it.
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {'ok': 0}}}

        with self.assertRaises(OperationFailure) as context:
            helpers._check_command_response(error_document)

        self.assertEqual('outer', str(context.exception))

    @client_context.require_version_min(2, 5, 3, -1)
    @client_context.require_test_commands
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
            db.command('aggregate', 'test', pipeline=pipeline)
            self.assertRaises(ExecutionTimeout, db.command,
                              'aggregate', 'test',
                              pipeline=pipeline, maxTimeMS=1)
            # Collection helper.
            db.test.aggregate(pipeline=pipeline)
            self.assertRaises(ExecutionTimeout,
                              db.test.aggregate, pipeline, maxTimeMS=1)
        finally:
            self.client.admin.command("configureFailPoint",
                                      "maxTimeAlwaysTimeOut",
                                      mode="off")


if __name__ == "__main__":
    unittest.main()
