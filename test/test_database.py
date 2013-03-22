# Copyright 2009-2012 10gen, Inc.
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
import os
import sys

sys.path[0:0] = [""]
import unittest

from nose.plugins.skip import SkipTest

from bson.code import Code
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.son import SON
from pymongo import (ALL,
                     auth,
                     OFF,
                     SLOW_ONLY)
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (CollectionInvalid,
                            ConfigurationError,
                            InvalidName,
                            OperationFailure)
from pymongo.son_manipulator import (AutoReference,
                                     NamespaceInjector,
                                     ObjectIdShuffler)
from test import version
from test.utils import is_mongos, server_started_with_auth
from test.test_client import get_client


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.client = get_client()

    def test_name(self):
        self.assertRaises(TypeError, Database, self.client, 4)
        self.assertRaises(InvalidName, Database, self.client, "my db")
        self.assertRaises(InvalidName, Database, self.client, "my\x00db")
        self.assertRaises(InvalidName, Database,
                          self.client, u"my\u0000db")
        self.assertEqual("name", Database(self.client, "name").name)

    def test_equality(self):
        self.assertNotEqual(Database(self.client, "test"),
                            Database(self.client, "mike"))
        self.assertEqual(Database(self.client, "test"),
                         Database(self.client, "test"))

        # Explicitly test inequality
        self.assertFalse(Database(self.client, "test") !=
                         Database(self.client, "test"))

    def test_repr(self):
        self.assertEqual(repr(Database(self.client, "pymongo_test")),
                         "Database(%r, %s)" % (self.client,
                                               repr(u"pymongo_test")))

    def test_get_coll(self):
        db = Database(self.client, "pymongo_test")
        self.assertEqual(db.test, db["test"])
        self.assertEqual(db.test, Collection(db, "test"))
        self.assertNotEqual(db.test, Collection(db, "mike"))
        self.assertEqual(db.test.mike, db["test.mike"])

    def test_create_collection(self):
        db = Database(self.client, "pymongo_test")

        db.test.insert({"hello": "world"})
        self.assertRaises(CollectionInvalid, db.create_collection, "test")

        db.drop_collection("test")

        self.assertRaises(TypeError, db.create_collection, 5)
        self.assertRaises(TypeError, db.create_collection, None)
        self.assertRaises(InvalidName, db.create_collection, "coll..ection")

        test = db.create_collection("test")
        test.save({"hello": u"world"})
        self.assertEqual(db.test.find_one()["hello"], "world")
        self.assertTrue(u"test" in db.collection_names())

        db.drop_collection("test.foo")
        db.create_collection("test.foo")
        self.assertTrue(u"test.foo" in db.collection_names())
        self.assertEqual(db.test.foo.options(), {})
        self.assertRaises(CollectionInvalid, db.create_collection, "test.foo")

    def test_collection_names(self):
        db = Database(self.client, "pymongo_test")
        db.test.save({"dummy": u"object"})
        db.test.mike.save({"dummy": u"object"})

        colls = db.collection_names()
        self.assertTrue("test" in colls)
        self.assertTrue("test.mike" in colls)
        for coll in colls:
            self.assertTrue("$" not in coll)

    def test_drop_collection(self):
        db = Database(self.client, "pymongo_test")

        self.assertRaises(TypeError, db.drop_collection, 5)
        self.assertRaises(TypeError, db.drop_collection, None)

        db.test.save({"dummy": u"object"})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection("test")
        self.assertFalse("test" in db.collection_names())

        db.test.save({"dummy": u"object"})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection(u"test")
        self.assertFalse("test" in db.collection_names())

        db.test.save({"dummy": u"object"})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection(db.test)
        self.assertFalse("test" in db.collection_names())

        db.test.save({"dummy": u"object"})
        self.assertTrue("test" in db.collection_names())
        db.test.drop()
        self.assertFalse("test" in db.collection_names())
        db.test.drop()

        db.drop_collection(db.test.doesnotexist)

    def test_validate_collection(self):
        db = self.client.pymongo_test

        self.assertRaises(TypeError, db.validate_collection, 5)
        self.assertRaises(TypeError, db.validate_collection, None)

        db.test.save({"dummy": u"object"})

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

    def test_profiling_levels(self):
        if is_mongos(self.client):
            raise SkipTest('profile is not supported by mongos')
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

    def test_profiling_info(self):
        if is_mongos(self.client):
            raise SkipTest('profile is not supported by mongos')
        db = self.client.pymongo_test

        db.set_profiling_level(ALL)
        db.test.find()
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
        if version.at_least(db.connection, (1, 9, 1, -1)):
            self.assertTrue(isinstance(info[0]['responseLength'], int))
            self.assertTrue(isinstance(info[0]['millis'], int))
            self.assertTrue(isinstance(info[0]['client'], basestring))
            self.assertTrue(isinstance(info[0]['user'], basestring))
            self.assertTrue(isinstance(info[0]['ntoreturn'], int))
            self.assertTrue(isinstance(info[0]['ns'], basestring))
            self.assertTrue(isinstance(info[0]['op'], basestring))
        else:
            self.assertTrue(isinstance(info[0]["info"], basestring))
            self.assertTrue(isinstance(info[0]["millis"], float))
        self.assertTrue(isinstance(info[0]["ts"], datetime.datetime))

    def test_iteration(self):
        db = self.client.pymongo_test

        def iterate():
            [a for a in db]

        self.assertRaises(TypeError, iterate)

    def test_errors(self):
        if is_mongos(self.client):
            raise SkipTest('getpreverror not supported by mongos')
        db = self.client.pymongo_test

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

    def test_last_status(self):
        db = self.client.pymongo_test

        db.test.remove({})
        db.test.save({"i": 1})

        db.test.update({"i": 1}, {"$set": {"i": 2}})
        self.assertTrue(db.last_status()["updatedExisting"])

        db.test.update({"i": 1}, {"$set": {"i": 500}})
        self.assertFalse(db.last_status()["updatedExisting"])

    def test_password_digest(self):
        self.assertRaises(TypeError, auth._password_digest, 5)
        self.assertRaises(TypeError, auth._password_digest, True)
        self.assertRaises(TypeError, auth._password_digest, None)

        self.assertTrue(isinstance(auth._password_digest("mike", "password"),
                                unicode))
        self.assertEqual(auth._password_digest("mike", "password"),
                         u"cd7e45b3b2767dc2fa9b6b548457ed00")
        self.assertEqual(auth._password_digest("mike", "password"),
                         auth._password_digest(u"mike", u"password"))
        self.assertEqual(auth._password_digest("Gustave", u"Dor\xe9"),
                         u"81e0e2364499209f466e75926a162d73")

    def test_authenticate_add_remove_user(self):
        if (is_mongos(self.client) and not
            version.at_least(self.client, (2, 0, 0))):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")
        db = self.client.pymongo_test
        db.system.users.remove({})
        db.remove_user("mike")

        self.assertRaises(TypeError, db.add_user, "user", '')
        self.assertRaises(TypeError, db.add_user, "user", 'password', 15)
        self.assertRaises(ConfigurationError, db.add_user,
                          "user", 'password', 'True')

        db.add_user("mike", "password")

        self.assertRaises(TypeError, db.authenticate, 5, "password")
        self.assertRaises(TypeError, db.authenticate, "mike", 5)

        self.assertRaises(OperationFailure,
                          db.authenticate, "mike", "not a real password")
        self.assertRaises(OperationFailure,
                          db.authenticate, "faker", "password")
        self.assertTrue(db.authenticate("mike", "password"))
        self.assertTrue(db.authenticate(u"mike", u"password"))
        db.logout()

        db.remove_user("mike")
        self.assertRaises(OperationFailure,
                          db.authenticate, "mike", "password")

        self.assertRaises(OperationFailure,
                          db.authenticate, "Gustave", u"Dor\xe9")
        db.add_user("Gustave", u"Dor\xe9")
        self.assertTrue(db.authenticate("Gustave", u"Dor\xe9"))
        db.logout()

        db.add_user("Gustave", "password")
        self.assertRaises(OperationFailure,
                          db.authenticate, "Gustave", u"Dor\xe9")
        self.assertTrue(db.authenticate("Gustave", u"password"))
        db.logout()

        db.add_user("Ross", "password", read_only=True)
        self.assertTrue(db.authenticate("Ross", u"password"))
        self.assertTrue(db.system.users.find({"readOnly": True}).count())
        db.logout()

    def test_authenticate_and_safe(self):
        if (is_mongos(self.client) and not
            version.at_least(self.client, (2, 0, 0))):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")
        db = self.client.auth_test
        db.system.users.remove({})
        db.add_user("bernie", "password")
        db.authenticate("bernie", "password")

        db.test.remove({})
        self.assertTrue(db.test.insert({"bim": "baz"}))
        self.assertEqual(1, db.test.count())

        self.assertEqual(1,
                         db.test.update({"bim": "baz"},
                                        {"$set": {"bim": "bar"}}).get('n'))

        self.assertEqual(1,
                         db.test.remove({}).get('n'))

        self.assertEqual(0, db.test.count())
        self.client.drop_database("auth_test")


    def test_authenticate_and_request(self):
        if (is_mongos(self.client) and not
            version.at_least(self.client, (2, 0, 0))):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")

        # Database.authenticate() needs to be in a request - check that it
        # always runs in a request, and that it restores the request state
        # (in or not in a request) properly when it's finished.
        self.assertFalse(self.client.auto_start_request)
        db = self.client.pymongo_test
        db.system.users.remove({})
        db.remove_user("mike")
        db.add_user("mike", "password")
        self.assertFalse(self.client.in_request())
        self.assertTrue(db.authenticate("mike", "password"))
        self.assertFalse(self.client.in_request())

        request_cx = get_client(auto_start_request=True)
        request_db = request_cx.pymongo_test
        self.assertTrue(request_cx.in_request())
        self.assertTrue(request_db.authenticate("mike", "password"))
        self.assertTrue(request_cx.in_request())

        # just make sure there are no exceptions here
        db.logout()
        db.collection.find_one()
        request_db.logout()
        request_db.collection.find_one()

    def test_authenticate_multiple(self):
        client = get_client()
        if (is_mongos(client) and not
            version.at_least(self.client, (2, 0, 0))):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")
        if not server_started_with_auth(client):
            raise SkipTest("Authentication is not enabled on server")

        # Setup
        users_db = client.pymongo_test
        admin_db = client.admin
        other_db = client.pymongo_test1
        users_db.system.users.remove()
        admin_db.system.users.remove()
        users_db.test.remove()
        other_db.test.remove()

        admin_db.add_user('admin', 'pass')
        self.assertTrue(admin_db.authenticate('admin', 'pass'))

        admin_db.add_user('ro-admin', 'pass', read_only=True)
        users_db.add_user('user', 'pass')

        admin_db.logout()
        self.assertRaises(OperationFailure, users_db.test.find_one)

        # Regular user should be able to query its own db, but
        # no other.
        users_db.authenticate('user', 'pass')
        self.assertEqual(0, users_db.test.count())
        self.assertRaises(OperationFailure, other_db.test.find_one)

        # Admin read-only user should be able to query any db,
        # but not write.
        admin_db.authenticate('ro-admin', 'pass')
        self.assertEqual(0, other_db.test.count())
        self.assertRaises(OperationFailure,
                          other_db.test.insert, {})

        # Force close all sockets
        client.disconnect()

        # We should still be able to write to the regular user's db
        self.assertTrue(users_db.test.remove())
        # And read from other dbs...
        self.assertEqual(0, other_db.test.count())
        # But still not write to other dbs...
        self.assertRaises(OperationFailure,
                          other_db.test.insert, {})

        # Cleanup
        admin_db.logout()
        users_db.logout()
        self.assertTrue(admin_db.authenticate('admin', 'pass'))
        self.assertTrue(admin_db.system.users.remove())
        self.assertEqual(0, admin_db.system.users.count())
        self.assertTrue(users_db.system.users.remove())

    def test_id_ordering(self):
        # PyMongo attempts to have _id show up first
        # when you iterate key/value pairs in a document.
        # This isn't reliable since python dicts don't
        # guarantee any particular order. This will never
        # work right in Jython or Python >= 3.3 with
        # hash randomization enabled.
        db = self.client.pymongo_test
        db.test.remove({})
        db.test.insert(SON([("hello", "world"),
                            ("_id", 5)]))

        if ((sys.version_info >= (3, 3) and
             os.environ.get('PYTHONHASHSEED') != '0') or
            sys.platform.startswith('java')):
            # See http://bugs.python.org/issue13703 for why we
            # use as_class=SON in certain environments.
            cursor = db.test.find(as_class=SON)
        else:
            cursor = db.test.find()

        for x in cursor:
            for (k, v) in x.items():
                self.assertEqual(k, "_id")
                break

    def test_deref(self):
        db = self.client.pymongo_test
        db.test.remove({})

        self.assertRaises(TypeError, db.dereference, 5)
        self.assertRaises(TypeError, db.dereference, "hello")
        self.assertRaises(TypeError, db.dereference, None)

        self.assertEqual(None, db.dereference(DBRef("test", ObjectId())))
        obj = {"x": True}
        key = db.test.save(obj)
        self.assertEqual(obj, db.dereference(DBRef("test", key)))
        self.assertEqual(obj,
                         db.dereference(DBRef("test", key, "pymongo_test")))
        self.assertRaises(ValueError,
                          db.dereference, DBRef("test", key, "foo"))

        self.assertEqual(None, db.dereference(DBRef("test", 4)))
        obj = {"_id": 4}
        db.test.save(obj)
        self.assertEqual(obj, db.dereference(DBRef("test", 4)))

    def test_eval(self):
        db = self.client.pymongo_test
        db.test.remove({})

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
    def test_save_find_one(self):
        db = Database(self.client, "pymongo_test")
        db.test.remove({})

        a_doc = SON({"hello": u"world"})
        a_key = db.test.save(a_doc)
        self.assertTrue(isinstance(a_doc["_id"], ObjectId))
        self.assertEqual(a_doc["_id"], a_key)
        self.assertEqual(a_doc, db.test.find_one({"_id": a_doc["_id"]}))
        self.assertEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(None, db.test.find_one(ObjectId()))
        self.assertEqual(a_doc, db.test.find_one({"hello": u"world"}))
        self.assertEqual(None, db.test.find_one({"hello": u"test"}))

        b = db.test.find_one()
        b["hello"] = u"mike"
        db.test.save(b)

        self.assertNotEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one())

        count = 0
        for _ in db.test.find():
            count += 1
        self.assertEqual(count, 1)

    def test_long(self):
        db = self.client.pymongo_test
        db.test.remove({})
        db.test.save({"x": 9223372036854775807L})
        self.assertEqual(9223372036854775807L, db.test.find_one()["x"])

    def test_remove(self):
        db = self.client.pymongo_test
        db.test.remove({})

        one = db.test.save({"x": 1})
        db.test.save({"x": 2})
        db.test.save({"x": 3})
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 3)

        db.test.remove(one)
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 2)

        db.test.remove(db.test.find_one())
        db.test.remove(db.test.find_one())
        self.assertEqual(db.test.find_one(), None)

        one = db.test.save({"x": 1})
        db.test.save({"x": 2})
        db.test.save({"x": 3})

        self.assertTrue(db.test.find_one({"x": 2}))
        db.test.remove({"x": 2})
        self.assertFalse(db.test.find_one({"x": 2}))

        self.assertTrue(db.test.find_one())
        db.test.remove({})
        self.assertFalse(db.test.find_one())

    def test_save_a_bunch(self):
        db = self.client.pymongo_test
        db.test.remove({})

        for i in xrange(1000):
            db.test.save({"x": i})

        count = 0
        for _ in db.test.find():
            count += 1

        self.assertEqual(1000, count)

        # test that kill cursors doesn't assert or anything
        for _ in xrange(62):
            for _ in db.test.find():
                break

    def test_auto_ref_and_deref(self):
        db = self.client.pymongo_test
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())

        db.test.a.remove({})
        db.test.b.remove({})
        db.test.c.remove({})

        a = {"hello": u"world"}
        db.test.a.save(a)

        b = {"test": a}
        db.test.b.save(b)

        c = {"another test": b}
        db.test.c.save(c)

        a["hello"] = "mike"
        db.test.a.save(a)

        self.assertEqual(db.test.a.find_one(), a)
        self.assertEqual(db.test.b.find_one()["test"], a)
        self.assertEqual(db.test.c.find_one()["another test"]["test"], a)
        self.assertEqual(db.test.b.find_one(), b)
        self.assertEqual(db.test.c.find_one()["another test"], b)
        self.assertEqual(db.test.c.find_one(), c)

    # some stuff the user marc wanted to be able to do, make sure it works
    def test_marc(self):
        db = self.client.pymongo_test
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())

        db.drop_collection("users")
        db.drop_collection("messages")

        message_1 = {"title": "foo"}
        db.messages.save(message_1)
        message_2 = {"title": "bar"}
        db.messages.save(message_2)

        user = {"name": "marc",
                "messages": [message_1, message_2]}
        db.users.save(user)

        message = db.messages.find_one()
        db.messages.update(message, {"title": "buzz"})

        self.assertEqual("buzz", db.users.find_one()["messages"][0]["title"])
        self.assertEqual("bar", db.users.find_one()["messages"][1]["title"])

    def test_system_js(self):
        db = self.client.pymongo_test
        db.system.js.remove()

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

        if version.at_least(db.connection, (1, 3, 2, -1)):
            self.assertRaises(OperationFailure, db.system_js.add, 1, 5)

        # TODO right now CodeWScope doesn't work w/ system js
        # db.system_js.scope = Code("return hello;", {"hello": 8})
        # self.assertEqual(8, db.system_js.scope())

        self.assertRaises(OperationFailure, db.system_js.non_existant)

        # XXX: Broken in V8, works in SpiderMonkey
        if not version.at_least(db.connection, (2, 3, 0)):
            db.system_js.no_param = Code("return 5;")
            self.assertEqual(5, db.system_js.no_param())

    def test_system_js_list(self):
        db = self.client.pymongo_test
        db.system.js.remove()
        self.assertEqual([], db.system_js.list())

        db.system_js.foo = "function() { return 'blah'; }"
        self.assertEqual(["foo"], db.system_js.list())

        db.system_js.bar = "function() { return 'baz'; }"
        self.assertEqual(set(["foo", "bar"]), set(db.system_js.list()))

        del db.system_js.foo
        self.assertEqual(["bar"], db.system_js.list())

    def test_manipulator_properties(self):
        db = self.client.foo
        self.assertEqual(['ObjectIdInjector'], db.incoming_manipulators)
        self.assertEqual([], db.incoming_copying_manipulators)
        self.assertEqual([], db.outgoing_manipulators)
        self.assertEqual([], db.outgoing_copying_manipulators)
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())
        db.add_son_manipulator(ObjectIdShuffler())
        self.assertEqual(2, len(db.incoming_manipulators))
        for name in db.incoming_manipulators:
            self.assertTrue(name in ('ObjectIdInjector', 'NamespaceInjector'))
        self.assertEqual(2, len(db.incoming_copying_manipulators))
        for name in db.incoming_copying_manipulators:
            self.assertTrue(name in ('ObjectIdShuffler', 'AutoReference'))
        self.assertEqual([], db.outgoing_manipulators)
        self.assertEqual(['AutoReference'], db.outgoing_copying_manipulators)


if __name__ == "__main__":
    unittest.main()
