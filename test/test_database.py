# Copyright 2009-2010 10gen, Inc.
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
import random
import sys
sys.path[0:0] = [""]
import unittest

from pymongo import (ALL,
                     ASCENDING,
                     DESCENDING,
                     helpers,
                     OFF,
                     SLOW_ONLY)
from pymongo.code import Code
from pymongo.collection import Collection
from pymongo.connection import Connection
from pymongo.database import Database
from pymongo.dbref import DBRef
from pymongo.errors import (CollectionInvalid,
                            InvalidName,
                            InvalidOperation,
                            OperationFailure)
from pymongo.objectid import ObjectId
from pymongo.son import SON
from pymongo.son_manipulator import AutoReference, NamespaceInjector
from test import version
from test.test_connection import get_connection


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.connection = get_connection()

    def test_name(self):
        self.assertRaises(TypeError, Database, self.connection, 4)
        self.assertRaises(InvalidName, Database, self.connection, "my db")
        self.assertEqual("name", Database(self.connection, "name").name)

    def test_cmp(self):
        self.assertNotEqual(Database(self.connection, "test"),
                            Database(self.connection, "mike"))
        self.assertEqual(Database(self.connection, "test"),
                         Database(self.connection, "test"))

    def test_repr(self):
        self.assertEqual(repr(Database(self.connection, "pymongo_test")),
                         "Database(%r, u'pymongo_test')" % self.connection)

    def test_get_coll(self):
        db = Database(self.connection, "pymongo_test")
        self.assertEqual(db.test, db["test"])
        self.assertEqual(db.test, Collection(db, "test"))
        self.assertNotEqual(db.test, Collection(db, "mike"))
        self.assertEqual(db.test.mike, db["test.mike"])

    def test_create_collection(self):
        db = Database(self.connection, "pymongo_test")

        db.test.insert({"hello": "world"})
        self.assertRaises(CollectionInvalid, db.create_collection, "test")

        db.drop_collection("test")

        self.assertRaises(TypeError, db.create_collection, 5)
        self.assertRaises(TypeError, db.create_collection, None)
        self.assertRaises(InvalidName, db.create_collection, "coll..ection")

        test = db.create_collection("test")
        test.save({"hello": u"world"})
        self.assertEqual(db.test.find_one()["hello"], "world")
        self.assert_(u"test" in db.collection_names())

        db.drop_collection("test.foo")
        db.create_collection("test.foo")
        self.assert_(u"test.foo" in db.collection_names())
        self.assertEqual(db.test.foo.options(), {})
        self.assertRaises(CollectionInvalid, db.create_collection, "test.foo")

    def test_collection_names(self):
        db = Database(self.connection, "pymongo_test")
        db.test.save({"dummy": u"object"})
        db.test.mike.save({"dummy": u"object"})

        colls = db.collection_names()
        self.assert_("test" in colls)
        self.assert_("test.mike" in colls)
        for coll in colls:
            self.assert_("$" not in coll)

    def test_drop_collection(self):
        db = Database(self.connection, "pymongo_test")

        self.assertRaises(TypeError, db.drop_collection, 5)
        self.assertRaises(TypeError, db.drop_collection, None)

        db.test.save({"dummy": u"object"})
        self.assert_("test" in db.collection_names())
        db.drop_collection("test")
        self.failIf("test" in db.collection_names())

        db.test.save({"dummy": u"object"})
        self.assert_("test" in db.collection_names())
        db.drop_collection(u"test")
        self.failIf("test" in db.collection_names())

        db.test.save({"dummy": u"object"})
        self.assert_("test" in db.collection_names())
        db.drop_collection(db.test)
        self.failIf("test" in db.collection_names())

        db.drop_collection(db.test.doesnotexist)

    def test_validate_collection(self):
        db = self.connection.pymongo_test

        self.assertRaises(TypeError, db.validate_collection, 5)
        self.assertRaises(TypeError, db.validate_collection, None)

        db.test.save({"dummy": u"object"})

        self.assertRaises(OperationFailure, db.validate_collection,
                          "test.doesnotexist")
        self.assertRaises(OperationFailure, db.validate_collection,
                          db.test.doesnotexist)

        self.assert_(db.validate_collection("test"))
        self.assert_(db.validate_collection(db.test))

    def test_profiling_levels(self):
        db = self.connection.pymongo_test
        self.assertEqual(db.profiling_level(), OFF) #default

        self.assertRaises(ValueError, db.set_profiling_level, 5.5)
        self.assertRaises(ValueError, db.set_profiling_level, None)
        self.assertRaises(ValueError, db.set_profiling_level, -1)

        db.set_profiling_level(SLOW_ONLY)
        self.assertEqual(db.profiling_level(), SLOW_ONLY)

        db.set_profiling_level(ALL)
        self.assertEqual(db.profiling_level(), ALL)

        db.set_profiling_level(OFF)
        self.assertEqual(db.profiling_level(), OFF)

    def test_profiling_info(self):
        db = self.connection.pymongo_test

        db.set_profiling_level(ALL)
        db.test.find()
        db.set_profiling_level(OFF)

        info = db.profiling_info()
        self.assert_(isinstance(info, list))
        self.assert_(len(info) >= 1)
        self.assert_(isinstance(info[0]["info"], basestring))
        self.assert_(isinstance(info[0]["ts"], datetime.datetime))
        self.assert_(isinstance(info[0]["millis"], float))

    def test_iteration(self):
        db = self.connection.pymongo_test

        def iterate():
            [a for a in db]

        self.assertRaises(TypeError, iterate)

    def test_errors(self):
        db = self.connection.pymongo_test

        db.reset_error_history()
        self.assertEqual(None, db.error())
        self.assertEqual(None, db.previous_error())

        db.command({"forceerror": 1}, check=False)
        self.assert_(db.error())
        self.assert_(db.previous_error())

        db.command({"forceerror": 1}, check=False)
        self.assert_(db.error())
        prev_error = db.previous_error()
        self.assertEqual(prev_error["nPrev"], 1)
        del prev_error["nPrev"]
        self.assertEqual(db.error(), prev_error)

        db.test.find_one()
        self.assertEqual(None, db.error())
        self.assert_(db.previous_error())
        self.assertEqual(db.previous_error()["nPrev"], 2)

        db.reset_error_history()
        self.assertEqual(None, db.error())
        self.assertEqual(None, db.previous_error())

    def test_command(self):
        db = self.connection.admin

        self.assertEqual(db.command("buildinfo"), db.command({"buildinfo": 1}))

    def test_last_status(self):
        db = self.connection.pymongo_test

        db.test.remove({})
        db.test.save({"i": 1})

        db.test.update({"i": 1}, {"$set": {"i": 2}})
        self.assert_(db.last_status()["updatedExisting"])

        db.test.update({"i": 1}, {"$set": {"i": 500}})
        self.failIf(db.last_status()["updatedExisting"])

    def test_password_digest(self):
        self.assertRaises(TypeError, helpers._password_digest, 5)
        self.assertRaises(TypeError, helpers._password_digest, True)
        self.assertRaises(TypeError, helpers._password_digest, None)

        self.assert_(isinstance(helpers._password_digest("mike", "password"),
                                unicode))
        self.assertEqual(helpers._password_digest("mike", "password"),
                         u"cd7e45b3b2767dc2fa9b6b548457ed00")
        self.assertEqual(helpers._password_digest("mike", "password"),
                         helpers._password_digest(u"mike", u"password"))
        self.assertEqual(helpers._password_digest("Gustave", u"Dor\xe9"),
                         u"81e0e2364499209f466e75926a162d73")

    def test_authenticate_add_remove_user(self):
        db = self.connection.pymongo_test
        db.system.users.remove({})
        db.remove_user("mike")
        db.add_user("mike", "password")

        self.assertRaises(TypeError, db.authenticate, 5, "password")
        self.assertRaises(TypeError, db.authenticate, "mike", 5)

        self.failIf(db.authenticate("mike", "not a real password"))
        self.failIf(db.authenticate("faker", "password"))
        self.assert_(db.authenticate("mike", "password"))
        self.assert_(db.authenticate(u"mike", u"password"))

        db.remove_user("mike")
        self.failIf(db.authenticate("mike", "password"))

        self.failIf(db.authenticate("Gustave", u"Dor\xe9"))
        db.add_user("Gustave", u"Dor\xe9")
        self.assert_(db.authenticate("Gustave", u"Dor\xe9"))

        db.add_user("Gustave", "password")
        self.failIf(db.authenticate("Gustave", u"Dor\xe9"))
        self.assert_(db.authenticate("Gustave", u"password"))

        # just make sure there are no exceptions here
        db.logout()
        db.logout()

    def test_id_ordering(self):
        db = self.connection.pymongo_test
        db.test.remove({})

        db.test.insert({"hello": "world", "_id": 5})
        db.test.insert(SON([("hello", "world"),
                            ("_id", 5)]))
        for x in db.test.find():
            for (k, v) in x.items():
                self.assertEqual(k, "_id")
                break

    def test_deref(self):
        db = self.connection.pymongo_test
        db.test.remove({})

        self.assertRaises(TypeError, db.dereference, 5)
        self.assertRaises(TypeError, db.dereference, "hello")
        self.assertRaises(TypeError, db.dereference, None)

        self.assertEqual(None, db.dereference(DBRef("test", ObjectId())))
        obj = {"x": True}
        key = db.test.save(obj)
        self.assertEqual(obj, db.dereference(DBRef("test", key)))
        self.assertEqual(obj, db.dereference(DBRef("test", key, "pymongo_test")))
        self.assertRaises(ValueError, db.dereference, DBRef("test", key, "foo"))

        self.assertEqual(None, db.dereference(DBRef("test", 4)))
        obj = {"_id": 4}
        db.test.save(obj)
        self.assertEqual(obj, db.dereference(DBRef("test", 4)))

    def test_eval(self):
        db = self.connection.pymongo_test
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
        db = Database(self.connection, "pymongo_test")
        db.test.remove({})

        a_doc = SON({"hello": u"world"})
        a_key = db.test.save(a_doc)
        self.assert_(isinstance(a_doc["_id"], ObjectId))
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
        db = self.connection.pymongo_test
        db.test.remove({})
        db.test.save({"x": 9223372036854775807L})
        self.assertEqual(9223372036854775807L, db.test.find_one()["x"])

    def test_remove(self):
        db = self.connection.pymongo_test
        db.test.remove({})

        self.assertRaises(TypeError, db.test.remove, 5)
        self.assertRaises(TypeError, db.test.remove, "test")
        self.assertRaises(TypeError, db.test.remove, [])

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

        self.assert_(db.test.find_one({"x": 2}))
        db.test.remove({"x": 2})
        self.failIf(db.test.find_one({"x": 2}))

        self.assert_(db.test.find_one())
        db.test.remove({})
        self.failIf(db.test.find_one())

    def test_save_a_bunch(self):
        db = self.connection.pymongo_test
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
        db = self.connection.pymongo_test
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
        db = self.connection.pymongo_test
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
        db = self.connection.pymongo_test
        db.system.js.remove()

        self.assertEqual(0, db.system.js.count())
        db.system_js.add = "function(a, b) { return a + b; }"
        self.assertEqual(1, db.system.js.count())
        self.assertEqual(6, db.system_js.add(1, 5))

        del db.system_js.add
        self.assertEqual(0, db.system.js.count())
        if version.at_least(db.connection, (1, 3, 2, -1)):
            self.assertRaises(OperationFailure, db.system_js.add, 1, 5)

        # TODO right now CodeWScope doesn't work w/ system js
        # db.system_js.scope = Code("return hello;", {"hello": 8})
        # self.assertEqual(8, db.system_js.scope())

        self.assertRaises(OperationFailure, db.system_js.non_existant)

        db.system_js.no_param = Code("return 5;")
        self.assertEqual(5, db.system_js.no_param())


if __name__ == "__main__":
    unittest.main()
