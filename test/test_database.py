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
import unittest

from nose.plugins.skip import SkipTest

from bson.binary import JAVA_LEGACY
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.regex import Regex
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.son import SON, RE_TYPE
from pymongo import (ALL,
                     auth,
                     OFF,
                     SLOW_ONLY,
                     helpers)
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (CollectionInvalid,
                            ExecutionTimeout,
                            InvalidName,
                            OperationFailure)
from pymongo.read_preferences import ReadPreference, Secondary
from pymongo.son_manipulator import (AutoReference,
                                     NamespaceInjector,
                                     SONManipulator,
                                     ObjectIdShuffler)
from pymongo.write_concern import WriteConcern
from test import version, skip_restricted_localhost
from test.utils import (catch_warnings, get_command_line,
                        is_mongos, server_started_with_auth)
from test.test_client import get_client


setUpModule = skip_restricted_localhost


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.client = get_client()

    def tearDown(self):
        self.client = None

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

    def test_get_collection(self):
        db = Database(self.client, "pymongo_test")
        codec_options = CodecOptions(
            tz_aware=True, uuid_representation=JAVA_LEGACY)
        write_concern = WriteConcern(w=2, j=True)
        coll = db.get_collection(
            'foo', codec_options, ReadPreference.SECONDARY, write_concern)
        self.assertEqual('foo', coll.name)
        self.assertEqual(codec_options, coll.codec_options)
        self.assertEqual(JAVA_LEGACY, coll.uuid_subtype)
        self.assertEqual(ReadPreference.SECONDARY, coll.read_preference)
        self.assertEqual(write_concern.document, coll.write_concern)

        pref = Secondary([{"dc": "sf"}])
        coll = db.get_collection('foo', read_preference=pref)
        self.assertEqual(pref.mode, coll.read_preference)
        self.assertEqual(pref.tag_sets, coll.tag_sets)
        self.assertEqual(db.codec_options, coll.codec_options)
        self.assertEqual(db.uuid_subtype, coll.uuid_subtype)
        self.assertEqual(db.write_concern, coll.write_concern)

    def test_create_collection(self):
        db = Database(self.client, "pymongo_test")

        db.test.insert({"hello": "world"})
        self.assertRaises(CollectionInvalid, db.create_collection, "test")

        db.drop_collection("test")

        self.assertRaises(TypeError, db.create_collection, 5)
        self.assertRaises(TypeError, db.create_collection, None)
        self.assertRaises(InvalidName, db.create_collection, "coll..ection")

        test = db.create_collection("test")
        self.assertTrue(u"test" in db.collection_names())
        test.save({"hello": u"world"})
        self.assertEqual(db.test.find_one()["hello"], "world")

        db.drop_collection("test.foo")
        db.create_collection("test.foo")
        self.assertTrue(u"test.foo" in db.collection_names())
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

        colls_without_systems = db.collection_names(False)
        for coll in colls_without_systems:
            self.assertTrue(not coll.startswith("system."))

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
        if version.at_least(db.connection, (1, 9, 1, -1)):
            self.assertTrue(isinstance(info[0]['responseLength'], int))
            self.assertTrue(isinstance(info[0]['millis'], int))
            self.assertTrue(isinstance(info[0]['client'], basestring))
            self.assertTrue(isinstance(info[0]['user'], basestring))
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
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
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
        finally:
            ctx.exit()

    def test_command(self):
        db = self.client.admin

        self.assertEqual(db.command("buildinfo"), db.command({"buildinfo": 1}))

    def test_command_ignores_network_timeout(self):
        # command() should ignore network_timeout.
        if not version.at_least(self.client, (1, 9, 0)):
            raise SkipTest("Need sleep() to test command with network timeout")

        db = self.client.pymongo_test

        # No errors.
        db.test.remove()
        db.test.insert({})
        cursor = db.test.find(
            {'$where': 'sleep(100); return true'}, network_timeout=0.001)

        self.assertEqual(1, cursor.count())
        # mongos doesn't support the eval command
        if not is_mongos(self.client):
            db.command('eval', 'sleep(100)', network_timeout=0.001)

    def test_command_with_compile_re(self):
        # We use 'aggregate' as our example command, since it's an easy way to
        # retrieve a BSON regex from a collection using a command. But until
        # MongoDB 2.3.2, aggregation turned regexes into strings: SERVER-6470.
        if not version.at_least(self.client, (2, 3, 2)):
            raise SkipTest(
                "Retrieving a regex with aggregation requires "
                "MongoDB >= 2.3.2")

        db = self.client.pymongo_test
        db.test.drop()
        db.test.insert({'r': re.compile('.*')})

        result = db.command('aggregate', 'test', pipeline=[])
        self.assertTrue(isinstance(result['result'][0]['r'], RE_TYPE))
        result = db.command('aggregate', 'test', pipeline=[], compile_re=False)
        self.assertTrue(isinstance(result['result'][0]['r'], Regex))

    def test_last_status(self):
        db = self.client.pymongo_test

        db.test.remove({})
        db.test.save({"i": 1})

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            db.test.update({"i": 1}, {"$set": {"i": 2}}, w=0)
            self.assertTrue(db.last_status()["updatedExisting"])

            db.test.update({"i": 1}, {"$set": {"i": 500}}, w=0)
            self.assertFalse(db.last_status()["updatedExisting"])
        finally:
            ctx.exit()

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

    def test_id_ordering(self):
        # PyMongo attempts to have _id show up first
        # when you iterate key/value pairs in a document.
        # This isn't reliable since python dicts don't
        # guarantee any particular order. This will never
        # work right in Jython or any Python or environment
        # with hash randomization enabled (e.g. tox).
        db = self.client.pymongo_test
        db.test.remove({})
        db.test.insert(SON([("hello", "world"),
                            ("_id", 5)]))

        cursor = db.test.find(as_class=SON)
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

    def test_deref_kwargs(self):
        db = self.client.pymongo_test
        db.test.remove({})

        db.test.insert({"_id": 4, "foo": "bar"})
        self.assertEqual(SON([("foo", "bar")]),
                         db.dereference(DBRef("test", 4),
                                        fields={"_id": False},
                                        as_class=SON))

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
        self.assertEqual([], db.incoming_manipulators)
        self.assertEqual([], db.incoming_copying_manipulators)
        self.assertEqual([], db.outgoing_manipulators)
        self.assertEqual([], db.outgoing_copying_manipulators)
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())
        db.add_son_manipulator(ObjectIdShuffler())
        self.assertEqual(1, len(db.incoming_manipulators))
        self.assertEqual(db.incoming_manipulators, ['NamespaceInjector'])
        self.assertEqual(2, len(db.incoming_copying_manipulators))
        for name in db.incoming_copying_manipulators:
            self.assertTrue(name in ('ObjectIdShuffler', 'AutoReference'))
        self.assertEqual([], db.outgoing_manipulators)
        self.assertEqual(['AutoReference'], db.outgoing_copying_manipulators)

    def test_command_response_without_ok(self):
        # Sometimes (SERVER-10891) the server's response to a badly-formatted
        # command document will have no 'ok' field. We should raise
        # OperationFailure instead of KeyError.
        self.assertRaises(
            OperationFailure,
            helpers._check_command_response, {}, reset=None)

        try:
            helpers._check_command_response({'$err': 'foo'}, reset=None)
        except OperationFailure, e:
            self.assertEqual(e.args[0], 'foo')
        else:
            self.fail("_check_command_response didn't raise OperationFailure")

    def test_mongos_response(self):
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {'ok': 0, 'errmsg': 'inner'}}}

        try:
            helpers._check_command_response(error_document, reset=None)
        except OperationFailure, exc:
            self.assertEqual('inner', str(exc))
        else:
            self.fail('OperationFailure not raised')

        # If a shard has no primary and you run a command like dbstats, which
        # cannot be run on a secondary, mongos's response includes empty "raw"
        # errors. See SERVER-15428.
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {}}}

        try:
            helpers._check_command_response(error_document, reset=None)
        except OperationFailure, exc:
            self.assertEqual('outer', str(exc))
        else:
            self.fail('OperationFailure not raised')

        # Raw error has ok: 0 but no errmsg. Not a known case, but test it.
        error_document = {
            'ok': 0,
            'errmsg': 'outer',
            'raw': {'shard0/host0,host1': {'ok': 0}}}

        try:
            helpers._check_command_response(error_document, reset=None)
        except OperationFailure, exc:
            self.assertEqual('outer', str(exc))
        else:
            self.fail('OperationFailure not raised')

    def test_command_max_time_ms(self):
        if not version.at_least(self.client, (2, 5, 3, -1)):
            raise SkipTest("MaxTimeMS requires MongoDB >= 2.5.3")
        if "enableTestCommands=1" not in get_command_line(self.client)["argv"]:
            raise SkipTest("Test commands must be enabled.")

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

    def test_object_to_dict_transformer(self):
        # PYTHON-709: Some users rely on their custom SONManipulators to run
        # before any other checks, so they can insert non-dict objects and
        # have them dictified before the _id is inserted or any other
        # processing.
        class Thing(object):
            def __init__(self, value):
                self.value = value

        class ThingTransformer(SONManipulator):
            def transform_incoming(self, thing, collection):
                return {'value': thing.value}

        db = self.client.foo
        db.add_son_manipulator(ThingTransformer())
        t = Thing('value')

        db.test.remove()
        db.test.insert([t])
        out = db.test.find_one()
        self.assertEqual('value', out.get('value'))

    def test_son_manipulator_outgoing(self):
        class Thing(object):
            def __init__(self, value):
                self.value = value

        class ThingTransformer(SONManipulator):
            def transform_outgoing(self, doc, collection):
                # We don't want this applied to the command return
                # value in pymongo.cursor.Cursor.
                if 'value' in doc:
                    return Thing(doc['value'])
                return doc

        db = self.client.foo
        db.add_son_manipulator(ThingTransformer())

        db.test.remove()
        db.test.insert({'value': 'value'})
        out = db.test.find_one()
        self.assertTrue(isinstance(out, Thing))
        self.assertEqual('value', out.value)

        if version.at_least(self.client, (2, 6)):
            out = db.test.aggregate([], cursor={}).next()
            self.assertTrue(isinstance(out, Thing))
            self.assertEqual('value', out.value)

    def test_son_manipulator_inheritance(self):
        class Thing(object):
            def __init__(self, value):
                self.value = value

        class ThingTransformer(SONManipulator):
            def transform_incoming(self, thing, collection):
                return {'value': thing.value}

            def transform_outgoing(self, son, collection):
                return Thing(son['value'])

        class Child(ThingTransformer):
            pass

        db = self.client.foo
        db.add_son_manipulator(Child())
        t = Thing('value')

        db.test.remove()
        db.test.insert([t])
        out = db.test.find_one()
        self.assertTrue(isinstance(out, Thing))
        self.assertEqual('value', out.value)

if __name__ == "__main__":
    unittest.main()
