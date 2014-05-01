# Copyright 2011-2014 MongoDB, Inc.
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

"""Test the pymongo common module."""

import sys
import unittest
import warnings

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.binary import UUIDLegacy, OLD_UUID_SUBTYPE, UUID_SUBTYPE
from bson.code import Code
from bson.objectid import ObjectId
from bson.son import SON
from pymongo.connection import Connection
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.errors import ConfigurationError, OperationFailure
from test import host, port, pair, version
from test.utils import catch_warnings, drop_collections

have_uuid = True
try:
    import uuid
except ImportError:
    have_uuid = False


class TestCommon(unittest.TestCase):

    def test_baseobject(self):

        ctx = catch_warnings()
        try:
            warnings.simplefilter("error", UserWarning)
            self.assertRaises(UserWarning, lambda:
                    MongoClient(host, port, wtimeout=1000, w=0))
            try:
                MongoClient(host, port, wtimeout=1000, w=1)
            except UserWarning:
                self.fail()

            try:
                MongoClient(host, port, wtimeout=1000)
            except UserWarning:
                self.fail()
        finally:
            ctx.exit()

        # Connection tests
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            c = Connection(pair)
            self.assertFalse(c.slave_okay)
            self.assertFalse(c.safe)
            self.assertEqual({}, c.get_lasterror_options())
            db = c.pymongo_test
            db.drop_collection("test")
            self.assertFalse(db.slave_okay)
            self.assertFalse(db.safe)
            self.assertEqual({}, db.get_lasterror_options())
            coll = db.test
            self.assertFalse(coll.slave_okay)
            self.assertFalse(coll.safe)
            self.assertEqual({}, coll.get_lasterror_options())

            self.assertEqual((False, {}), coll._get_write_mode())
            coll.safe = False
            coll.write_concern.update(w=1)
            self.assertEqual((True, {}), coll._get_write_mode())
            coll.write_concern.update(w=3)
            self.assertEqual((True, {'w': 3}), coll._get_write_mode())

            coll.safe = True
            coll.write_concern.update(w=0)
            self.assertEqual((False, {}), coll._get_write_mode())

            coll = db.test
            cursor = coll.find()
            self.assertFalse(cursor._Cursor__slave_okay)
            cursor = coll.find(slave_okay=True)
            self.assertTrue(cursor._Cursor__slave_okay)

            # MongoClient test
            c = MongoClient(pair)
            self.assertFalse(c.slave_okay)
            self.assertTrue(c.safe)
            self.assertEqual({}, c.get_lasterror_options())
            db = c.pymongo_test
            db.drop_collection("test")
            self.assertFalse(db.slave_okay)
            self.assertTrue(db.safe)
            self.assertEqual({}, db.get_lasterror_options())
            coll = db.test
            self.assertFalse(coll.slave_okay)
            self.assertTrue(coll.safe)
            self.assertEqual({}, coll.get_lasterror_options())

            self.assertEqual((True, {}), coll._get_write_mode())
            coll.safe = False
            coll.write_concern.update(w=1)
            self.assertEqual((True, {}), coll._get_write_mode())
            coll.write_concern.update(w=3)
            self.assertEqual((True, {'w': 3}), coll._get_write_mode())

            coll.safe = True
            coll.write_concern.update(w=0)
            self.assertEqual((False, {}), coll._get_write_mode())

            coll = db.test
            cursor = coll.find()
            self.assertFalse(cursor._Cursor__slave_okay)
            cursor = coll.find(slave_okay=True)
            self.assertTrue(cursor._Cursor__slave_okay)

            # Setting any safe operations overrides explicit safe
            self.assertTrue(MongoClient(host, port, wtimeout=1000, safe=False).safe)

            c = MongoClient(pair, slaveok=True, w='majority',
                            wtimeout=300, fsync=True, j=True)
            self.assertTrue(c.slave_okay)
            self.assertTrue(c.safe)
            d = {'w': 'majority', 'wtimeout': 300, 'fsync': True, 'j': True}
            self.assertEqual(d, c.get_lasterror_options())
            db = c.pymongo_test
            self.assertTrue(db.slave_okay)
            self.assertTrue(db.safe)
            self.assertEqual(d, db.get_lasterror_options())
            coll = db.test
            self.assertTrue(coll.slave_okay)
            self.assertTrue(coll.safe)
            self.assertEqual(d, coll.get_lasterror_options())
            cursor = coll.find()
            self.assertTrue(cursor._Cursor__slave_okay)
            cursor = coll.find(slave_okay=False)
            self.assertFalse(cursor._Cursor__slave_okay)

            c = MongoClient('mongodb://%s/?'
                           'w=2;wtimeoutMS=300;fsync=true;'
                           'journal=true' % (pair,))
            self.assertTrue(c.safe)
            d = {'w': 2, 'wtimeout': 300, 'fsync': True, 'j': True}
            self.assertEqual(d, c.get_lasterror_options())

            c = MongoClient('mongodb://%s/?'
                           'slaveok=true;w=1;wtimeout=300;'
                           'fsync=true;j=true' % (pair,))
            self.assertTrue(c.slave_okay)
            self.assertTrue(c.safe)
            d = {'w': 1, 'wtimeout': 300, 'fsync': True, 'j': True}
            self.assertEqual(d, c.get_lasterror_options())
            self.assertEqual(d, c.write_concern)
            db = c.pymongo_test
            self.assertTrue(db.slave_okay)
            self.assertTrue(db.safe)
            self.assertEqual(d, db.get_lasterror_options())
            self.assertEqual(d, db.write_concern)
            coll = db.test
            self.assertTrue(coll.slave_okay)
            self.assertTrue(coll.safe)
            self.assertEqual(d, coll.get_lasterror_options())
            self.assertEqual(d, coll.write_concern)
            cursor = coll.find()
            self.assertTrue(cursor._Cursor__slave_okay)
            cursor = coll.find(slave_okay=False)
            self.assertFalse(cursor._Cursor__slave_okay)

            c.unset_lasterror_options()
            self.assertTrue(c.slave_okay)
            self.assertTrue(c.safe)
            c.safe = False
            self.assertFalse(c.safe)
            c.slave_okay = False
            self.assertFalse(c.slave_okay)
            self.assertEqual({}, c.get_lasterror_options())
            self.assertEqual({}, c.write_concern)
            db = c.pymongo_test
            self.assertFalse(db.slave_okay)
            self.assertFalse(db.safe)
            self.assertEqual({}, db.get_lasterror_options())
            self.assertEqual({}, db.write_concern)
            coll = db.test
            self.assertFalse(coll.slave_okay)
            self.assertFalse(coll.safe)
            self.assertEqual({}, coll.get_lasterror_options())
            self.assertEqual({}, coll.write_concern)
            cursor = coll.find()
            self.assertFalse(cursor._Cursor__slave_okay)
            cursor = coll.find(slave_okay=True)
            self.assertTrue(cursor._Cursor__slave_okay)

            coll.set_lasterror_options(fsync=True)
            self.assertEqual({'fsync': True}, coll.get_lasterror_options())
            self.assertEqual({'fsync': True}, coll.write_concern)
            self.assertEqual({}, db.get_lasterror_options())
            self.assertEqual({}, db.write_concern)
            self.assertFalse(db.safe)
            self.assertEqual({}, c.get_lasterror_options())
            self.assertEqual({}, c.write_concern)
            self.assertFalse(c.safe)

            db.set_lasterror_options(w='majority')
            self.assertEqual({'fsync': True}, coll.get_lasterror_options())
            self.assertEqual({'fsync': True}, coll.write_concern)
            self.assertEqual({'w': 'majority'}, db.get_lasterror_options())
            self.assertEqual({'w': 'majority'}, db.write_concern)
            self.assertEqual({}, c.get_lasterror_options())
            self.assertEqual({}, c.write_concern)
            self.assertFalse(c.safe)
            db.slave_okay = True
            self.assertTrue(db.slave_okay)
            self.assertFalse(c.slave_okay)
            self.assertFalse(coll.slave_okay)
            cursor = coll.find()
            self.assertFalse(cursor._Cursor__slave_okay)
            cursor = db.coll2.find()
            self.assertTrue(cursor._Cursor__slave_okay)
            cursor = db.coll2.find(slave_okay=False)
            self.assertFalse(cursor._Cursor__slave_okay)

            self.assertRaises(ConfigurationError, coll.set_lasterror_options, foo=20)
            self.assertRaises(TypeError, coll._BaseObject__set_slave_okay, 20)
            self.assertRaises(TypeError, coll._BaseObject__set_safe, 20)

            coll.remove()
            self.assertEqual(None, coll.find_one(slave_okay=True))
            coll.unset_lasterror_options()
            coll.set_lasterror_options(w=4, wtimeout=10)
            # Fails if we don't have 4 active nodes or we don't have replication...
            self.assertRaises(OperationFailure, coll.insert, {'foo': 'bar'})
            # Succeeds since we override the lasterror settings per query.
            self.assertTrue(coll.insert({'foo': 'bar'}, fsync=True))
            drop_collections(db)
        finally:
            ctx.exit()

    def test_uuid_subtype(self):
        if not have_uuid:
            raise SkipTest("No uuid module")

        self.client = MongoClient(pair)
        self.db = self.client.pymongo_test
        coll = self.client.pymongo_test.uuid
        coll.drop()

        def change_subtype(collection, subtype):
            collection.uuid_subtype = subtype

        # Test property
        self.assertEqual(OLD_UUID_SUBTYPE, coll.uuid_subtype)
        self.assertRaises(ConfigurationError, change_subtype, coll, 7)
        self.assertRaises(ConfigurationError, change_subtype, coll, 2)

        # Test basic query
        uu = uuid.uuid4()
        # Insert as binary subtype 3
        coll.insert({'uu': uu})
        self.assertEqual(uu, coll.find_one({'uu': uu})['uu'])
        coll.uuid_subtype = UUID_SUBTYPE
        self.assertEqual(UUID_SUBTYPE, coll.uuid_subtype)
        self.assertEqual(None, coll.find_one({'uu': uu}))
        self.assertEqual(uu, coll.find_one({'uu': UUIDLegacy(uu)})['uu'])

        # Test Cursor.count
        self.assertEqual(0, coll.find({'uu': uu}).count())
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual(1, coll.find({'uu': uu}).count())

        # Test remove
        coll.uuid_subtype = UUID_SUBTYPE
        coll.remove({'uu': uu})
        self.assertEqual(1, coll.count())
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        coll.remove({'uu': uu})
        self.assertEqual(0, coll.count())

        # Test save
        coll.insert({'_id': uu, 'i': 0})
        self.assertEqual(1, coll.count())
        self.assertEqual(1, coll.find({'_id': uu}).count())
        self.assertEqual(0, coll.find_one({'_id': uu})['i'])
        doc = coll.find_one({'_id': uu})
        doc['i'] = 1
        coll.save(doc)
        self.assertEqual(1, coll.find_one({'_id': uu})['i'])

        # Test update
        coll.uuid_subtype = UUID_SUBTYPE
        coll.update({'_id': uu}, {'$set': {'i': 2}})
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual(1, coll.find_one({'_id': uu})['i'])
        coll.update({'_id': uu}, {'$set': {'i': 2}})
        self.assertEqual(2, coll.find_one({'_id': uu})['i'])

        # Test Cursor.distinct
        self.assertEqual([2], coll.find({'_id': uu}).distinct('i'))
        coll.uuid_subtype = UUID_SUBTYPE
        self.assertEqual([], coll.find({'_id': uu}).distinct('i'))

        # Test find_and_modify
        self.assertEqual(None, coll.find_and_modify({'_id': uu},
                                                     {'$set': {'i': 5}}))
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual(2, coll.find_and_modify({'_id': uu},
                                                  {'$set': {'i': 5}})['i'])
        self.assertEqual(5, coll.find_one({'_id': uu})['i'])

        # Test command
        db = self.client.pymongo_test
        no_obj_error = "No matching object found"
        result = db.command('findAndModify', 'uuid',
                            allowable_errors=[no_obj_error],
                            uuid_subtype=UUID_SUBTYPE,
                            query={'_id': uu},
                            update={'$set': {'i': 6}})
        self.assertEqual(None, result.get('value'))
        self.assertEqual(5, db.command('findAndModify', 'uuid',
                                       update={'$set': {'i': 6}},
                                       query={'_id': uu})['value']['i'])
        self.assertEqual(6, db.command('findAndModify', 'uuid',
                                       update={'$set': {'i': 7}},
                                       query={'_id': UUIDLegacy(uu)}
                                      )['value']['i'])

        # Test (inline)_map_reduce
        coll.drop()
        coll.insert({"_id": uu, "x": 1, "tags": ["dog", "cat"]})
        coll.insert({"_id": uuid.uuid4(), "x": 3,
                     "tags": ["mouse", "cat", "dog"]})

        map = Code("function () {"
                   "  this.tags.forEach(function(z) {"
                   "    emit(z, 1);"
                   "  });"
                   "}")

        reduce = Code("function (key, values) {"
                      "  var total = 0;"
                      "  for (var i = 0; i < values.length; i++) {"
                      "    total += values[i];"
                      "  }"
                      "  return total;"
                      "}")

        coll.uuid_subtype = UUID_SUBTYPE
        q = {"_id": uu}
        if version.at_least(self.db.connection, (1, 7, 4)):
            result = coll.inline_map_reduce(map, reduce, query=q)
            self.assertEqual([], result)

        result = coll.map_reduce(map, reduce, "results", query=q)
        self.assertEqual(0, db.results.count())

        coll.uuid_subtype = OLD_UUID_SUBTYPE
        q = {"_id": uu}
        if version.at_least(self.db.connection, (1, 7, 4)):
            result = coll.inline_map_reduce(map, reduce, query=q)
            self.assertEqual(2, len(result))

        result = coll.map_reduce(map, reduce, "results", query=q)
        self.assertEqual(2, db.results.count())

        db.drop_collection("result")
        coll.drop()

        # Test group
        coll.insert({"_id": uu, "a": 2})
        coll.insert({"_id": uuid.uuid4(), "a": 1})

        reduce = "function (obj, prev) { prev.count++; }"
        coll.uuid_subtype = UUID_SUBTYPE
        self.assertEqual([],
                         coll.group([], {"_id": uu},
                                     {"count": 0}, reduce))
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual([{"count": 1}],
                         coll.group([], {"_id": uu},
                                    {"count": 0}, reduce))

    def test_write_concern(self):
        c = MongoClient(pair)

        self.assertEqual({}, c.write_concern)
        wc = {'w': 2, 'wtimeout': 1000}
        c.write_concern = wc
        self.assertEqual(wc, c.write_concern)
        wc = {'w': 3, 'wtimeout': 1000}
        c.write_concern['w'] = 3
        self.assertEqual(wc, c.write_concern)
        wc = {'w': 3}
        del c.write_concern['wtimeout']
        self.assertEqual(wc, c.write_concern)

        wc = {'w': 3, 'wtimeout': 1000}
        c = MongoClient(pair, w=3, wtimeout=1000)
        self.assertEqual(wc, c.write_concern)
        wc = {'w': 2, 'wtimeout': 1000}
        c.write_concern = wc
        self.assertEqual(wc, c.write_concern)

        db = c.pymongo_test
        self.assertEqual(wc, db.write_concern)
        coll = db.test
        self.assertEqual(wc, coll.write_concern)
        coll.write_concern = {'j': True}
        self.assertEqual({'j': True}, coll.write_concern)
        self.assertEqual(wc, db.write_concern)

        wc = SON([('w', 2)])
        coll.write_concern = wc
        self.assertEqual(wc.to_dict(), coll.write_concern)

        def f():
            c.write_concern = {'foo': 'bar'}
        self.assertRaises(ConfigurationError, f)

        def f():
            c.write_concern['foo'] = 'bar'
        self.assertRaises(ConfigurationError, f)

        def f():
            c.write_concern = [('foo', 'bar')]
        self.assertRaises(ConfigurationError, f)

    def test_mongo_client(self):
        m = MongoClient(pair, w=0)
        coll = m.pymongo_test.write_concern_test
        coll.drop()

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            doc = {"_id": ObjectId()}
            coll.insert(doc)
            self.assertTrue(coll.insert(doc, safe=False))
            self.assertTrue(coll.insert(doc, w=0))
            self.assertTrue(coll.insert(doc))
            self.assertRaises(OperationFailure, coll.insert, doc, safe=True)
            self.assertRaises(OperationFailure, coll.insert, doc, w=1)

            m = MongoClient(pair)
            coll = m.pymongo_test.write_concern_test
            self.assertTrue(coll.insert(doc, safe=False))
            self.assertTrue(coll.insert(doc, w=0))
            self.assertRaises(OperationFailure, coll.insert, doc)
            self.assertRaises(OperationFailure, coll.insert, doc, safe=True)
            self.assertRaises(OperationFailure, coll.insert, doc, w=1)

            m = MongoClient("mongodb://%s/" % (pair,))
            self.assertTrue(m.safe)
            coll = m.pymongo_test.write_concern_test
            self.assertRaises(OperationFailure, coll.insert, doc)
            m = MongoClient("mongodb://%s/?w=0" % (pair,))
            self.assertFalse(m.safe)
            coll = m.pymongo_test.write_concern_test
            self.assertTrue(coll.insert(doc))
        finally:
            ctx.exit()

        # Equality tests
        self.assertEqual(m, MongoClient("mongodb://%s/?w=0" % (pair,)))
        self.assertFalse(m != MongoClient("mongodb://%s/?w=0" % (pair,)))

    def test_mongo_replica_set_client(self):
        c = MongoClient(pair)
        ismaster = c.admin.command('ismaster')
        if 'setName' in ismaster:
            setname = str(ismaster.get('setName'))
        else:
            raise SkipTest("Not connected to a replica set.")
        m = MongoReplicaSetClient(pair, replicaSet=setname, w=0)
        coll = m.pymongo_test.write_concern_test
        coll.drop()

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            doc = {"_id": ObjectId()}
            coll.insert(doc)
            self.assertTrue(coll.insert(doc, safe=False))
            self.assertTrue(coll.insert(doc, w=0))
            self.assertTrue(coll.insert(doc))
            self.assertRaises(OperationFailure, coll.insert, doc, safe=True)
            self.assertRaises(OperationFailure, coll.insert, doc, w=1)

            m = MongoReplicaSetClient(pair, replicaSet=setname)
            coll = m.pymongo_test.write_concern_test
            self.assertTrue(coll.insert(doc, safe=False))
            self.assertTrue(coll.insert(doc, w=0))
            self.assertRaises(OperationFailure, coll.insert, doc)
            self.assertRaises(OperationFailure, coll.insert, doc, safe=True)
            self.assertRaises(OperationFailure, coll.insert, doc, w=1)

            m = MongoReplicaSetClient("mongodb://%s/?replicaSet=%s" % (pair, setname))
            self.assertTrue(m.safe)
            coll = m.pymongo_test.write_concern_test
            self.assertRaises(OperationFailure, coll.insert, doc)
            m = MongoReplicaSetClient("mongodb://%s/?replicaSet=%s;w=0" % (pair, setname))
            self.assertFalse(m.safe)
            coll = m.pymongo_test.write_concern_test
            self.assertTrue(coll.insert(doc))
        finally:
            ctx.exit()


if __name__ == "__main__":
    unittest.main()
