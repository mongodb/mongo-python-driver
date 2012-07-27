# Copyright 2011-2012 10gen, Inc.
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

import os
import unittest
import warnings

from pymongo.connection import Connection
from pymongo.errors import ConfigurationError, OperationFailure
from test.utils import drop_collections


host = os.environ.get("DB_IP", 'localhost')
port = int(os.environ.get("DB_PORT", 27017))
pair = '%s:%d' % (host, port)


class TestCommon(unittest.TestCase):

    def test_baseobject(self):

        # In Python 2.6+ we could use the catch_warnings context
        # manager to test this warning nicely. As we can't do that
        # we must test raising errors before the ignore filter is applied.
        warnings.simplefilter("error", UserWarning)
        self.assertRaises(UserWarning, lambda:
                Connection(wtimeout=1000, safe=False))
        try:
            Connection(wtimeout=1000, safe=True)
        except UserWarning:
            self.assertFalse(True)

        try:
            Connection(wtimeout=1000)
        except UserWarning:
            self.assertFalse(True)

        warnings.resetwarnings()

        warnings.simplefilter("ignore")

        c = Connection(pair)
        self.assertFalse(c.slave_okay)
        self.assertFalse(c.safe)
        self.assertEqual({}, c.get_lasterror_options())
        db = c.test
        db.drop_collection("test")
        self.assertFalse(db.slave_okay)
        self.assertFalse(db.safe)
        self.assertEqual({}, db.get_lasterror_options())
        coll = db.test
        self.assertFalse(coll.slave_okay)
        self.assertFalse(coll.safe)
        self.assertEqual({}, coll.get_lasterror_options())
        cursor = coll.find()
        self.assertFalse(cursor._Cursor__slave_okay)
        cursor = coll.find(slave_okay=True)
        self.assertTrue(cursor._Cursor__slave_okay)

        # Setting any safe operations overrides explicit safe
        self.assertTrue(Connection(wtimeout=1000, safe=False).safe)

        c = Connection(pair, slaveok=True, w='majority',
                                     wtimeout=300, fsync=True, j=True)
        self.assertTrue(c.slave_okay)
        self.assertTrue(c.safe)
        d = {'w': 'majority', 'wtimeout': 300, 'fsync': True, 'j': True}
        self.assertEqual(d, c.get_lasterror_options())
        db = c.test
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

        c = Connection('mongodb://%s/?'
                       'w=2;wtimeoutMS=300;fsync=true;'
                       'journal=true' % (pair,))
        self.assertTrue(c.safe)
        d = {'w': 2, 'wtimeout': 300, 'fsync': True, 'j': True}
        self.assertEqual(d, c.get_lasterror_options())

        c = Connection('mongodb://%s/?'
                       'slaveok=true;w=1;wtimeout=300;'
                       'fsync=true;j=true' % (pair,))
        self.assertTrue(c.slave_okay)
        self.assertTrue(c.safe)
        d = {'w': 1, 'wtimeout': 300, 'fsync': True, 'j': True}
        self.assertEqual(d, c.get_lasterror_options())
        db = c.test
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

        c.unset_lasterror_options()
        self.assertTrue(c.slave_okay)
        self.assertTrue(c.safe)
        c.safe = False
        self.assertFalse(c.safe)
        c.slave_okay = False
        self.assertFalse(c.slave_okay)
        self.assertEqual({}, c.get_lasterror_options())
        db = c.test
        self.assertFalse(db.slave_okay)
        self.assertFalse(db.safe)
        self.assertEqual({}, db.get_lasterror_options())
        coll = db.test
        self.assertFalse(coll.slave_okay)
        self.assertFalse(coll.safe)
        self.assertEqual({}, coll.get_lasterror_options())
        cursor = coll.find()
        self.assertFalse(cursor._Cursor__slave_okay)
        cursor = coll.find(slave_okay=True)
        self.assertTrue(cursor._Cursor__slave_okay)

        coll.set_lasterror_options(j=True)
        self.assertEqual({'j': True}, coll.get_lasterror_options())
        self.assertEqual({}, db.get_lasterror_options())
        self.assertFalse(db.safe)
        self.assertEqual({}, c.get_lasterror_options())
        self.assertFalse(c.safe)

        db.set_lasterror_options(w='majority')
        self.assertEqual({'j': True}, coll.get_lasterror_options())
        self.assertEqual({'w': 'majority'}, db.get_lasterror_options())
        self.assertEqual({}, c.get_lasterror_options())
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

        warnings.resetwarnings()


if __name__ == "__main__":
    unittest.main()
