# Copyright 2009-2011 10gen, Inc.
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

import unittest

from pymongo.connection import Connection
from pymongo.errors import ConfigurationError, OperationFailure


class TestCommon(unittest.TestCase):

    def test_baseobject(self):

        c = Connection()
        self.assertFalse(c.slave_okay)
        self.assertFalse(c.safe)
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

        c = Connection(slaveok=True, w=1, wtimeout=300, fsync=True, j=True)
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

        c = Connection('mongodb://localhost:27017/?'
                       'slaveok=true;w=1;wtimeout=300;fsync=true;j=true')
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

        db.set_lasterror_options(w=1)
        self.assertEqual({'j': True}, coll.get_lasterror_options())
        self.assertEqual({'w': 1}, db.get_lasterror_options())
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
        self.assertEquals(None, coll.find_one(slave_okay=True))
        coll.unset_lasterror_options()
        coll.set_lasterror_options(w=4, wtimeout=10)
        # Fails if we don't have 4 active nodes or we don't have replication...
        self.assertRaises(OperationFailure, coll.insert, {'foo': 'bar'})
        # Succeeds since we override the lasterror settings per query.
        self.assert_(coll.insert({'foo': 'bar'}, fsync=True))
        c.drop_database(db)


if __name__ == "__main__":
    unittest.main()
