# Copyright 2009 10gen, Inc.
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

"""Tests for the grid_file module.
"""

import unittest
import datetime

import qcheck
from test_connection import get_connection
from gridfs.grid_file import GridFile

class TestGridFile(unittest.TestCase):
    def setUp(self):
        self.db = get_connection().test

    def test_basic(self):
        self.db._files.remove({})
        self.db._chunks.remove({})

        self.assertEqual(self.db._files.find().count(), 0)
        self.assertEqual(self.db._chunks.find().count(), 0)
        file = GridFile({"filename": "test"}, self.db, "w")
        file.write("hello world")
        file.close()

        self.assertEqual(self.db._files.find().count(), 1)
        self.assertEqual(self.db._chunks.find().count(), 1)

        file = GridFile({"filename": "test"}, self.db)
        self.assertEqual(file.read(), "hello world")
        file.close()

        # make sure it's still there...
        file = GridFile({"filename": "test"}, self.db)
        self.assertEqual(file.read(), "hello world")
        file.close()

        file = GridFile({"filename": "test"}, self.db, "w")
        file.close()

        self.assertEqual(self.db._files.find().count(), 1)
        self.assertEqual(self.db._chunks.find().count(), 0)

        file = GridFile({"filename": "test"}, self.db)
        self.assertEqual(file.next, None)
        self.assertEqual(file.read(), "")
        file.close()

    def test_create_grid_file(self):
        self.db._files.remove({})
        self.db._chunks.remove({})

        # just write a blank file so that reads on {} don't fail
        file = GridFile({"filename": "test"}, self.db, "w")
        file.close()

        self.assertRaises(TypeError, GridFile, "hello", self.db)
        self.assertRaises(TypeError, GridFile, None, self.db)
        self.assertRaises(TypeError, GridFile, 5, self.db)

        self.assertRaises(TypeError, GridFile, {}, "hello")
        self.assertRaises(TypeError, GridFile, {}, None)
        self.assertRaises(TypeError, GridFile, {}, 5)
        self.assertTrue(GridFile({}, self.db))

        self.assertRaises(TypeError, GridFile, {}, self.db, None)
        self.assertRaises(TypeError, GridFile, {}, self.db, 5)
        self.assertRaises(TypeError, GridFile, {}, self.db, [])
        self.assertRaises(ValueError, GridFile, {}, self.db, "m")
        self.assertRaises(ValueError, GridFile, {}, self.db, u"m")
        self.assertTrue(GridFile({}, self.db, "r"))
        self.assertTrue(GridFile({}, self.db, u"r"))
        self.assertTrue(GridFile({}, self.db, "w"))
        self.assertTrue(GridFile({}, self.db, u"w"))

        self.assertRaises(TypeError, GridFile, {}, self.db, "r", None)
        self.assertRaises(TypeError, GridFile, {}, self.db, "r", 5)
        self.assertRaises(TypeError, GridFile, {}, self.db, "r", [])

        self.assertRaises(IOError, GridFile, {"filename": "mike"}, self.db)
        self.assertTrue(GridFile({"filename": "test"}, self.db))

    def test_properties(self):
        self.db._files.remove({})
        self.db._chunks.remove({})

        file = GridFile({"filename": "test"}, self.db, "w")
        self.assertEqual(file.mode, "w")
        self.assertFalse(file.closed)
        file.close()
        self.assertTrue(file.closed)

        self.assertRaises(IOError, GridFile, {"filename": "mike"}, self.db)
        a = GridFile({"filename": "test"}, self.db)

        self.assertEqual(a.mode, "r")
        self.assertFalse(a.closed)

        self.assertEqual(a.length, 0)
        self.assertEqual(a.content_type, None)
        self.assertEqual(a.name, "test")
        self.assertEqual(a.chunk_size, 256000)
        self.assertTrue(isinstance(a.upload_date, datetime.datetime))
        self.assertEqual(a.aliases, None)
        self.assertEqual(a.next, None)

        a.content_type = "something"
        self.assertEqual(a.content_type, "something")

        def set_length():
            a.length = 10
        self.assertRaises(AttributeError, set_length)

        def set_chunk_size():
            a.chunk_size = 100
        self.assertRaises(AttributeError, set_chunk_size)

        def set_upload_date():
            a.upload_date = datetime.datetime.now()
        self.assertRaises(AttributeError, set_upload_date)

        a.aliases = ["hello", "world"]
        self.assertEqual(a.aliases, ["hello", "world"])

        def set_next():
            a.next = None
        self.assertRaises(AttributeError, set_next)

        a.name = "mike"
        a.close()

        self.assertRaises(IOError, GridFile, {"filename": "test"}, self.db)
        a = GridFile({"filename": "mike"}, self.db)

if __name__ == "__main__":
    unittest.main()
