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

"""Tests for the gridfs package.
"""

import unittest

try:
    import pymongo
except ImportError:
    import sys
    sys.path[0:0] = [""]

import gridfs
from test_connection import get_connection

class TestGridfs(unittest.TestCase):
    def setUp(self):
        self.db = get_connection().test
        self.db._files.remove({})
        self.db._chunks.remove({})
        self.fs = gridfs.GridFS(self.db)

    def test_open(self):
        self.assertRaises(IOError, self.fs.open, "my file", "r")
        f = self.fs.open("my file", "w")
        f.write("hello gridfs world!")
        f.close()

        g = self.fs.open("my file", "r")
        self.assertEqual("hello gridfs world!", g.read())
        g.close()

    def test_list(self):
        self.assertEqual(self.fs.list(), [])

        f = self.fs.open("mike", "w")
        f.close()

        f = self.fs.open("test", "w")
        f.close()

        f = self.fs.open("hello world", "w")
        f.close()

        self.assertEqual(["mike", "test", "hello world"], self.fs.list())

    def test_remove(self):
        self.assertRaises(TypeError, self.fs.remove, 5)
        self.assertRaises(TypeError, self.fs.remove, None)
        self.assertRaises(TypeError, self.fs.remove, [])

        f = self.fs.open("mike", "w")
        f.write("hi")
        f.close()
        f = self.fs.open("test", "w")
        f.write("bye")
        f.close()
        f = self.fs.open("hello world", "w")
        f.write("fly")
        f.close()
        self.assertEqual(["mike", "test", "hello world"], self.fs.list())
        self.assertEqual(self.db._files.find().count(), 3)
        self.assertEqual(self.db._chunks.find().count(), 3)

        self.fs.remove("test")

        self.assertEqual(["mike", "hello world"], self.fs.list())
        self.assertEqual(self.db._files.find().count(), 2)
        self.assertEqual(self.db._chunks.find().count(), 2)
        self.assertEqual(self.fs.open("mike").read(), "hi")
        self.assertEqual(self.fs.open("hello world").read(), "fly")
        self.assertRaises(IOError, self.fs.open, "test")

        self.fs.remove({})

        self.assertEqual([], self.fs.list())
        self.assertEqual(self.db._files.find().count(), 0)
        self.assertEqual(self.db._chunks.find().count(), 0)
        self.assertRaises(IOError, self.fs.open, "test")
        self.assertRaises(IOError, self.fs.open, "mike")
        self.assertRaises(IOError, self.fs.open, "hello world")

if __name__ == "__main__":
    unittest.main()
