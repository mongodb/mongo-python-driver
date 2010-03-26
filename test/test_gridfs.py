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

"""Tests for the gridfs package.
"""

import datetime
import unittest
import threading
import sys
sys.path[0:0] = [""]

import gridfs
from gridfs.errors import NoFile
from test_connection import get_connection


class JustWrite(threading.Thread):

    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs

    def run(self):
        for _ in range(10):
            file = self.fs.open("test", "w")
            file.write("hello")
            file.close()


class JustRead(threading.Thread):

    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs

    def run(self):
        for _ in range(10):
            file = self.fs.open("test")
            assert file.read() == "hello"
            file.close()


class TestGridfs(unittest.TestCase):

    def setUp(self):
        self.db = get_connection().pymongo_test
        self.db.drop_collection("fs.files")
        self.db.drop_collection("fs.chunks")
        self.db.drop_collection("pymongo_test.files")
        self.db.drop_collection("pymongo_test.chunks")
        self.fs = gridfs.GridFS(self.db)
        self.alt = gridfs.GridFS(self.db, "pymongo_test")

    def test_basic(self):
        oid = self.fs.put("hello world")
        self.assertEqual("hello world", self.fs.get(oid).read())
        self.assertEqual(1, self.db.fs.files.count())
        self.assertEqual(1, self.db.fs.chunks.count())

        self.fs.delete(oid)
        self.assertRaises(NoFile, self.fs.get, oid)
        self.assertEqual(0, self.db.fs.files.count())
        self.assertEqual(0, self.db.fs.chunks.count())

        self.assertRaises(NoFile, self.fs.get, "foo")
        oid = self.fs.put("hello world", _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual("hello world", self.fs.get("foo").read())


    def test_list(self):
        self.assertEqual([], self.fs.list())
        self.fs.put("hello world")
        self.assertEqual([], self.fs.list())

        self.fs.put("", filename="mike")
        self.fs.put("foo", filename="test")
        self.fs.put("", filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]),
                         set(self.fs.list()))

    def test_empty_file(self):
        oid = self.fs.put("")
        self.assertEqual("", self.fs.get(oid).read())
        self.assertEqual(1, self.db.fs.files.count())
        self.assertEqual(0, self.db.fs.chunks.count())

        raw = self.db.fs.files.find_one()
        self.assertEqual(0, raw["length"])
        self.assertEqual(oid, raw["_id"])
        self.assert_(isinstance(raw["uploadDate"], datetime.datetime))
        self.assertEqual(256*1024, raw["chunkSize"])
        self.assert_(isinstance(raw["md5"], basestring))


    # def test_remove(self):
    #     self.assertRaises(TypeError, self.fs.remove, 5)
    #     self.assertRaises(TypeError, self.fs.remove, None)
    #     self.assertRaises(TypeError, self.fs.remove, [])

    #     f = self.fs.open("mike", "w")
    #     f.write("hi")
    #     f.close()
    #     f = self.fs.open("test", "w")
    #     f.write("bye")
    #     f.close()
    #     f = self.fs.open("hello world", "w")
    #     f.write("fly")
    #     f.close()
    #     self.assertEqual(set(["mike", "test", "hello world"]),
    #                      set(self.fs.list()))
    #     self.assertEqual(self.db.fs.files.find().count(), 3)
    #     self.assertEqual(self.db.fs.chunks.find().count(), 3)

    #     self.fs.remove("test")

    #     self.assertEqual(set(["mike", "hello world"]), set(self.fs.list()))
    #     self.assertEqual(self.db.fs.files.find().count(), 2)
    #     self.assertEqual(self.db.fs.chunks.find().count(), 2)
    #     f = self.fs.open("mike")
    #     self.assertEqual(f.read(), "hi")
    #     f.close()
    #     f = self.fs.open("hello world")
    #     self.assertEqual(f.read(), "fly")
    #     f.close()
    #     self.assertRaises(IOError, self.fs.open, "test")

    #     self.fs.remove({})

    #     self.assertEqual([], self.fs.list())
    #     self.assertEqual(self.db.fs.files.find().count(), 0)
    #     self.assertEqual(self.db.fs.chunks.find().count(), 0)
    #     self.assertRaises(IOError, self.fs.open, "test")
    #     self.assertRaises(IOError, self.fs.open, "mike")
    #     self.assertRaises(IOError, self.fs.open, "hello world")

    # def test_open_alt_coll(self):
    #     f = self.fs.open("my file", "w", "pymongo_test")
    #     f.write("hello gridfs world!")
    #     f.close()

    #     self.assertRaises(IOError, self.fs.open, "my file", "r")
    #     g = self.fs.open("my file", "r", "pymongo_test")
    #     self.assertEqual("hello gridfs world!", g.read())
    #     g.close()

    # def test_list_alt_coll(self):
    #     f = self.fs.open("mike", "w", "pymongo_test")
    #     f.close()

    #     f = self.fs.open("test", "w", "pymongo_test")
    #     f.close()

    #     f = self.fs.open("hello world", "w", "pymongo_test")
    #     f.close()

    #     self.assertEqual([], self.fs.list())
    #     self.assertEqual(set(["mike", "test", "hello world"]),
    #                      set(self.fs.list("pymongo_test")))

    # def test_remove_alt_coll(self):
    #     f = self.fs.open("mike", "w", "pymongo_test")
    #     f.write("hi")
    #     f.close()
    #     f = self.fs.open("test", "w", "pymongo_test")
    #     f.write("bye")
    #     f.close()
    #     f = self.fs.open("hello world", "w", "pymongo_test")
    #     f.write("fly")
    #     f.close()

    #     self.fs.remove("test")
    #     self.assertEqual(set(["mike", "test", "hello world"]),
    #                      set(self.fs.list("pymongo_test")))
    #     self.fs.remove("test", "pymongo_test")
    #     self.assertEqual(set(["mike", "hello world"]),
    #                      set(self.fs.list("pymongo_test")))

    #     f = self.fs.open("mike", collection="pymongo_test")
    #     self.assertEqual(f.read(), "hi")
    #     f.close()
    #     f = self.fs.open("hello world", collection="pymongo_test")
    #     self.assertEqual(f.read(), "fly")
    #     f.close()

    #     self.fs.remove({}, "pymongo_test")

    #     self.assertEqual([], self.fs.list("pymongo_test"))
    #     self.assertEqual(self.db.pymongo_test.files.find().count(), 0)
    #     self.assertEqual(self.db.pymongo_test.chunks.find().count(), 0)

    # def test_threaded_reads(self):
    #     f = self.fs.open("test", "w")
    #     f.write("hello")
    #     f.close()

    #     threads = []
    #     for i in range(10):
    #         threads.append(JustRead(self.fs))
    #         threads[i].start()

    #     for i in range(10):
    #         threads[i].join()

    # def test_threaded_writes(self):
    #     threads = []
    #     for i in range(10):
    #         threads.append(JustWrite(self.fs))
    #         threads[i].start()

    #     for i in range(10):
    #         threads[i].join()

    #     f = self.fs.open("test")
    #     self.assertEqual(f.read(), "hello")
    #     f.close()

    # # NOTE I do recognize how gross this is. There is no good way to
    # # test the with statement because it is a syntax error in older
    # # python versions.  One option would be to use eval and skip the
    # # test if it is a syntax error.
    # if sys.version_info[:2] == (2, 5):
    #     import gridfs15
    #     test_with_statement = gridfs15.test_with_statement
    # elif sys.version_info[:3] >= (2, 6, 0):
    #     import gridfs16
    #     test_with_statement = gridfs16.test_with_statement

if __name__ == "__main__":
    unittest.main()
