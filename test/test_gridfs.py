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

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
import datetime
import unittest
import threading
import time
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
            file = self.fs.new_file(filename="test")
            file.write("hello")
            file.close()


class JustRead(threading.Thread):

    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs

    def run(self):
        for _ in range(10):
            file = self.fs.get("test")
            assert file.read() == "hello"


class TestGridfs(unittest.TestCase):

    def setUp(self):
        self.db = get_connection().pymongo_test
        self.db.drop_collection("fs.files")
        self.db.drop_collection("fs.chunks")
        self.db.drop_collection("alt.files")
        self.db.drop_collection("alt.chunks")
        self.fs = gridfs.GridFS(self.db)
        self.alt = gridfs.GridFS(self.db, "alt")

    def test_gridfs(self):
        self.assertRaises(TypeError, gridfs.GridFS, "foo")
        self.assertRaises(TypeError, gridfs.GridFS, self.db, 5)

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

    def test_alt_collection(self):
        oid = self.alt.put("hello world")
        self.assertEqual("hello world", self.alt.get(oid).read())
        self.assertEqual(1, self.db.alt.files.count())
        self.assertEqual(1, self.db.alt.chunks.count())

        self.alt.delete(oid)
        self.assertRaises(NoFile, self.alt.get, oid)
        self.assertEqual(0, self.db.alt.files.count())
        self.assertEqual(0, self.db.alt.chunks.count())

        self.assertRaises(NoFile, self.alt.get, "foo")
        oid = self.alt.put("hello world", _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual("hello world", self.alt.get("foo").read())

        self.alt.put("", filename="mike")
        self.alt.put("foo", filename="test")
        self.alt.put("", filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]),
                         set(self.alt.list()))

    def test_threaded_reads(self):
        self.fs.put("hello", _id="test")

        threads = []
        for i in range(10):
            threads.append(JustRead(self.fs))
            threads[i].start()

        for i in range(10):
            threads[i].join()

    def test_threaded_writes(self):
        threads = []
        for i in range(10):
            threads.append(JustWrite(self.fs))
            threads[i].start()

        for i in range(10):
            threads[i].join()

        f = self.fs.get_last_version("test")
        self.assertEqual(f.read(), "hello")

    def test_get_last_version(self):
        a = self.fs.put("foo", filename="test")
        time.sleep(0.01)
        b = self.fs.new_file(filename="test")
        b.write("bar")
        b.close()
        time.sleep(0.01)
        b = b._id
        c = self.fs.put("baz", filename="test")

        self.assertEqual("baz", self.fs.get_last_version("test").read())
        self.fs.delete(c)
        self.assertEqual("bar", self.fs.get_last_version("test").read())
        self.fs.delete(b)
        self.assertEqual("foo", self.fs.get_last_version("test").read())
        self.fs.delete(a)
        self.assertRaises(NoFile, self.fs.get_last_version, "test")

    def test_put_filelike(self):
        oid = self.fs.put(StringIO("hello world"), chunk_size=1)
        self.assertEqual(11, self.db.fs.chunks.count())
        self.assertEqual("hello world", self.fs.get(oid).read())


if __name__ == "__main__":
    unittest.main()
