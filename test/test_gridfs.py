# -*- coding: utf-8 -*-
#
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
from gridfs.errors import (FileExists,
                           NoFile)
from test_connection import get_connection


class JustWrite(threading.Thread):

    def __init__(self, fs, n):
        threading.Thread.__init__(self)
        self.fs = fs
        self.n = n

    def run(self):
        for _ in range(self.n):
            file = self.fs.new_file(filename="test")
            file.write("hello")
            file.close()


class JustRead(threading.Thread):

    def __init__(self, fs, n, results):
        threading.Thread.__init__(self)
        self.fs = fs
        self.n = n
        self.results = results

    def run(self):
        for _ in range(self.n):
            file = self.fs.get("test")
            data = file.read()
            self.results.append(data)
            assert data == "hello"


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
        self.assertEqual(256 * 1024, raw["chunkSize"])
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
        results = []
        for i in range(10):
            threads.append(JustRead(self.fs, 10, results))
            threads[i].start()

        for i in range(10):
            threads[i].join()

        self.assertEqual(
            100 * ['hello'],
            results
        )

    def test_threaded_writes(self):
        threads = []
        for i in range(10):
            threads.append(JustWrite(self.fs, 10))
            threads[i].start()

        for i in range(10):
            threads[i].join()

        f = self.fs.get_last_version("test")
        self.assertEqual(f.read(), "hello")

        # Should have created 100 versions of 'test' file
        self.assertEqual(
            100,
            self.db.fs.files.find({'filename':'test'}).count()
        )

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

    def test_get_last_version_with_metadata(self):
        a = self.fs.put("foo", filename="test", author="author")
        time.sleep(0.01)
        b = self.fs.put("bar", filename="test", author="author")

        self.assertEqual("bar", self.fs.get_last_version(author="author").read())
        self.fs.delete(b)
        self.assertEqual("foo", self.fs.get_last_version(author="author").read())
        self.fs.delete(a)

        a = self.fs.put("foo", filename="test", author="author1")
        time.sleep(0.01)
        b = self.fs.put("bar", filename="test", author="author2")

        self.assertEqual("foo", self.fs.get_last_version(author="author1").read())
        self.assertEqual("bar", self.fs.get_last_version(author="author2").read())
        self.assertEqual("bar", self.fs.get_last_version(filename="test").read())

        self.assertRaises(NoFile, self.fs.get_last_version, author="author3")
        self.assertRaises(NoFile, self.fs.get_last_version, filename="nottest", author="author1")

        self.fs.delete(a)
        self.fs.delete(b)

    def test_get_version(self):
        self.fs.put("foo", filename="test")
        time.sleep(0.01)
        self.fs.put("bar", filename="test")
        time.sleep(0.01)
        self.fs.put("baz", filename="test")
        time.sleep(0.01)

        self.assertEqual("foo", self.fs.get_version("test", 0).read())
        self.assertEqual("bar", self.fs.get_version("test", 1).read())
        self.assertEqual("baz", self.fs.get_version("test", 2).read())

        self.assertEqual("baz", self.fs.get_version("test", -1).read())
        self.assertEqual("bar", self.fs.get_version("test", -2).read())
        self.assertEqual("foo", self.fs.get_version("test", -3).read())

        self.assertRaises(NoFile, self.fs.get_version, "test", 3)
        self.assertRaises(NoFile, self.fs.get_version, "test", -4)

    def test_get_version_with_metadata(self):
        a = self.fs.put("foo", filename="test", author="author1")
        time.sleep(0.01)
        b = self.fs.put("bar", filename="test", author="author1")
        time.sleep(0.01)
        c = self.fs.put("baz", filename="test", author="author2")

        self.assertEqual("foo", self.fs.get_version(filename="test", author="author1", version=-2).read())
        self.assertEqual("bar", self.fs.get_version(filename="test", author="author1", version=-1).read())
        self.assertEqual("foo", self.fs.get_version(filename="test", author="author1", version=0).read())
        self.assertEqual("bar", self.fs.get_version(filename="test", author="author1", version=1).read())
        self.assertEqual("baz", self.fs.get_version(filename="test", author="author2", version=0).read())
        self.assertEqual("baz", self.fs.get_version(filename="test", version=-1).read())
        self.assertEqual("baz", self.fs.get_version(filename="test", version=2).read())

        self.assertRaises(NoFile, self.fs.get_version, filename="test", author="author3")
        self.assertRaises(NoFile, self.fs.get_version, filename="test", author="author1", version=2)

        self.fs.delete(a)
        self.fs.delete(b)
        self.fs.delete(c)

    def test_put_filelike(self):
        oid = self.fs.put(StringIO("hello world"), chunk_size=1)
        self.assertEqual(11, self.db.fs.chunks.count())
        self.assertEqual("hello world", self.fs.get(oid).read())

    def test_put_duplicate(self):
        oid = self.fs.put("hello")
        self.assertRaises(FileExists, self.fs.put, "world", _id=oid)

    def test_exists(self):
        oid = self.fs.put("hello")
        self.assert_(self.fs.exists(oid))
        self.assert_(self.fs.exists({"_id": oid}))
        self.assert_(self.fs.exists(_id=oid))

        self.assertFalse(self.fs.exists(filename="mike"))
        self.assertFalse(self.fs.exists("mike"))

        oid = self.fs.put("hello", filename="mike", foo=12)
        self.assert_(self.fs.exists(oid))
        self.assert_(self.fs.exists({"_id": oid}))
        self.assert_(self.fs.exists(_id=oid))
        self.assert_(self.fs.exists(filename="mike"))
        self.assert_(self.fs.exists({"filename": "mike"}))
        self.assert_(self.fs.exists(foo=12))
        self.assert_(self.fs.exists({"foo": 12}))
        self.assert_(self.fs.exists(foo={"$gt": 11}))
        self.assert_(self.fs.exists({"foo": {"$gt": 11}}))

        self.assertFalse(self.fs.exists(foo=13))
        self.assertFalse(self.fs.exists({"foo": 13}))
        self.assertFalse(self.fs.exists(foo={"$gt": 12}))
        self.assertFalse(self.fs.exists({"foo": {"$gt": 12}}))

    def test_put_unicode(self):
        self.assertRaises(TypeError, self.fs.put, u"hello")

        oid = self.fs.put(u"hello", encoding="utf-8")
        self.assertEqual("hello", self.fs.get(oid).read())
        self.assertEqual("utf-8", self.fs.get(oid).encoding)

        oid = self.fs.put(u"aé", encoding="iso-8859-1")
        self.assertEqual(u"aé".encode("iso-8859-1"), self.fs.get(oid).read())
        self.assertEqual("iso-8859-1", self.fs.get(oid).encoding)

    def test_missing_length_iter(self):
        # Test fix that guards against PHP-237
        self.fs.put("", filename="empty")
        doc = self.db.fs.files.find_one({"filename": "empty"})
        doc.pop("length")
        self.db.fs.files.save(doc)
        f = self.fs.get_last_version(filename="empty")

        def iterate_file(grid_file):
            for chunk in grid_file:
                pass
            return True

        self.assertTrue(iterate_file(f))

    def test_request(self):
        c = self.db.connection
        c.start_request()
        n = 5
        for i in range(n):
            file = self.fs.new_file(filename="test")
            file.write("hello")
            file.close()

        c.end_request()

        self.assertEqual(
            n,
            self.db.fs.files.find({'filename':'test'}).count()
        )


if __name__ == "__main__":
    unittest.main()
