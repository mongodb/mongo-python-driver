# -*- coding: utf-8 -*-
#
# Copyright 2009-present MongoDB, Inc.
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
import sys
import threading
import time
from io import BytesIO

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import joinall, one, rs_client, rs_or_single_client, single_client

import gridfs
from bson.binary import Binary
from gridfs.errors import CorruptGridFile, FileExists, NoFile
from gridfs.grid_file import GridOutCursor
from pymongo.database import Database
from pymongo.errors import (
    ConfigurationError,
    NotPrimaryError,
    ServerSelectionTimeoutError,
)
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference


class JustWrite(threading.Thread):
    def __init__(self, fs, n):
        threading.Thread.__init__(self)
        self.fs = fs
        self.n = n
        self.setDaemon(True)

    def run(self):
        for _ in range(self.n):
            file = self.fs.new_file(filename="test")
            file.write(b"hello")
            file.close()


class JustRead(threading.Thread):
    def __init__(self, fs, n, results):
        threading.Thread.__init__(self)
        self.fs = fs
        self.n = n
        self.results = results
        self.setDaemon(True)

    def run(self):
        for _ in range(self.n):
            file = self.fs.get("test")
            data = file.read()
            self.results.append(data)
            assert data == b"hello"


class TestGridfsNoConnect(unittest.TestCase):
    db: Database

    @classmethod
    def setUpClass(cls):
        cls.db = MongoClient(connect=False).pymongo_test

    def test_gridfs(self):
        self.assertRaises(TypeError, gridfs.GridFS, "foo")
        self.assertRaises(TypeError, gridfs.GridFS, self.db, 5)


class TestGridfs(IntegrationTest):
    fs: gridfs.GridFS
    alt: gridfs.GridFS

    @classmethod
    def setUpClass(cls):
        super(TestGridfs, cls).setUpClass()
        cls.fs = gridfs.GridFS(cls.db)
        cls.alt = gridfs.GridFS(cls.db, "alt")

    def setUp(self):
        self.cleanup_colls(
            self.db.fs.files, self.db.fs.chunks, self.db.alt.files, self.db.alt.chunks
        )

    def test_basic(self):
        oid = self.fs.put(b"hello world")
        self.assertEqual(b"hello world", self.fs.get(oid).read())
        self.assertEqual(1, self.db.fs.files.count_documents({}))
        self.assertEqual(1, self.db.fs.chunks.count_documents({}))

        self.fs.delete(oid)
        self.assertRaises(NoFile, self.fs.get, oid)
        self.assertEqual(0, self.db.fs.files.count_documents({}))
        self.assertEqual(0, self.db.fs.chunks.count_documents({}))

        self.assertRaises(NoFile, self.fs.get, "foo")
        oid = self.fs.put(b"hello world", _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual(b"hello world", self.fs.get("foo").read())

    def test_multi_chunk_delete(self):
        self.db.fs.drop()
        self.assertEqual(0, self.db.fs.files.count_documents({}))
        self.assertEqual(0, self.db.fs.chunks.count_documents({}))
        gfs = gridfs.GridFS(self.db)
        oid = gfs.put(b"hello", chunkSize=1)
        self.assertEqual(1, self.db.fs.files.count_documents({}))
        self.assertEqual(5, self.db.fs.chunks.count_documents({}))
        gfs.delete(oid)
        self.assertEqual(0, self.db.fs.files.count_documents({}))
        self.assertEqual(0, self.db.fs.chunks.count_documents({}))

    def test_list(self):
        self.assertEqual([], self.fs.list())
        self.fs.put(b"hello world")
        self.assertEqual([], self.fs.list())

        # PYTHON-598: in server versions before 2.5.x, creating an index on
        # filename, uploadDate causes list() to include None.
        self.fs.get_last_version()
        self.assertEqual([], self.fs.list())

        self.fs.put(b"", filename="mike")
        self.fs.put(b"foo", filename="test")
        self.fs.put(b"", filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]), set(self.fs.list()))

    def test_empty_file(self):
        oid = self.fs.put(b"")
        self.assertEqual(b"", self.fs.get(oid).read())
        self.assertEqual(1, self.db.fs.files.count_documents({}))
        self.assertEqual(0, self.db.fs.chunks.count_documents({}))

        raw = self.db.fs.files.find_one()
        assert raw is not None
        self.assertEqual(0, raw["length"])
        self.assertEqual(oid, raw["_id"])
        self.assertTrue(isinstance(raw["uploadDate"], datetime.datetime))
        self.assertEqual(255 * 1024, raw["chunkSize"])
        self.assertNotIn("md5", raw)

    def test_corrupt_chunk(self):
        files_id = self.fs.put(b"foobar")
        self.db.fs.chunks.update_one({"files_id": files_id}, {"$set": {"data": Binary(b"foo", 0)}})
        try:
            out = self.fs.get(files_id)
            self.assertRaises(CorruptGridFile, out.read)

            out = self.fs.get(files_id)
            self.assertRaises(CorruptGridFile, out.readline)
        finally:
            self.fs.delete(files_id)

    def test_put_ensures_index(self):
        chunks = self.db.fs.chunks
        files = self.db.fs.files
        # Ensure the collections are removed.
        chunks.drop()
        files.drop()
        self.fs.put(b"junk")

        self.assertTrue(
            any(
                info.get("key") == [("files_id", 1), ("n", 1)]
                for info in chunks.index_information().values()
            )
        )
        self.assertTrue(
            any(
                info.get("key") == [("filename", 1), ("uploadDate", 1)]
                for info in files.index_information().values()
            )
        )

    def test_alt_collection(self):
        oid = self.alt.put(b"hello world")
        self.assertEqual(b"hello world", self.alt.get(oid).read())
        self.assertEqual(1, self.db.alt.files.count_documents({}))
        self.assertEqual(1, self.db.alt.chunks.count_documents({}))

        self.alt.delete(oid)
        self.assertRaises(NoFile, self.alt.get, oid)
        self.assertEqual(0, self.db.alt.files.count_documents({}))
        self.assertEqual(0, self.db.alt.chunks.count_documents({}))

        self.assertRaises(NoFile, self.alt.get, "foo")
        oid = self.alt.put(b"hello world", _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual(b"hello world", self.alt.get("foo").read())

        self.alt.put(b"", filename="mike")
        self.alt.put(b"foo", filename="test")
        self.alt.put(b"", filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]), set(self.alt.list()))

    def test_threaded_reads(self):
        self.fs.put(b"hello", _id="test")

        threads = []
        results: list = []
        for i in range(10):
            threads.append(JustRead(self.fs, 10, results))
            threads[i].start()

        joinall(threads)

        self.assertEqual(100 * [b"hello"], results)

    def test_threaded_writes(self):
        threads = []
        for i in range(10):
            threads.append(JustWrite(self.fs, 10))
            threads[i].start()

        joinall(threads)

        f = self.fs.get_last_version("test")
        self.assertEqual(f.read(), b"hello")

        # Should have created 100 versions of 'test' file
        self.assertEqual(100, self.db.fs.files.count_documents({"filename": "test"}))

    def test_get_last_version(self):
        one = self.fs.put(b"foo", filename="test")
        time.sleep(0.01)
        two = self.fs.new_file(filename="test")
        two.write(b"bar")
        two.close()
        time.sleep(0.01)
        two = two._id
        three = self.fs.put(b"baz", filename="test")

        self.assertEqual(b"baz", self.fs.get_last_version("test").read())
        self.fs.delete(three)
        self.assertEqual(b"bar", self.fs.get_last_version("test").read())
        self.fs.delete(two)
        self.assertEqual(b"foo", self.fs.get_last_version("test").read())
        self.fs.delete(one)
        self.assertRaises(NoFile, self.fs.get_last_version, "test")

    def test_get_last_version_with_metadata(self):
        one = self.fs.put(b"foo", filename="test", author="author")
        time.sleep(0.01)
        two = self.fs.put(b"bar", filename="test", author="author")

        self.assertEqual(b"bar", self.fs.get_last_version(author="author").read())
        self.fs.delete(two)
        self.assertEqual(b"foo", self.fs.get_last_version(author="author").read())
        self.fs.delete(one)

        one = self.fs.put(b"foo", filename="test", author="author1")
        time.sleep(0.01)
        two = self.fs.put(b"bar", filename="test", author="author2")

        self.assertEqual(b"foo", self.fs.get_last_version(author="author1").read())
        self.assertEqual(b"bar", self.fs.get_last_version(author="author2").read())
        self.assertEqual(b"bar", self.fs.get_last_version(filename="test").read())

        self.assertRaises(NoFile, self.fs.get_last_version, author="author3")
        self.assertRaises(NoFile, self.fs.get_last_version, filename="nottest", author="author1")

        self.fs.delete(one)
        self.fs.delete(two)

    def test_get_version(self):
        self.fs.put(b"foo", filename="test")
        time.sleep(0.01)
        self.fs.put(b"bar", filename="test")
        time.sleep(0.01)
        self.fs.put(b"baz", filename="test")
        time.sleep(0.01)

        self.assertEqual(b"foo", self.fs.get_version("test", 0).read())
        self.assertEqual(b"bar", self.fs.get_version("test", 1).read())
        self.assertEqual(b"baz", self.fs.get_version("test", 2).read())

        self.assertEqual(b"baz", self.fs.get_version("test", -1).read())
        self.assertEqual(b"bar", self.fs.get_version("test", -2).read())
        self.assertEqual(b"foo", self.fs.get_version("test", -3).read())

        self.assertRaises(NoFile, self.fs.get_version, "test", 3)
        self.assertRaises(NoFile, self.fs.get_version, "test", -4)

    def test_get_version_with_metadata(self):
        one = self.fs.put(b"foo", filename="test", author="author1")
        time.sleep(0.01)
        two = self.fs.put(b"bar", filename="test", author="author1")
        time.sleep(0.01)
        three = self.fs.put(b"baz", filename="test", author="author2")

        self.assertEqual(
            b"foo", self.fs.get_version(filename="test", author="author1", version=-2).read()
        )
        self.assertEqual(
            b"bar", self.fs.get_version(filename="test", author="author1", version=-1).read()
        )
        self.assertEqual(
            b"foo", self.fs.get_version(filename="test", author="author1", version=0).read()
        )
        self.assertEqual(
            b"bar", self.fs.get_version(filename="test", author="author1", version=1).read()
        )
        self.assertEqual(
            b"baz", self.fs.get_version(filename="test", author="author2", version=0).read()
        )
        self.assertEqual(b"baz", self.fs.get_version(filename="test", version=-1).read())
        self.assertEqual(b"baz", self.fs.get_version(filename="test", version=2).read())

        self.assertRaises(NoFile, self.fs.get_version, filename="test", author="author3")
        self.assertRaises(NoFile, self.fs.get_version, filename="test", author="author1", version=2)

        self.fs.delete(one)
        self.fs.delete(two)
        self.fs.delete(three)

    def test_put_filelike(self):
        oid = self.fs.put(BytesIO(b"hello world"), chunk_size=1)
        self.assertEqual(11, self.db.fs.chunks.count_documents({}))
        self.assertEqual(b"hello world", self.fs.get(oid).read())

    def test_file_exists(self):
        oid = self.fs.put(b"hello")
        self.assertRaises(FileExists, self.fs.put, b"world", _id=oid)

        one = self.fs.new_file(_id=123)
        one.write(b"some content")
        one.close()

        two = self.fs.new_file(_id=123)
        self.assertRaises(FileExists, two.write, b"x" * 262146)

    def test_exists(self):
        oid = self.fs.put(b"hello")
        self.assertTrue(self.fs.exists(oid))
        self.assertTrue(self.fs.exists({"_id": oid}))
        self.assertTrue(self.fs.exists(_id=oid))

        self.assertFalse(self.fs.exists(filename="mike"))
        self.assertFalse(self.fs.exists("mike"))

        oid = self.fs.put(b"hello", filename="mike", foo=12)
        self.assertTrue(self.fs.exists(oid))
        self.assertTrue(self.fs.exists({"_id": oid}))
        self.assertTrue(self.fs.exists(_id=oid))
        self.assertTrue(self.fs.exists(filename="mike"))
        self.assertTrue(self.fs.exists({"filename": "mike"}))
        self.assertTrue(self.fs.exists(foo=12))
        self.assertTrue(self.fs.exists({"foo": 12}))
        self.assertTrue(self.fs.exists(foo={"$gt": 11}))
        self.assertTrue(self.fs.exists({"foo": {"$gt": 11}}))

        self.assertFalse(self.fs.exists(foo=13))
        self.assertFalse(self.fs.exists({"foo": 13}))
        self.assertFalse(self.fs.exists(foo={"$gt": 12}))
        self.assertFalse(self.fs.exists({"foo": {"$gt": 12}}))

    def test_put_unicode(self):
        self.assertRaises(TypeError, self.fs.put, "hello")

        oid = self.fs.put("hello", encoding="utf-8")
        self.assertEqual(b"hello", self.fs.get(oid).read())
        self.assertEqual("utf-8", self.fs.get(oid).encoding)

        oid = self.fs.put("aé", encoding="iso-8859-1")
        self.assertEqual("aé".encode("iso-8859-1"), self.fs.get(oid).read())
        self.assertEqual("iso-8859-1", self.fs.get(oid).encoding)

    def test_missing_length_iter(self):
        # Test fix that guards against PHP-237
        self.fs.put(b"", filename="empty")
        doc = self.db.fs.files.find_one({"filename": "empty"})
        assert doc is not None
        doc.pop("length")
        self.db.fs.files.replace_one({"_id": doc["_id"]}, doc)
        f = self.fs.get_last_version(filename="empty")

        def iterate_file(grid_file):
            for chunk in grid_file:
                pass
            return True

        self.assertTrue(iterate_file(f))

    def test_gridfs_lazy_connect(self):
        client = MongoClient("badhost", connect=False, serverSelectionTimeoutMS=10)
        db = client.db
        gfs = gridfs.GridFS(db)
        self.assertRaises(ServerSelectionTimeoutError, gfs.list)

        fs = gridfs.GridFS(db)
        f = fs.new_file()
        self.assertRaises(ServerSelectionTimeoutError, f.close)

    def test_gridfs_find(self):
        self.fs.put(b"test2", filename="two")
        time.sleep(0.01)
        self.fs.put(b"test2+", filename="two")
        time.sleep(0.01)
        self.fs.put(b"test1", filename="one")
        time.sleep(0.01)
        self.fs.put(b"test2++", filename="two")
        files = self.db.fs.files
        self.assertEqual(3, files.count_documents({"filename": "two"}))
        self.assertEqual(4, files.count_documents({}))
        cursor = self.fs.find(no_cursor_timeout=False).sort("uploadDate", -1).skip(1).limit(2)
        gout = next(cursor)
        self.assertEqual(b"test1", gout.read())
        cursor.rewind()
        gout = next(cursor)
        self.assertEqual(b"test1", gout.read())
        gout = next(cursor)
        self.assertEqual(b"test2+", gout.read())
        self.assertRaises(StopIteration, cursor.__next__)
        cursor.close()
        self.assertRaises(TypeError, self.fs.find, {}, {"_id": True})

    def test_delete_not_initialized(self):
        # Creating a cursor with invalid arguments will not run __init__
        # but will still call __del__.
        cursor = GridOutCursor.__new__(GridOutCursor)  # Skip calling __init__
        with self.assertRaises(TypeError):
            cursor.__init__(self.db.fs.files, {}, {"_id": True})  # type: ignore
        cursor.__del__()  # no error

    def test_gridfs_find_one(self):
        self.assertEqual(None, self.fs.find_one())

        id1 = self.fs.put(b"test1", filename="file1")
        res = self.fs.find_one()
        assert res is not None
        self.assertEqual(b"test1", res.read())

        id2 = self.fs.put(b"test2", filename="file2", meta="data")
        res1 = self.fs.find_one(id1)
        assert res1 is not None
        self.assertEqual(b"test1", res1.read())
        res2 = self.fs.find_one(id2)
        assert res2 is not None
        self.assertEqual(b"test2", res2.read())

        res3 = self.fs.find_one({"filename": "file1"})
        assert res3 is not None
        self.assertEqual(b"test1", res3.read())

        res4 = self.fs.find_one(id2)
        assert res4 is not None
        self.assertEqual("data", res4.meta)

    def test_grid_in_non_int_chunksize(self):
        # Lua, and perhaps other buggy GridFS clients, store size as a float.
        data = b"data"
        self.fs.put(data, filename="f")
        self.db.fs.files.update_one({"filename": "f"}, {"$set": {"chunkSize": 100.0}})

        self.assertEqual(data, self.fs.get_version("f").read())

    def test_unacknowledged(self):
        # w=0 is prohibited.
        with self.assertRaises(ConfigurationError):
            gridfs.GridFS(rs_or_single_client(w=0).pymongo_test)

    def test_md5(self):
        gin = self.fs.new_file()
        gin.write(b"no md5 sum")
        gin.close()
        self.assertIsNone(gin.md5)

        gout = self.fs.get(gin._id)
        self.assertIsNone(gout.md5)

        _id = self.fs.put(b"still no md5 sum")
        gout = self.fs.get(_id)
        self.assertIsNone(gout.md5)


class TestGridfsReplicaSet(IntegrationTest):
    @classmethod
    @client_context.require_secondaries_count(1)
    def setUpClass(cls):
        super(TestGridfsReplicaSet, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        client_context.client.drop_database("gfsreplica")

    def test_gridfs_replica_set(self):
        rsc = rs_client(w=client_context.w, read_preference=ReadPreference.SECONDARY)

        fs = gridfs.GridFS(rsc.gfsreplica, "gfsreplicatest")

        gin = fs.new_file()
        self.assertEqual(gin._coll.read_preference, ReadPreference.PRIMARY)

        oid = fs.put(b"foo")
        content = fs.get(oid).read()
        self.assertEqual(b"foo", content)

    def test_gridfs_secondary(self):
        secondary_host, secondary_port = one(self.client.secondaries)
        secondary_connection = single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY
        )

        # Should detect it's connected to secondary and not attempt to
        # create index
        fs = gridfs.GridFS(secondary_connection.gfsreplica, "gfssecondarytest")

        # This won't detect secondary, raises error
        self.assertRaises(NotPrimaryError, fs.put, b"foo")

    def test_gridfs_secondary_lazy(self):
        # Should detect it's connected to secondary and not attempt to
        # create index.
        secondary_host, secondary_port = one(self.client.secondaries)
        client = single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY, connect=False
        )

        # Still no connection.
        fs = gridfs.GridFS(client.gfsreplica, "gfssecondarylazytest")

        # Connects, doesn't create index.
        self.assertRaises(NoFile, fs.get_last_version)
        self.assertRaises(NotPrimaryError, fs.put, "data", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
