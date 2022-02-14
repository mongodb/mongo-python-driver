# -*- coding: utf-8 -*-
#
# Copyright 2015-present MongoDB, Inc.
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
import itertools
import threading
import time
from io import BytesIO
from test import IntegrationTest, client_context, unittest
from test.utils import joinall, one, rs_client, rs_or_single_client, single_client

import gridfs
from bson.binary import Binary
from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.son import SON
from gridfs.errors import CorruptGridFile, NoFile
from pymongo.errors import (
    ConfigurationError,
    NotPrimaryError,
    ServerSelectionTimeoutError,
)
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference


class JustWrite(threading.Thread):
    def __init__(self, gfs, num):
        threading.Thread.__init__(self)
        self.gfs = gfs
        self.num = num
        self.setDaemon(True)

    def run(self):
        for _ in range(self.num):
            file = self.gfs.open_upload_stream("test")
            file.write(b"hello")
            file.close()


class JustRead(threading.Thread):
    def __init__(self, gfs, num, results):
        threading.Thread.__init__(self)
        self.gfs = gfs
        self.num = num
        self.results = results
        self.setDaemon(True)

    def run(self):
        for _ in range(self.num):
            file = self.gfs.open_download_stream_by_name("test")
            data = file.read()
            self.results.append(data)
            assert data == b"hello"


class TestGridfs(IntegrationTest):
    fs: gridfs.GridFSBucket
    alt: gridfs.GridFSBucket

    @classmethod
    def setUpClass(cls):
        super(TestGridfs, cls).setUpClass()
        cls.fs = gridfs.GridFSBucket(cls.db)
        cls.alt = gridfs.GridFSBucket(cls.db, bucket_name="alt")

    def setUp(self):
        self.cleanup_colls(
            self.db.fs.files, self.db.fs.chunks, self.db.alt.files, self.db.alt.chunks
        )

    def test_basic(self):
        oid = self.fs.upload_from_stream("test_filename", b"hello world")
        self.assertEqual(b"hello world", self.fs.open_download_stream(oid).read())
        self.assertEqual(1, self.db.fs.files.count_documents({}))
        self.assertEqual(1, self.db.fs.chunks.count_documents({}))

        self.fs.delete(oid)
        self.assertRaises(NoFile, self.fs.open_download_stream, oid)
        self.assertEqual(0, self.db.fs.files.count_documents({}))
        self.assertEqual(0, self.db.fs.chunks.count_documents({}))

    def test_multi_chunk_delete(self):
        self.assertEqual(0, self.db.fs.files.count_documents({}))
        self.assertEqual(0, self.db.fs.chunks.count_documents({}))
        gfs = gridfs.GridFSBucket(self.db)
        oid = gfs.upload_from_stream("test_filename", b"hello", chunk_size_bytes=1)
        self.assertEqual(1, self.db.fs.files.count_documents({}))
        self.assertEqual(5, self.db.fs.chunks.count_documents({}))
        gfs.delete(oid)
        self.assertEqual(0, self.db.fs.files.count_documents({}))
        self.assertEqual(0, self.db.fs.chunks.count_documents({}))

    def test_empty_file(self):
        oid = self.fs.upload_from_stream("test_filename", b"")
        self.assertEqual(b"", self.fs.open_download_stream(oid).read())
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
        files_id = self.fs.upload_from_stream("test_filename", b"foobar")
        self.db.fs.chunks.update_one({"files_id": files_id}, {"$set": {"data": Binary(b"foo", 0)}})
        try:
            out = self.fs.open_download_stream(files_id)
            self.assertRaises(CorruptGridFile, out.read)

            out = self.fs.open_download_stream(files_id)
            self.assertRaises(CorruptGridFile, out.readline)
        finally:
            self.fs.delete(files_id)

    def test_upload_ensures_index(self):
        chunks = self.db.fs.chunks
        files = self.db.fs.files
        # Ensure the collections are removed.
        chunks.drop()
        files.drop()
        self.fs.upload_from_stream("filename", b"junk")

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

    def test_ensure_index_shell_compat(self):
        files = self.db.fs.files
        for i, j in itertools.combinations_with_replacement([1, 1.0, Int64(1)], 2):
            # Create the index with different numeric types (as might be done
            # from the mongo shell).
            shell_index = [("filename", i), ("uploadDate", j)]
            self.db.command(
                "createIndexes",
                files.name,
                indexes=[{"key": SON(shell_index), "name": "filename_1.0_uploadDate_1.0"}],
            )

            # No error.
            self.fs.upload_from_stream("filename", b"data")

            self.assertTrue(
                any(
                    info.get("key") == [("filename", 1), ("uploadDate", 1)]
                    for info in files.index_information().values()
                )
            )
            files.drop()

    def test_alt_collection(self):
        oid = self.alt.upload_from_stream("test_filename", b"hello world")
        self.assertEqual(b"hello world", self.alt.open_download_stream(oid).read())
        self.assertEqual(1, self.db.alt.files.count_documents({}))
        self.assertEqual(1, self.db.alt.chunks.count_documents({}))

        self.alt.delete(oid)
        self.assertRaises(NoFile, self.alt.open_download_stream, oid)
        self.assertEqual(0, self.db.alt.files.count_documents({}))
        self.assertEqual(0, self.db.alt.chunks.count_documents({}))

        self.assertRaises(NoFile, self.alt.open_download_stream, "foo")
        self.alt.upload_from_stream("foo", b"hello world")
        self.assertEqual(b"hello world", self.alt.open_download_stream_by_name("foo").read())

        self.alt.upload_from_stream("mike", b"")
        self.alt.upload_from_stream("test", b"foo")
        self.alt.upload_from_stream("hello world", b"")

        self.assertEqual(
            set(["mike", "test", "hello world", "foo"]),
            set(k["filename"] for k in list(self.db.alt.files.find())),
        )

    def test_threaded_reads(self):
        self.fs.upload_from_stream("test", b"hello")

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

        fstr = self.fs.open_download_stream_by_name("test")
        self.assertEqual(fstr.read(), b"hello")

        # Should have created 100 versions of 'test' file
        self.assertEqual(100, self.db.fs.files.count_documents({"filename": "test"}))

    def test_get_last_version(self):
        one = self.fs.upload_from_stream("test", b"foo")
        time.sleep(0.01)
        two = self.fs.open_upload_stream("test")
        two.write(b"bar")
        two.close()
        time.sleep(0.01)
        two = two._id
        three = self.fs.upload_from_stream("test", b"baz")

        self.assertEqual(b"baz", self.fs.open_download_stream_by_name("test").read())
        self.fs.delete(three)
        self.assertEqual(b"bar", self.fs.open_download_stream_by_name("test").read())
        self.fs.delete(two)
        self.assertEqual(b"foo", self.fs.open_download_stream_by_name("test").read())
        self.fs.delete(one)
        self.assertRaises(NoFile, self.fs.open_download_stream_by_name, "test")

    def test_get_version(self):
        self.fs.upload_from_stream("test", b"foo")
        time.sleep(0.01)
        self.fs.upload_from_stream("test", b"bar")
        time.sleep(0.01)
        self.fs.upload_from_stream("test", b"baz")
        time.sleep(0.01)

        self.assertEqual(b"foo", self.fs.open_download_stream_by_name("test", revision=0).read())
        self.assertEqual(b"bar", self.fs.open_download_stream_by_name("test", revision=1).read())
        self.assertEqual(b"baz", self.fs.open_download_stream_by_name("test", revision=2).read())

        self.assertEqual(b"baz", self.fs.open_download_stream_by_name("test", revision=-1).read())
        self.assertEqual(b"bar", self.fs.open_download_stream_by_name("test", revision=-2).read())
        self.assertEqual(b"foo", self.fs.open_download_stream_by_name("test", revision=-3).read())

        self.assertRaises(NoFile, self.fs.open_download_stream_by_name, "test", revision=3)
        self.assertRaises(NoFile, self.fs.open_download_stream_by_name, "test", revision=-4)

    def test_upload_from_stream(self):
        oid = self.fs.upload_from_stream("test_file", BytesIO(b"hello world"), chunk_size_bytes=1)
        self.assertEqual(11, self.db.fs.chunks.count_documents({}))
        self.assertEqual(b"hello world", self.fs.open_download_stream(oid).read())

    def test_upload_from_stream_with_id(self):
        oid = ObjectId()
        self.fs.upload_from_stream_with_id(
            oid, "test_file_custom_id", BytesIO(b"custom id"), chunk_size_bytes=1
        )
        self.assertEqual(b"custom id", self.fs.open_download_stream(oid).read())

    def test_open_upload_stream(self):
        gin = self.fs.open_upload_stream("from_stream")
        gin.write(b"from stream")
        gin.close()
        self.assertEqual(b"from stream", self.fs.open_download_stream(gin._id).read())

    def test_open_upload_stream_with_id(self):
        oid = ObjectId()
        gin = self.fs.open_upload_stream_with_id(oid, "from_stream_custom_id")
        gin.write(b"from stream with custom id")
        gin.close()
        self.assertEqual(b"from stream with custom id", self.fs.open_download_stream(oid).read())

    def test_missing_length_iter(self):
        # Test fix that guards against PHP-237
        self.fs.upload_from_stream("empty", b"")
        doc = self.db.fs.files.find_one({"filename": "empty"})
        assert doc is not None
        doc.pop("length")
        self.db.fs.files.replace_one({"_id": doc["_id"]}, doc)
        fstr = self.fs.open_download_stream_by_name("empty")

        def iterate_file(grid_file):
            for _ in grid_file:
                pass
            return True

        self.assertTrue(iterate_file(fstr))

    def test_gridfs_lazy_connect(self):
        client = MongoClient("badhost", connect=False, serverSelectionTimeoutMS=0)
        cdb = client.db
        gfs = gridfs.GridFSBucket(cdb)
        self.assertRaises(ServerSelectionTimeoutError, gfs.delete, 0)

        gfs = gridfs.GridFSBucket(cdb)
        self.assertRaises(
            ServerSelectionTimeoutError, gfs.upload_from_stream, "test", b""
        )  # Still no connection.

    def test_gridfs_find(self):
        self.fs.upload_from_stream("two", b"test2")
        time.sleep(0.01)
        self.fs.upload_from_stream("two", b"test2+")
        time.sleep(0.01)
        self.fs.upload_from_stream("one", b"test1")
        time.sleep(0.01)
        self.fs.upload_from_stream("two", b"test2++")
        files = self.db.fs.files
        self.assertEqual(3, files.count_documents({"filename": "two"}))
        self.assertEqual(4, files.count_documents({}))
        cursor = self.fs.find(
            {}, no_cursor_timeout=False, sort=[("uploadDate", -1)], skip=1, limit=2
        )
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

    def test_grid_in_non_int_chunksize(self):
        # Lua, and perhaps other buggy GridFS clients, store size as a float.
        data = b"data"
        self.fs.upload_from_stream("f", data)
        self.db.fs.files.update_one({"filename": "f"}, {"$set": {"chunkSize": 100.0}})

        self.assertEqual(data, self.fs.open_download_stream_by_name("f").read())

    def test_unacknowledged(self):
        # w=0 is prohibited.
        with self.assertRaises(ConfigurationError):
            gridfs.GridFSBucket(rs_or_single_client(w=0).pymongo_test)

    def test_rename(self):
        _id = self.fs.upload_from_stream("first_name", b"testing")
        self.assertEqual(b"testing", self.fs.open_download_stream_by_name("first_name").read())

        self.fs.rename(_id, "second_name")
        self.assertRaises(NoFile, self.fs.open_download_stream_by_name, "first_name")
        self.assertEqual(b"testing", self.fs.open_download_stream_by_name("second_name").read())

    def test_abort(self):
        gin = self.fs.open_upload_stream("test_filename", chunk_size_bytes=5)
        gin.write(b"test1")
        gin.write(b"test2")
        gin.write(b"test3")
        self.assertEqual(3, self.db.fs.chunks.count_documents({"files_id": gin._id}))
        gin.abort()
        self.assertTrue(gin.closed)
        self.assertRaises(ValueError, gin.write, b"test4")
        self.assertEqual(0, self.db.fs.chunks.count_documents({"files_id": gin._id}))

    def test_download_to_stream(self):
        file1 = BytesIO(b"hello world")
        # Test with one chunk.
        oid = self.fs.upload_from_stream("one_chunk", file1)
        self.assertEqual(1, self.db.fs.chunks.count_documents({}))
        file2 = BytesIO()
        self.fs.download_to_stream(oid, file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

        # Test with many chunks.
        self.db.drop_collection("fs.files")
        self.db.drop_collection("fs.chunks")
        file1.seek(0)
        oid = self.fs.upload_from_stream("many_chunks", file1, chunk_size_bytes=1)
        self.assertEqual(11, self.db.fs.chunks.count_documents({}))
        file2 = BytesIO()
        self.fs.download_to_stream(oid, file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

    def test_download_to_stream_by_name(self):
        file1 = BytesIO(b"hello world")
        # Test with one chunk.
        _ = self.fs.upload_from_stream("one_chunk", file1)
        self.assertEqual(1, self.db.fs.chunks.count_documents({}))
        file2 = BytesIO()
        self.fs.download_to_stream_by_name("one_chunk", file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

        # Test with many chunks.
        self.db.drop_collection("fs.files")
        self.db.drop_collection("fs.chunks")
        file1.seek(0)
        self.fs.upload_from_stream("many_chunks", file1, chunk_size_bytes=1)
        self.assertEqual(11, self.db.fs.chunks.count_documents({}))

        file2 = BytesIO()
        self.fs.download_to_stream_by_name("many_chunks", file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

    def test_md5(self):
        gin = self.fs.open_upload_stream("no md5")
        gin.write(b"no md5 sum")
        gin.close()
        self.assertIsNone(gin.md5)

        gout = self.fs.open_download_stream(gin._id)
        self.assertIsNone(gout.md5)

        gin = self.fs.open_upload_stream_with_id(ObjectId(), "also no md5")
        gin.write(b"also no md5 sum")
        gin.close()
        self.assertIsNone(gin.md5)

        gout = self.fs.open_download_stream(gin._id)
        self.assertIsNone(gout.md5)


class TestGridfsBucketReplicaSet(IntegrationTest):
    @classmethod
    @client_context.require_secondaries_count(1)
    def setUpClass(cls):
        super(TestGridfsBucketReplicaSet, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        client_context.client.drop_database("gfsbucketreplica")

    def test_gridfs_replica_set(self):
        rsc = rs_client(w=client_context.w, read_preference=ReadPreference.SECONDARY)

        gfs = gridfs.GridFSBucket(rsc.gfsbucketreplica, "gfsbucketreplicatest")
        oid = gfs.upload_from_stream("test_filename", b"foo")
        content = gfs.open_download_stream(oid).read()
        self.assertEqual(b"foo", content)

    def test_gridfs_secondary(self):
        secondary_host, secondary_port = one(self.client.secondaries)
        secondary_connection = single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY
        )

        # Should detect it's connected to secondary and not attempt to
        # create index
        gfs = gridfs.GridFSBucket(secondary_connection.gfsbucketreplica, "gfsbucketsecondarytest")

        # This won't detect secondary, raises error
        self.assertRaises(NotPrimaryError, gfs.upload_from_stream, "test_filename", b"foo")

    def test_gridfs_secondary_lazy(self):
        # Should detect it's connected to secondary and not attempt to
        # create index.
        secondary_host, secondary_port = one(self.client.secondaries)
        client = single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY, connect=False
        )

        # Still no connection.
        gfs = gridfs.GridFSBucket(client.gfsbucketreplica, "gfsbucketsecondarylazytest")

        # Connects, doesn't create index.
        self.assertRaises(NoFile, gfs.open_download_stream_by_name, "test_filename")
        self.assertRaises(NotPrimaryError, gfs.upload_from_stream, "test_filename", b"data")


if __name__ == "__main__":
    unittest.main()
