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

"""Tests for the gridfs package."""
from __future__ import annotations

import asyncio
import datetime
import itertools
import sys
import threading
import time
from io import BytesIO
from test.asynchronous.helpers import ConcurrentRunner
from unittest.mock import patch

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.asynchronous.utils import async_joinall
from test.utils_shared import one

import gridfs
from bson.binary import Binary
from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.son import SON
from gridfs.errors import CorruptGridFile, NoFile
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.errors import (
    ConfigurationError,
    NotPrimaryError,
    ServerSelectionTimeoutError,
    WriteConcernError,
)
from pymongo.read_preferences import ReadPreference

_IS_SYNC = False


class JustWrite(ConcurrentRunner):
    def __init__(self, gfs, num):
        super().__init__()
        self.gfs = gfs
        self.num = num
        self.daemon = True

    async def run(self):
        for _ in range(self.num):
            file = self.gfs.open_upload_stream("test")
            await file.write(b"hello")
            await file.close()


class JustRead(ConcurrentRunner):
    def __init__(self, gfs, num, results):
        super().__init__()
        self.gfs = gfs
        self.num = num
        self.results = results
        self.daemon = True

    async def run(self):
        for _ in range(self.num):
            file = await self.gfs.open_download_stream_by_name("test")
            data = await file.read()
            self.results.append(data)
            assert data == b"hello"


class TestGridfs(AsyncIntegrationTest):
    fs: gridfs.AsyncGridFSBucket
    alt: gridfs.AsyncGridFSBucket

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fs = gridfs.AsyncGridFSBucket(self.db)
        self.alt = gridfs.AsyncGridFSBucket(self.db, bucket_name="alt")
        await self.cleanup_colls(
            self.db.fs.files, self.db.fs.chunks, self.db.alt.files, self.db.alt.chunks
        )

    async def test_basic(self):
        oid = await self.fs.upload_from_stream("test_filename", b"hello world")
        self.assertEqual(b"hello world", await (await self.fs.open_download_stream(oid)).read())
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(1, await self.db.fs.chunks.count_documents({}))

        await self.fs.delete(oid)
        with self.assertRaises(NoFile):
            await self.fs.open_download_stream(oid)
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))

    async def test_multi_chunk_delete(self):
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))
        gfs = gridfs.AsyncGridFSBucket(self.db)
        oid = await gfs.upload_from_stream("test_filename", b"hello", chunk_size_bytes=1)
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(5, await self.db.fs.chunks.count_documents({}))
        await gfs.delete(oid)
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))

    async def test_delete_by_name(self):
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))
        gfs = gridfs.AsyncGridFSBucket(self.db)
        await gfs.upload_from_stream("test_filename", b"hello", chunk_size_bytes=1)
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(5, await self.db.fs.chunks.count_documents({}))
        await gfs.delete_by_name("test_filename")
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))

    async def test_empty_file(self):
        oid = await self.fs.upload_from_stream("test_filename", b"")
        self.assertEqual(b"", await (await self.fs.open_download_stream(oid)).read())
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))

        raw = await self.db.fs.files.find_one()
        assert raw is not None
        self.assertEqual(0, raw["length"])
        self.assertEqual(oid, raw["_id"])
        self.assertIsInstance(raw["uploadDate"], datetime.datetime)
        self.assertEqual(255 * 1024, raw["chunkSize"])
        self.assertNotIn("md5", raw)

    async def test_corrupt_chunk(self):
        files_id = await self.fs.upload_from_stream("test_filename", b"foobar")
        await self.db.fs.chunks.update_one(
            {"files_id": files_id}, {"$set": {"data": Binary(b"foo", 0)}}
        )
        try:
            out = await self.fs.open_download_stream(files_id)
            with self.assertRaises(CorruptGridFile):
                await out.read()

            out = await self.fs.open_download_stream(files_id)
            with self.assertRaises(CorruptGridFile):
                await out.readline()
        finally:
            await self.fs.delete(files_id)

    async def test_upload_ensures_index(self):
        chunks = self.db.fs.chunks
        files = self.db.fs.files
        # Ensure the collections are removed.
        await chunks.drop()
        await files.drop()
        await self.fs.upload_from_stream("filename", b"junk")

        self.assertIn(
            [("files_id", 1), ("n", 1)],
            [info.get("key") for info in (await chunks.index_information()).values()],
            "Missing required index on chunks collection: {files_id: 1, n: 1}",
        )

        self.assertIn(
            [("filename", 1), ("uploadDate", 1)],
            [info.get("key") for info in (await files.index_information()).values()],
            "Missing required index on files collection: {filename: 1, uploadDate: 1}",
        )

    async def test_ensure_index_shell_compat(self):
        files = self.db.fs.files
        for i, j in itertools.combinations_with_replacement([1, 1.0, Int64(1)], 2):
            # Create the index with different numeric types (as might be done
            # from the mongo shell).
            shell_index = [("filename", i), ("uploadDate", j)]
            await self.db.command(
                "createIndexes",
                files.name,
                indexes=[{"key": SON(shell_index), "name": "filename_1.0_uploadDate_1.0"}],
            )

            # No error.
            await self.fs.upload_from_stream("filename", b"data")

            self.assertIn(
                [("filename", 1), ("uploadDate", 1)],
                [info.get("key") for info in (await files.index_information()).values()],
                "Missing required index on files collection: {filename: 1, uploadDate: 1}",
            )
            await files.drop()

    async def test_alt_collection(self):
        oid = await self.alt.upload_from_stream("test_filename", b"hello world")
        self.assertEqual(b"hello world", await (await self.alt.open_download_stream(oid)).read())
        self.assertEqual(1, await self.db.alt.files.count_documents({}))
        self.assertEqual(1, await self.db.alt.chunks.count_documents({}))

        await self.alt.delete(oid)
        with self.assertRaises(NoFile):
            await self.alt.open_download_stream(oid)
        self.assertEqual(0, await self.db.alt.files.count_documents({}))
        self.assertEqual(0, await self.db.alt.chunks.count_documents({}))

        with self.assertRaises(NoFile):
            await self.alt.open_download_stream("foo")
        await self.alt.upload_from_stream("foo", b"hello world")
        self.assertEqual(
            b"hello world", await (await self.alt.open_download_stream_by_name("foo")).read()
        )

        await self.alt.upload_from_stream("mike", b"")
        await self.alt.upload_from_stream("test", b"foo")
        await self.alt.upload_from_stream("hello world", b"")

        self.assertEqual(
            {"mike", "test", "hello world", "foo"},
            {k["filename"] for k in await self.db.alt.files.find().to_list()},
        )

    async def test_threaded_reads(self):
        await self.fs.upload_from_stream("test", b"hello")

        threads = []
        results: list = []
        for i in range(10):
            threads.append(JustRead(self.fs, 10, results))
            await threads[i].start()

        await async_joinall(threads)

        self.assertEqual(100 * [b"hello"], results)

    async def test_threaded_writes(self):
        threads = []
        for i in range(10):
            threads.append(JustWrite(self.fs, 10))
            await threads[i].start()

        await async_joinall(threads)

        fstr = await self.fs.open_download_stream_by_name("test")
        self.assertEqual(await fstr.read(), b"hello")

        # Should have created 100 versions of 'test' file
        self.assertEqual(100, await self.db.fs.files.count_documents({"filename": "test"}))

    async def test_get_last_version(self):
        one = await self.fs.upload_from_stream("test", b"foo")
        await asyncio.sleep(0.01)
        two = self.fs.open_upload_stream("test")
        await two.write(b"bar")
        await two.close()
        await asyncio.sleep(0.01)
        two = two._id
        three = await self.fs.upload_from_stream("test", b"baz")

        self.assertEqual(b"baz", await (await self.fs.open_download_stream_by_name("test")).read())
        await self.fs.delete(three)
        self.assertEqual(b"bar", await (await self.fs.open_download_stream_by_name("test")).read())
        await self.fs.delete(two)
        self.assertEqual(b"foo", await (await self.fs.open_download_stream_by_name("test")).read())
        await self.fs.delete(one)
        with self.assertRaises(NoFile):
            await self.fs.open_download_stream_by_name("test")

    async def test_get_version(self):
        await self.fs.upload_from_stream("test", b"foo")
        await asyncio.sleep(0.01)
        await self.fs.upload_from_stream("test", b"bar")
        await asyncio.sleep(0.01)
        await self.fs.upload_from_stream("test", b"baz")
        await asyncio.sleep(0.01)

        self.assertEqual(
            b"foo", await (await self.fs.open_download_stream_by_name("test", revision=0)).read()
        )
        self.assertEqual(
            b"bar", await (await self.fs.open_download_stream_by_name("test", revision=1)).read()
        )
        self.assertEqual(
            b"baz", await (await self.fs.open_download_stream_by_name("test", revision=2)).read()
        )

        self.assertEqual(
            b"baz", await (await self.fs.open_download_stream_by_name("test", revision=-1)).read()
        )
        self.assertEqual(
            b"bar", await (await self.fs.open_download_stream_by_name("test", revision=-2)).read()
        )
        self.assertEqual(
            b"foo", await (await self.fs.open_download_stream_by_name("test", revision=-3)).read()
        )

        with self.assertRaises(NoFile):
            await self.fs.open_download_stream_by_name("test", revision=3)
        with self.assertRaises(NoFile):
            await self.fs.open_download_stream_by_name("test", revision=-4)

    async def test_upload_from_stream(self):
        oid = await self.fs.upload_from_stream(
            "test_file", BytesIO(b"hello world"), chunk_size_bytes=1
        )
        self.assertEqual(11, await self.db.fs.chunks.count_documents({}))
        self.assertEqual(b"hello world", await (await self.fs.open_download_stream(oid)).read())

    async def test_upload_from_stream_with_id(self):
        oid = ObjectId()
        await self.fs.upload_from_stream_with_id(
            oid, "test_file_custom_id", BytesIO(b"custom id"), chunk_size_bytes=1
        )
        self.assertEqual(b"custom id", await (await self.fs.open_download_stream(oid)).read())

    @patch("gridfs.asynchronous.grid_file._UPLOAD_BUFFER_CHUNKS", 3)
    @async_client_context.require_failCommand_fail_point
    async def test_upload_bulk_write_error(self):
        # Test BulkWriteError from insert_many is converted to an insert_one style error.
        expected_wce = {
            "code": 100,
            "codeName": "UnsatisfiableWriteConcern",
            "errmsg": "Not enough data-bearing nodes",
        }
        cause_wce = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {"failCommands": ["insert"], "writeConcernError": expected_wce},
        }
        gin = self.fs.open_upload_stream("test_file", chunk_size_bytes=1)
        async with self.fail_point(cause_wce):
            # Assert we raise WriteConcernError, not BulkWriteError.
            with self.assertRaises(WriteConcernError):
                await gin.write(b"hello world")
        # 3 chunks were uploaded.
        self.assertEqual(3, await self.db.fs.chunks.count_documents({"files_id": gin._id}))
        await gin.abort()

    @patch("gridfs.asynchronous.grid_file._UPLOAD_BUFFER_CHUNKS", 10)
    async def test_upload_batching(self):
        async with self.fs.open_upload_stream("test_file", chunk_size_bytes=1) as gin:
            await gin.write(b"s" * (10 - 1))
            # No chunks were uploaded yet.
            self.assertEqual(0, await self.db.fs.chunks.count_documents({"files_id": gin._id}))
            await gin.write(b"s")
            # All chunks were uploaded since we hit the _UPLOAD_BUFFER_CHUNKS limit.
            self.assertEqual(10, await self.db.fs.chunks.count_documents({"files_id": gin._id}))

    async def test_open_upload_stream(self):
        gin = self.fs.open_upload_stream("from_stream")
        await gin.write(b"from stream")
        await gin.close()
        self.assertEqual(b"from stream", await (await self.fs.open_download_stream(gin._id)).read())

    async def test_open_upload_stream_with_id(self):
        oid = ObjectId()
        gin = self.fs.open_upload_stream_with_id(oid, "from_stream_custom_id")
        await gin.write(b"from stream with custom id")
        await gin.close()
        self.assertEqual(
            b"from stream with custom id", await (await self.fs.open_download_stream(oid)).read()
        )

    async def test_missing_length_iter(self):
        # Test fix that guards against PHP-237
        await self.fs.upload_from_stream("empty", b"")
        doc = await self.db.fs.files.find_one({"filename": "empty"})
        assert doc is not None
        doc.pop("length")
        await self.db.fs.files.replace_one({"_id": doc["_id"]}, doc)
        fstr = await self.fs.open_download_stream_by_name("empty")

        async def iterate_file(grid_file):
            async for _ in grid_file:
                pass
            return True

        self.assertTrue(await iterate_file(fstr))

    async def test_gridfs_lazy_connect(self):
        client = await self.async_single_client(
            "badhost", connect=False, serverSelectionTimeoutMS=0
        )
        cdb = client.db
        gfs = gridfs.AsyncGridFSBucket(cdb)
        with self.assertRaises(ServerSelectionTimeoutError):
            await gfs.delete(0)

        gfs = gridfs.AsyncGridFSBucket(cdb)
        with self.assertRaises(ServerSelectionTimeoutError):
            await gfs.upload_from_stream("test", b"")  # Still no connection.

    async def test_gridfs_find(self):
        await self.fs.upload_from_stream("two", b"test2")
        await asyncio.sleep(0.01)
        await self.fs.upload_from_stream("two", b"test2+")
        await asyncio.sleep(0.01)
        await self.fs.upload_from_stream("one", b"test1")
        await asyncio.sleep(0.01)
        await self.fs.upload_from_stream("two", b"test2++")
        files = self.db.fs.files
        self.assertEqual(3, await files.count_documents({"filename": "two"}))
        self.assertEqual(4, await files.count_documents({}))
        cursor = self.fs.find(
            {}, no_cursor_timeout=False, sort=[("uploadDate", -1)], skip=1, limit=2
        )
        gout = await cursor.next()
        self.assertEqual(b"test1", await gout.read())
        await cursor.rewind()
        gout = await cursor.next()
        self.assertEqual(b"test1", await gout.read())
        gout = await cursor.next()
        self.assertEqual(b"test2+", await gout.read())
        with self.assertRaises(StopAsyncIteration):
            await cursor.next()
        await cursor.close()
        self.assertRaises(TypeError, self.fs.find, {}, {"_id": True})

    async def test_grid_in_non_int_chunksize(self):
        # Lua, and perhaps other buggy AsyncGridFS clients, store size as a float.
        data = b"data"
        await self.fs.upload_from_stream("f", data)
        await self.db.fs.files.update_one({"filename": "f"}, {"$set": {"chunkSize": 100.0}})

        self.assertEqual(data, await (await self.fs.open_download_stream_by_name("f")).read())

    async def test_unacknowledged(self):
        # w=0 is prohibited.
        with self.assertRaises(ConfigurationError):
            gridfs.AsyncGridFSBucket((await self.async_rs_or_single_client(w=0)).pymongo_test)

    async def test_rename(self):
        _id = await self.fs.upload_from_stream("first_name", b"testing")
        self.assertEqual(
            b"testing", await (await self.fs.open_download_stream_by_name("first_name")).read()
        )

        await self.fs.rename(_id, "second_name")
        with self.assertRaises(NoFile):
            await self.fs.open_download_stream_by_name("first_name")
        self.assertEqual(
            b"testing", await (await self.fs.open_download_stream_by_name("second_name")).read()
        )

    async def test_rename_by_name(self):
        _id = await self.fs.upload_from_stream("first_name", b"testing")
        self.assertEqual(
            b"testing", await (await self.fs.open_download_stream_by_name("first_name")).read()
        )

        await self.fs.rename_by_name("first_name", "second_name")
        with self.assertRaises(NoFile):
            await self.fs.open_download_stream_by_name("first_name")
        self.assertEqual(
            b"testing", await (await self.fs.open_download_stream_by_name("second_name")).read()
        )

    @patch("gridfs.asynchronous.grid_file._UPLOAD_BUFFER_SIZE", 5)
    async def test_abort(self):
        gin = self.fs.open_upload_stream("test_filename", chunk_size_bytes=5)
        await gin.write(b"test1")
        await gin.write(b"test2")
        await gin.write(b"test3")
        self.assertEqual(3, await self.db.fs.chunks.count_documents({"files_id": gin._id}))
        await gin.abort()
        self.assertTrue(gin.closed)
        with self.assertRaises(ValueError):
            await gin.write(b"test4")
        self.assertEqual(0, await self.db.fs.chunks.count_documents({"files_id": gin._id}))

    async def test_download_to_stream(self):
        file1 = BytesIO(b"hello world")
        # Test with one chunk.
        oid = await self.fs.upload_from_stream("one_chunk", file1)
        self.assertEqual(1, await self.db.fs.chunks.count_documents({}))
        file2 = BytesIO()
        await self.fs.download_to_stream(oid, file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

        # Test with many chunks.
        await self.db.drop_collection("fs.files")
        await self.db.drop_collection("fs.chunks")
        file1.seek(0)
        oid = await self.fs.upload_from_stream("many_chunks", file1, chunk_size_bytes=1)
        self.assertEqual(11, await self.db.fs.chunks.count_documents({}))
        file2 = BytesIO()
        await self.fs.download_to_stream(oid, file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

    async def test_download_to_stream_by_name(self):
        file1 = BytesIO(b"hello world")
        # Test with one chunk.
        _ = await self.fs.upload_from_stream("one_chunk", file1)
        self.assertEqual(1, await self.db.fs.chunks.count_documents({}))
        file2 = BytesIO()
        await self.fs.download_to_stream_by_name("one_chunk", file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

        # Test with many chunks.
        await self.db.drop_collection("fs.files")
        await self.db.drop_collection("fs.chunks")
        file1.seek(0)
        await self.fs.upload_from_stream("many_chunks", file1, chunk_size_bytes=1)
        self.assertEqual(11, await self.db.fs.chunks.count_documents({}))

        file2 = BytesIO()
        await self.fs.download_to_stream_by_name("many_chunks", file2)
        file1.seek(0)
        file2.seek(0)
        self.assertEqual(file1.read(), file2.read())

    async def test_md5(self):
        gin = self.fs.open_upload_stream("no md5")
        await gin.write(b"no md5 sum")
        await gin.close()
        self.assertIsNone(gin.md5)

        gout = await self.fs.open_download_stream(gin._id)
        self.assertIsNone(gout.md5)

        gin = self.fs.open_upload_stream_with_id(ObjectId(), "also no md5")
        await gin.write(b"also no md5 sum")
        await gin.close()
        self.assertIsNone(gin.md5)

        gout = await self.fs.open_download_stream(gin._id)
        self.assertIsNone(gout.md5)


class TestGridfsBucketReplicaSet(AsyncIntegrationTest):
    @async_client_context.require_secondaries_count(1)
    async def asyncSetUp(self):
        await super().asyncSetUp()

    @classmethod
    @async_client_context.require_connection
    async def asyncTearDownClass(cls):
        await async_client_context.client.drop_database("gfsbucketreplica")

    async def test_gridfs_replica_set(self):
        rsc = await self.async_rs_client(
            w=async_client_context.w, read_preference=ReadPreference.SECONDARY
        )

        gfs = gridfs.AsyncGridFSBucket(rsc.gfsbucketreplica, "gfsbucketreplicatest")
        oid = await gfs.upload_from_stream("test_filename", b"foo")
        content = await (await gfs.open_download_stream(oid)).read()
        self.assertEqual(b"foo", content)

    async def test_gridfs_secondary(self):
        secondary_host, secondary_port = one(await self.client.secondaries)
        secondary_connection = await self.async_single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY
        )

        # Should detect it's connected to secondary and not attempt to
        # create index
        gfs = gridfs.AsyncGridFSBucket(
            secondary_connection.gfsbucketreplica, "gfsbucketsecondarytest"
        )

        # This won't detect secondary, raises error
        with self.assertRaises(NotPrimaryError):
            await gfs.upload_from_stream("test_filename", b"foo")

    async def test_gridfs_secondary_lazy(self):
        # Should detect it's connected to secondary and not attempt to
        # create index.
        secondary_host, secondary_port = one(await self.client.secondaries)
        client = await self.async_single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY, connect=False
        )

        # Still no connection.
        gfs = gridfs.AsyncGridFSBucket(client.gfsbucketreplica, "gfsbucketsecondarylazytest")

        # Connects, doesn't create index.
        with self.assertRaises(NoFile):
            await gfs.open_download_stream_by_name("test_filename")
        with self.assertRaises(NotPrimaryError):
            await gfs.upload_from_stream("test_filename", b"data")


if __name__ == "__main__":
    unittest.main()
