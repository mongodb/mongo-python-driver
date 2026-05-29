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

"""Tests for the gridfs package."""
from __future__ import annotations

import asyncio
import datetime
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
from gridfs.asynchronous.grid_file import DEFAULT_CHUNK_SIZE, AsyncGridOutCursor
from gridfs.errors import CorruptGridFile, FileExists, NoFile
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.errors import (
    ConfigurationError,
    NotPrimaryError,
    ServerSelectionTimeoutError,
)
from pymongo.read_preferences import ReadPreference

_IS_SYNC = False


class JustWrite(ConcurrentRunner):
    def __init__(self, fs, n):
        super().__init__()
        self.fs = fs
        self.n = n
        self.daemon = True

    async def run(self):
        for _ in range(self.n):
            file = self.fs.new_file(filename="test")
            await file.write(b"hello")
            await file.close()


class JustRead(ConcurrentRunner):
    def __init__(self, fs, n, results):
        super().__init__()
        self.fs = fs
        self.n = n
        self.results = results
        self.daemon = True

    async def run(self):
        for _ in range(self.n):
            file = await self.fs.get("test")
            data = await file.read()
            self.results.append(data)
            assert data == b"hello"


class TestGridfsNoConnect(unittest.IsolatedAsyncioTestCase):
    db: AsyncDatabase

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.db = AsyncMongoClient(connect=False).pymongo_test

    async def test_gridfs(self):
        self.assertRaises(TypeError, gridfs.AsyncGridFS, "foo")
        self.assertRaises(TypeError, gridfs.AsyncGridFS, self.db, 5)


class TestGridfs(AsyncIntegrationTest):
    fs: gridfs.AsyncGridFS
    alt: gridfs.AsyncGridFS

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fs = gridfs.AsyncGridFS(self.db)
        self.alt = gridfs.AsyncGridFS(self.db, "alt")
        await self.cleanup_colls(
            self.db.fs.files, self.db.fs.chunks, self.db.alt.files, self.db.alt.chunks
        )

    async def test_basic(self):
        oid = await self.fs.put(b"hello world")
        self.assertEqual(b"hello world", await (await self.fs.get(oid)).read())
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(1, await self.db.fs.chunks.count_documents({}))

        await self.fs.delete(oid)
        with self.assertRaises(NoFile):
            await self.fs.get(oid)
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))

        with self.assertRaises(NoFile):
            await self.fs.get("foo")
        oid = await self.fs.put(b"hello world", _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual(b"hello world", await (await self.fs.get("foo")).read())

    async def test_multi_chunk_delete(self):
        await self.db.fs.drop()
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))
        gfs = gridfs.AsyncGridFS(self.db)
        oid = await gfs.put(b"hello", chunkSize=1)
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(5, await self.db.fs.chunks.count_documents({}))
        await gfs.delete(oid)
        self.assertEqual(0, await self.db.fs.files.count_documents({}))
        self.assertEqual(0, await self.db.fs.chunks.count_documents({}))

    async def test_list(self):
        self.assertEqual([], await self.fs.list())
        await self.fs.put(b"hello world")
        self.assertEqual([], await self.fs.list())

        # PYTHON-598: in server versions before 2.5.x, creating an index on
        # filename, uploadDate causes list() to include None.
        await self.fs.get_last_version()
        self.assertEqual([], await self.fs.list())

        await self.fs.put(b"", filename="mike")
        await self.fs.put(b"foo", filename="test")
        await self.fs.put(b"", filename="hello world")

        self.assertEqual({"mike", "test", "hello world"}, set(await self.fs.list()))

    async def test_empty_file(self):
        oid = await self.fs.put(b"")
        self.assertEqual(b"", await (await self.fs.get(oid)).read())
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
        files_id = await self.fs.put(b"foobar")
        await self.db.fs.chunks.update_one(
            {"files_id": files_id}, {"$set": {"data": Binary(b"foo", 0)}}
        )
        try:
            out = await self.fs.get(files_id)
            with self.assertRaises(CorruptGridFile):
                await out.read()

            out = await self.fs.get(files_id)
            with self.assertRaises(CorruptGridFile):
                await out.readline()
        finally:
            await self.fs.delete(files_id)

    async def test_put_ensures_index(self):
        chunks = self.db.fs.chunks
        files = self.db.fs.files
        # Ensure the collections are removed.
        await chunks.drop()
        await files.drop()
        await self.fs.put(b"junk")

        self.assertTrue(
            any(
                info.get("key") == [("files_id", 1), ("n", 1)]
                for info in (await chunks.index_information()).values()
            )
        )
        self.assertTrue(
            any(
                info.get("key") == [("filename", 1), ("uploadDate", 1)]
                for info in (await files.index_information()).values()
            )
        )

    async def test_alt_collection(self):
        oid = await self.alt.put(b"hello world")
        self.assertEqual(b"hello world", await (await self.alt.get(oid)).read())
        self.assertEqual(1, await self.db.alt.files.count_documents({}))
        self.assertEqual(1, await self.db.alt.chunks.count_documents({}))

        await self.alt.delete(oid)
        with self.assertRaises(NoFile):
            await self.alt.get(oid)
        self.assertEqual(0, await self.db.alt.files.count_documents({}))
        self.assertEqual(0, await self.db.alt.chunks.count_documents({}))

        with self.assertRaises(NoFile):
            await self.alt.get("foo")
        oid = await self.alt.put(b"hello world", _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual(b"hello world", await (await self.alt.get("foo")).read())

        await self.alt.put(b"", filename="mike")
        await self.alt.put(b"foo", filename="test")
        await self.alt.put(b"", filename="hello world")

        self.assertEqual({"mike", "test", "hello world"}, set(await self.alt.list()))

    async def test_threaded_reads(self):
        await self.fs.put(b"hello", _id="test")

        tasks = []
        results: list = []
        for i in range(10):
            tasks.append(JustRead(self.fs, 10, results))
            await tasks[i].start()

        await async_joinall(tasks)

        self.assertEqual(100 * [b"hello"], results)

    async def test_threaded_writes(self):
        tasks = []
        for i in range(10):
            tasks.append(JustWrite(self.fs, 10))
            await tasks[i].start()

        await async_joinall(tasks)

        f = await self.fs.get_last_version("test")
        self.assertEqual(await f.read(), b"hello")

        # Should have created 100 versions of 'test' file
        self.assertEqual(100, await self.db.fs.files.count_documents({"filename": "test"}))

    async def test_get_last_version(self):
        one = await self.fs.put(b"foo", filename="test")
        await asyncio.sleep(0.01)
        two = self.fs.new_file(filename="test")
        await two.write(b"bar")
        await two.close()
        await asyncio.sleep(0.01)
        two = two._id
        three = await self.fs.put(b"baz", filename="test")

        self.assertEqual(b"baz", await (await self.fs.get_last_version("test")).read())
        await self.fs.delete(three)
        self.assertEqual(b"bar", await (await self.fs.get_last_version("test")).read())
        await self.fs.delete(two)
        self.assertEqual(b"foo", await (await self.fs.get_last_version("test")).read())
        await self.fs.delete(one)
        with self.assertRaises(NoFile):
            await self.fs.get_last_version("test")

    async def test_get_last_version_with_metadata(self):
        one = await self.fs.put(b"foo", filename="test", author="author")
        await asyncio.sleep(0.01)
        two = await self.fs.put(b"bar", filename="test", author="author")

        self.assertEqual(b"bar", await (await self.fs.get_last_version(author="author")).read())
        await self.fs.delete(two)
        self.assertEqual(b"foo", await (await self.fs.get_last_version(author="author")).read())
        await self.fs.delete(one)

        one = await self.fs.put(b"foo", filename="test", author="author1")
        await asyncio.sleep(0.01)
        two = await self.fs.put(b"bar", filename="test", author="author2")

        self.assertEqual(b"foo", await (await self.fs.get_last_version(author="author1")).read())
        self.assertEqual(b"bar", await (await self.fs.get_last_version(author="author2")).read())
        self.assertEqual(b"bar", await (await self.fs.get_last_version(filename="test")).read())

        with self.assertRaises(NoFile):
            await self.fs.get_last_version(author="author3")
        with self.assertRaises(NoFile):
            await self.fs.get_last_version(filename="nottest", author="author1")

        await self.fs.delete(one)
        await self.fs.delete(two)

    async def test_get_version(self):
        await self.fs.put(b"foo", filename="test")
        await asyncio.sleep(0.01)
        await self.fs.put(b"bar", filename="test")
        await asyncio.sleep(0.01)
        await self.fs.put(b"baz", filename="test")
        await asyncio.sleep(0.01)

        self.assertEqual(b"foo", await (await self.fs.get_version("test", 0)).read())
        self.assertEqual(b"bar", await (await self.fs.get_version("test", 1)).read())
        self.assertEqual(b"baz", await (await self.fs.get_version("test", 2)).read())

        self.assertEqual(b"baz", await (await self.fs.get_version("test", -1)).read())
        self.assertEqual(b"bar", await (await self.fs.get_version("test", -2)).read())
        self.assertEqual(b"foo", await (await self.fs.get_version("test", -3)).read())

        with self.assertRaises(NoFile):
            await self.fs.get_version("test", 3)
        with self.assertRaises(NoFile):
            await self.fs.get_version("test", -4)

    async def test_get_version_with_metadata(self):
        one = await self.fs.put(b"foo", filename="test", author="author1")
        await asyncio.sleep(0.01)
        two = await self.fs.put(b"bar", filename="test", author="author1")
        await asyncio.sleep(0.01)
        three = await self.fs.put(b"baz", filename="test", author="author2")

        self.assertEqual(
            b"foo",
            await (await self.fs.get_version(filename="test", author="author1", version=-2)).read(),
        )
        self.assertEqual(
            b"bar",
            await (await self.fs.get_version(filename="test", author="author1", version=-1)).read(),
        )
        self.assertEqual(
            b"foo",
            await (await self.fs.get_version(filename="test", author="author1", version=0)).read(),
        )
        self.assertEqual(
            b"bar",
            await (await self.fs.get_version(filename="test", author="author1", version=1)).read(),
        )
        self.assertEqual(
            b"baz",
            await (await self.fs.get_version(filename="test", author="author2", version=0)).read(),
        )
        self.assertEqual(
            b"baz", await (await self.fs.get_version(filename="test", version=-1)).read()
        )
        self.assertEqual(
            b"baz", await (await self.fs.get_version(filename="test", version=2)).read()
        )

        with self.assertRaises(NoFile):
            await self.fs.get_version(filename="test", author="author3")
        with self.assertRaises(NoFile):
            await self.fs.get_version(filename="test", author="author1", version=2)

        await self.fs.delete(one)
        await self.fs.delete(two)
        await self.fs.delete(three)

    async def test_put_filelike(self):
        oid = await self.fs.put(BytesIO(b"hello world"), chunk_size=1)
        self.assertEqual(11, await self.db.fs.chunks.count_documents({}))
        self.assertEqual(b"hello world", await (await self.fs.get(oid)).read())

    async def test_file_exists(self):
        oid = await self.fs.put(b"hello")
        with self.assertRaises(FileExists):
            await self.fs.put(b"world", _id=oid)

        one = self.fs.new_file(_id=123)
        await one.write(b"some content")
        await one.close()

        # Attempt to upload a file with more chunks to the same _id.
        with patch("gridfs.asynchronous.grid_file._UPLOAD_BUFFER_SIZE", DEFAULT_CHUNK_SIZE):
            two = self.fs.new_file(_id=123)
            with self.assertRaises(FileExists):
                await two.write(b"x" * DEFAULT_CHUNK_SIZE * 3)
        # Original file is still readable (no extra chunks were uploaded).
        self.assertEqual(await (await self.fs.get(123)).read(), b"some content")

        two = self.fs.new_file(_id=123)
        await two.write(b"some content")
        with self.assertRaises(FileExists):
            await two.close()
        # Original file is still readable.
        self.assertEqual(await (await self.fs.get(123)).read(), b"some content")

    async def test_exists(self):
        oid = await self.fs.put(b"hello")
        self.assertTrue(await self.fs.exists(oid))
        self.assertTrue(await self.fs.exists({"_id": oid}))
        self.assertTrue(await self.fs.exists(_id=oid))

        self.assertFalse(await self.fs.exists(filename="mike"))
        self.assertFalse(await self.fs.exists("mike"))

        oid = await self.fs.put(b"hello", filename="mike", foo=12)
        self.assertTrue(await self.fs.exists(oid))
        self.assertTrue(await self.fs.exists({"_id": oid}))
        self.assertTrue(await self.fs.exists(_id=oid))
        self.assertTrue(await self.fs.exists(filename="mike"))
        self.assertTrue(await self.fs.exists({"filename": "mike"}))
        self.assertTrue(await self.fs.exists(foo=12))
        self.assertTrue(await self.fs.exists({"foo": 12}))
        self.assertTrue(await self.fs.exists(foo={"$gt": 11}))
        self.assertTrue(await self.fs.exists({"foo": {"$gt": 11}}))

        self.assertFalse(await self.fs.exists(foo=13))
        self.assertFalse(await self.fs.exists({"foo": 13}))
        self.assertFalse(await self.fs.exists(foo={"$gt": 12}))
        self.assertFalse(await self.fs.exists({"foo": {"$gt": 12}}))

    async def test_put_unicode(self):
        with self.assertRaises(TypeError):
            await self.fs.put("hello")

        oid = await self.fs.put("hello", encoding="utf-8")
        self.assertEqual(b"hello", await (await self.fs.get(oid)).read())
        self.assertEqual("utf-8", (await self.fs.get(oid)).encoding)

        oid = await self.fs.put("aé", encoding="iso-8859-1")
        self.assertEqual("aé".encode("iso-8859-1"), await (await self.fs.get(oid)).read())
        self.assertEqual("iso-8859-1", (await self.fs.get(oid)).encoding)

    async def test_missing_length_iter(self):
        # Test fix that guards against PHP-237
        await self.fs.put(b"", filename="empty")
        doc = await self.db.fs.files.find_one({"filename": "empty"})
        assert doc is not None
        doc.pop("length")
        await self.db.fs.files.replace_one({"_id": doc["_id"]}, doc)
        f = await self.fs.get_last_version(filename="empty")

        async def iterate_file(grid_file):
            async for _chunk in grid_file:
                pass
            return True

        self.assertTrue(await iterate_file(f))

    async def test_gridfs_lazy_connect(self):
        client = await self.async_single_client(
            "badhost", connect=False, serverSelectionTimeoutMS=10
        )
        db = client.db
        gfs = gridfs.AsyncGridFS(db)
        with self.assertRaises(ServerSelectionTimeoutError):
            await gfs.list()

        fs = gridfs.AsyncGridFS(db)
        f = fs.new_file()
        with self.assertRaises(ServerSelectionTimeoutError):
            await f.close()

    async def test_gridfs_find(self):
        await self.fs.put(b"test2", filename="two")
        await asyncio.sleep(0.01)
        await self.fs.put(b"test2+", filename="two")
        await asyncio.sleep(0.01)
        await self.fs.put(b"test1", filename="one")
        await asyncio.sleep(0.01)
        await self.fs.put(b"test2++", filename="two")
        files = self.db.fs.files
        self.assertEqual(3, await files.count_documents({"filename": "two"}))
        self.assertEqual(4, await files.count_documents({}))
        cursor = self.fs.find(no_cursor_timeout=False).sort("uploadDate", -1).skip(1).limit(2)
        gout = await cursor.next()
        self.assertEqual(b"test1", await gout.read())
        await cursor.rewind()
        gout = await cursor.next()
        self.assertEqual(b"test1", await gout.read())
        gout = await cursor.next()
        self.assertEqual(b"test2+", await gout.read())
        with self.assertRaises(StopAsyncIteration):
            await cursor.__anext__()
        await cursor.rewind()
        items = await cursor.to_list()
        self.assertEqual(len(items), 2)
        await cursor.rewind()
        items = await cursor.to_list(1)
        self.assertEqual(len(items), 1)
        await cursor.close()
        self.assertRaises(TypeError, self.fs.find, {}, {"_id": True})

    async def test_delete_not_initialized(self):
        # Creating a cursor with invalid arguments will not run __init__
        # but will still call __del__.
        cursor = AsyncGridOutCursor.__new__(AsyncGridOutCursor)  # Skip calling __init__
        with self.assertRaises(TypeError):
            cursor.__init__(self.db.fs.files, {}, {"_id": True})  # type: ignore
        cursor.__del__()  # no error

    async def test_gridfs_find_one(self):
        self.assertEqual(None, await self.fs.find_one())

        id1 = await self.fs.put(b"test1", filename="file1")
        res = await self.fs.find_one()
        assert res is not None
        self.assertEqual(b"test1", await res.read())

        id2 = await self.fs.put(b"test2", filename="file2", meta="data")
        res1 = await self.fs.find_one(id1)
        assert res1 is not None
        self.assertEqual(b"test1", await res1.read())
        res2 = await self.fs.find_one(id2)
        assert res2 is not None
        self.assertEqual(b"test2", await res2.read())

        res3 = await self.fs.find_one({"filename": "file1"})
        assert res3 is not None
        self.assertEqual(b"test1", await res3.read())

        res4 = await self.fs.find_one(id2)
        assert res4 is not None
        self.assertEqual("data", res4.meta)

    async def test_grid_in_non_int_chunksize(self):
        # Lua, and perhaps other buggy AsyncGridFS clients, store size as a float.
        data = b"data"
        await self.fs.put(data, filename="f")
        await self.db.fs.files.update_one({"filename": "f"}, {"$set": {"chunkSize": 100.0}})

        self.assertEqual(data, await (await self.fs.get_version("f")).read())

    async def test_unacknowledged(self):
        # w=0 is prohibited.
        with self.assertRaises(ConfigurationError):
            gridfs.AsyncGridFS((await self.async_rs_or_single_client(w=0)).pymongo_test)

    async def test_md5(self):
        gin = self.fs.new_file()
        await gin.write(b"no md5 sum")
        await gin.close()
        self.assertIsNone(gin.md5)

        gout = await self.fs.get(gin._id)
        self.assertIsNone(gout.md5)

        _id = await self.fs.put(b"still no md5 sum")
        gout = await self.fs.get(_id)
        self.assertIsNone(gout.md5)


class TestGridfsReplicaSet(AsyncIntegrationTest):
    @async_client_context.require_secondaries_count(1)
    async def asyncSetUp(self):
        await super().asyncSetUp()

    @classmethod
    @async_client_context.require_connection
    async def asyncTearDownClass(cls):
        await async_client_context.client.drop_database("gfsreplica")

    async def test_gridfs_replica_set(self):
        rsc = await self.async_rs_client(
            w=async_client_context.w, read_preference=ReadPreference.SECONDARY
        )

        fs = gridfs.AsyncGridFS(rsc.gfsreplica, "gfsreplicatest")

        gin = fs.new_file()
        self.assertEqual(gin._coll.read_preference, ReadPreference.PRIMARY)

        oid = await fs.put(b"foo")
        content = await (await fs.get(oid)).read()
        self.assertEqual(b"foo", content)

    async def test_gridfs_secondary(self):
        secondary_host, secondary_port = one(await self.client.secondaries)
        secondary_connection = await self.async_single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY
        )

        # Should detect it's connected to secondary and not attempt to
        # create index
        fs = gridfs.AsyncGridFS(secondary_connection.gfsreplica, "gfssecondarytest")

        # This won't detect secondary, raises error
        with self.assertRaises(NotPrimaryError):
            await fs.put(b"foo")

    async def test_gridfs_secondary_lazy(self):
        # Should detect it's connected to secondary and not attempt to
        # create index.
        secondary_host, secondary_port = one(await self.client.secondaries)
        client = await self.async_single_client(
            secondary_host, secondary_port, read_preference=ReadPreference.SECONDARY, connect=False
        )

        # Still no connection.
        fs = gridfs.AsyncGridFS(client.gfsreplica, "gfssecondarylazytest")

        # Connects, doesn't create index.
        with self.assertRaises(NoFile):
            await fs.get_last_version()
        with self.assertRaises(NotPrimaryError):
            await fs.put("data", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
