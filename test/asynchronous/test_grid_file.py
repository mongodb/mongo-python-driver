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

"""Tests for the grid_file module."""
from __future__ import annotations

import datetime
import io
import sys
import zipfile
from io import BytesIO
from test.asynchronous import (
    AsyncIntegrationTest,
    AsyncUnitTest,
    async_client_context,
    qcheck,
    unittest,
)

from pymongo.asynchronous.database import AsyncDatabase

sys.path[0:0] = [""]

from test.utils_shared import OvertCommandListener

from bson.objectid import ObjectId
from gridfs.asynchronous.grid_file import (
    _SEEK_CUR,
    _SEEK_END,
    DEFAULT_CHUNK_SIZE,
    AsyncGridFS,
    AsyncGridIn,
    AsyncGridOut,
    AsyncGridOutCursor,
)
from gridfs.errors import NoFile
from pymongo import AsyncMongoClient
from pymongo.asynchronous.helpers import aiter, anext
from pymongo.errors import ConfigurationError, ServerSelectionTimeoutError
from pymongo.message import _CursorAddress

_IS_SYNC = False


class AsyncTestGridFileNoConnect(AsyncUnitTest):
    """Test GridFile features on a client that does not connect."""

    db: AsyncDatabase

    @classmethod
    def setUpClass(cls):
        cls.db = AsyncMongoClient(connect=False).pymongo_test

    def test_grid_in_custom_opts(self):
        self.assertRaises(TypeError, AsyncGridIn, "foo")

        a = AsyncGridIn(
            self.db.fs,
            _id=5,
            filename="my_file",
            contentType="text/html",
            chunkSize=1000,
            aliases=["foo"],
            metadata={"foo": 1, "bar": 2},
            bar=3,
            baz="hello",
        )

        self.assertEqual(5, a._id)
        self.assertEqual("my_file", a.filename)
        self.assertEqual("my_file", a.name)
        self.assertEqual("text/html", a.content_type)
        self.assertEqual(1000, a.chunk_size)
        self.assertEqual(["foo"], a.aliases)
        self.assertEqual({"foo": 1, "bar": 2}, a.metadata)
        self.assertEqual(3, a.bar)
        self.assertEqual("hello", a.baz)
        self.assertRaises(AttributeError, getattr, a, "mike")

        b = AsyncGridIn(self.db.fs, content_type="text/html", chunk_size=1000, baz=100)
        self.assertEqual("text/html", b.content_type)
        self.assertEqual(1000, b.chunk_size)
        self.assertEqual(100, b.baz)


class AsyncTestGridFile(AsyncIntegrationTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.cleanup_colls(self.db.fs.files, self.db.fs.chunks)

    async def test_basic(self):
        f = AsyncGridIn(self.db.fs, filename="test")
        await f.write(b"hello world")
        await f.close()
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(1, await self.db.fs.chunks.count_documents({}))

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"hello world", await g.read())

        # make sure it's still there...
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"hello world", await g.read())

        f = AsyncGridIn(self.db.fs, filename="test")
        await f.close()
        self.assertEqual(2, await self.db.fs.files.count_documents({}))
        self.assertEqual(1, await self.db.fs.chunks.count_documents({}))

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"", await g.read())

        # test that reading 0 returns proper type
        self.assertEqual(b"", await g.read(0))

    async def test_md5(self):
        f = AsyncGridIn(self.db.fs)
        await f.write(b"hello world\n")
        await f.close()
        self.assertEqual(None, f.md5)

    async def test_alternate_collection(self):
        await self.db.alt.files.delete_many({})
        await self.db.alt.chunks.delete_many({})

        f = AsyncGridIn(self.db.alt)
        await f.write(b"hello world")
        await f.close()

        self.assertEqual(1, await self.db.alt.files.count_documents({}))
        self.assertEqual(1, await self.db.alt.chunks.count_documents({}))

        g = AsyncGridOut(self.db.alt, f._id)
        self.assertEqual(b"hello world", await g.read())

    async def test_grid_in_default_opts(self):
        self.assertRaises(TypeError, AsyncGridIn, "foo")

        a = AsyncGridIn(self.db.fs)

        self.assertIsInstance(a._id, ObjectId)
        self.assertRaises(AttributeError, setattr, a, "_id", 5)

        self.assertEqual(None, a.filename)
        self.assertEqual(None, a.name)
        a.filename = "my_file"
        self.assertEqual("my_file", a.filename)
        self.assertEqual("my_file", a.name)

        self.assertEqual(None, a.content_type)
        a.content_type = "text/html"

        self.assertEqual("text/html", a.content_type)

        self.assertRaises(AttributeError, getattr, a, "length")
        self.assertRaises(AttributeError, setattr, a, "length", 5)

        self.assertEqual(255 * 1024, a.chunk_size)
        self.assertRaises(AttributeError, setattr, a, "chunk_size", 5)

        self.assertRaises(AttributeError, getattr, a, "upload_date")
        self.assertRaises(AttributeError, setattr, a, "upload_date", 5)

        self.assertRaises(AttributeError, getattr, a, "aliases")
        a.aliases = ["foo"]

        self.assertEqual(["foo"], a.aliases)

        self.assertRaises(AttributeError, getattr, a, "metadata")
        a.metadata = {"foo": 1}

        self.assertEqual({"foo": 1}, a.metadata)

        self.assertRaises(AttributeError, setattr, a, "md5", 5)

        await a.close()

        if _IS_SYNC:
            a.forty_two = 42
        else:
            self.assertRaises(AttributeError, setattr, a, "forty_two", 42)
            await a.set("forty_two", 42)

        self.assertEqual(42, a.forty_two)

        self.assertIsInstance(a._id, ObjectId)
        self.assertRaises(AttributeError, setattr, a, "_id", 5)

        self.assertEqual("my_file", a.filename)
        self.assertEqual("my_file", a.name)

        self.assertEqual("text/html", a.content_type)

        self.assertEqual(0, a.length)
        self.assertRaises(AttributeError, setattr, a, "length", 5)

        self.assertEqual(255 * 1024, a.chunk_size)
        self.assertRaises(AttributeError, setattr, a, "chunk_size", 5)

        self.assertIsInstance(a.upload_date, datetime.datetime)
        self.assertRaises(AttributeError, setattr, a, "upload_date", 5)

        self.assertEqual(["foo"], a.aliases)

        self.assertEqual({"foo": 1}, a.metadata)

        self.assertEqual(None, a.md5)
        self.assertRaises(AttributeError, setattr, a, "md5", 5)

        # Make sure custom attributes that were set both before and after
        # a.close() are reflected in b. PYTHON-411.
        b = await AsyncGridFS(self.db).get_last_version(filename=a.filename)
        self.assertEqual(a.metadata, b.metadata)
        self.assertEqual(a.aliases, b.aliases)
        self.assertEqual(a.forty_two, b.forty_two)

    async def test_grid_out_default_opts(self):
        self.assertRaises(TypeError, AsyncGridOut, "foo")

        gout = AsyncGridOut(self.db.fs, 5)
        with self.assertRaises(NoFile):
            if not _IS_SYNC:
                await gout.open()
            gout.name

        a = AsyncGridIn(self.db.fs)
        await a.close()

        b = AsyncGridOut(self.db.fs, a._id)
        if not _IS_SYNC:
            await b.open()

        self.assertEqual(a._id, b._id)
        self.assertEqual(0, b.length)
        self.assertEqual(None, b.content_type)
        self.assertEqual(None, b.name)
        self.assertEqual(None, b.filename)
        self.assertEqual(255 * 1024, b.chunk_size)
        self.assertIsInstance(b.upload_date, datetime.datetime)
        self.assertEqual(None, b.aliases)
        self.assertEqual(None, b.metadata)
        self.assertEqual(None, b.md5)

        for attr in [
            "_id",
            "name",
            "content_type",
            "length",
            "chunk_size",
            "upload_date",
            "aliases",
            "metadata",
            "md5",
        ]:
            self.assertRaises(AttributeError, setattr, b, attr, 5)

    async def test_grid_out_cursor_options(self):
        self.assertRaises(
            TypeError, AsyncGridOutCursor.__init__, self.db.fs, {}, projection={"filename": 1}
        )

        cursor = AsyncGridOutCursor(self.db.fs, {})
        cursor_clone = cursor.clone()

        cursor_dict = cursor.__dict__.copy()
        cursor_dict.pop("_session")
        cursor_clone_dict = cursor_clone.__dict__.copy()
        cursor_clone_dict.pop("_session")
        self.assertDictEqual(cursor_dict, cursor_clone_dict)

        self.assertRaises(NotImplementedError, cursor.add_option, 0)
        self.assertRaises(NotImplementedError, cursor.remove_option, 0)

    async def test_grid_out_custom_opts(self):
        one = AsyncGridIn(
            self.db.fs,
            _id=5,
            filename="my_file",
            contentType="text/html",
            chunkSize=1000,
            aliases=["foo"],
            metadata={"foo": 1, "bar": 2},
            bar=3,
            baz="hello",
        )
        await one.write(b"hello world")
        await one.close()

        two = AsyncGridOut(self.db.fs, 5)

        if not _IS_SYNC:
            await two.open()

        self.assertEqual("my_file", two.name)
        self.assertEqual("my_file", two.filename)
        self.assertEqual(5, two._id)
        self.assertEqual(11, two.length)
        self.assertEqual("text/html", two.content_type)
        self.assertEqual(1000, two.chunk_size)
        self.assertIsInstance(two.upload_date, datetime.datetime)
        self.assertEqual(["foo"], two.aliases)
        self.assertEqual({"foo": 1, "bar": 2}, two.metadata)
        self.assertEqual(3, two.bar)
        self.assertEqual(None, two.md5)

        for attr in [
            "_id",
            "name",
            "content_type",
            "length",
            "chunk_size",
            "upload_date",
            "aliases",
            "metadata",
            "md5",
        ]:
            self.assertRaises(AttributeError, setattr, two, attr, 5)

    async def test_grid_out_file_document(self):
        one = AsyncGridIn(self.db.fs)
        await one.write(b"foo bar")
        await one.close()

        two = AsyncGridOut(self.db.fs, file_document=await self.db.fs.files.find_one())
        self.assertEqual(b"foo bar", await two.read())

        three = AsyncGridOut(self.db.fs, 5, file_document=await self.db.fs.files.find_one())
        self.assertEqual(b"foo bar", await three.read())

        four = AsyncGridOut(self.db.fs, file_document={})
        with self.assertRaises(NoFile):
            if not _IS_SYNC:
                await four.open()
            four.name

    async def test_write_file_like(self):
        one = AsyncGridIn(self.db.fs)
        await one.write(b"hello world")
        await one.close()

        two = AsyncGridOut(self.db.fs, one._id)

        three = AsyncGridIn(self.db.fs)
        await three.write(two)
        await three.close()

        four = AsyncGridOut(self.db.fs, three._id)
        self.assertEqual(b"hello world", await four.read())

        five = AsyncGridIn(self.db.fs, chunk_size=2)
        await five.write(b"hello")
        buffer = BytesIO(b" world")
        await five.write(buffer)
        await five.write(b" and mongodb")
        await five.close()
        self.assertEqual(
            b"hello world and mongodb", await AsyncGridOut(self.db.fs, five._id).read()
        )

    async def test_write_lines(self):
        a = AsyncGridIn(self.db.fs)
        await a.writelines([b"hello ", b"world"])
        await a.close()

        self.assertEqual(b"hello world", await AsyncGridOut(self.db.fs, a._id).read())

    async def test_close(self):
        f = AsyncGridIn(self.db.fs)
        await f.close()
        with self.assertRaises(ValueError):
            await f.write("test")
        await f.close()

    async def test_closed(self):
        f = AsyncGridIn(self.db.fs, chunkSize=5)
        await f.write(b"Hello world.\nHow are you?")
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        if not _IS_SYNC:
            await g.open()
        self.assertFalse(g.closed)
        await g.read(1)
        self.assertFalse(g.closed)
        await g.read(100)
        self.assertFalse(g.closed)
        await g.close()
        self.assertTrue(g.closed)

    async def test_multi_chunk_file(self):
        random_string = b"a" * (DEFAULT_CHUNK_SIZE + 1000)

        f = AsyncGridIn(self.db.fs)
        await f.write(random_string)
        await f.close()

        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(2, await self.db.fs.chunks.count_documents({}))

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(random_string, await g.read())

    async def test_small_chunks(self):
        self.files = 0
        self.chunks = 0

        async def helper(data):
            f = AsyncGridIn(self.db.fs, chunkSize=1)
            await f.write(data)
            await f.close()

            self.files += 1
            self.chunks += len(data)

            self.assertEqual(self.files, await self.db.fs.files.count_documents({}))
            self.assertEqual(self.chunks, await self.db.fs.chunks.count_documents({}))

            g = AsyncGridOut(self.db.fs, f._id)
            self.assertEqual(data, await g.read())

            g = AsyncGridOut(self.db.fs, f._id)
            self.assertEqual(data, await g.read(10) + await g.read(10))
            return True

        await qcheck.check_unittest(self, helper, qcheck.gen_string(qcheck.gen_range(0, 20)))

    async def test_seek(self):
        f = AsyncGridIn(self.db.fs, chunkSize=3)
        await f.write(b"hello world")
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"hello world", await g.read())
        await g.seek(0)
        self.assertEqual(b"hello world", await g.read())
        await g.seek(1)
        self.assertEqual(b"ello world", await g.read())
        with self.assertRaises(IOError):
            await g.seek(-1)

        await g.seek(-3, _SEEK_END)
        self.assertEqual(b"rld", await g.read())
        await g.seek(0, _SEEK_END)
        self.assertEqual(b"", await g.read())
        with self.assertRaises(IOError):
            await g.seek(-100, _SEEK_END)

        await g.seek(3)
        await g.seek(3, _SEEK_CUR)
        self.assertEqual(b"world", await g.read())
        with self.assertRaises(IOError):
            await g.seek(-100, _SEEK_CUR)

    async def test_tell(self):
        f = AsyncGridIn(self.db.fs, chunkSize=3)
        await f.write(b"hello world")
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(0, g.tell())
        await g.read(0)
        self.assertEqual(0, g.tell())
        await g.read(1)
        self.assertEqual(1, g.tell())
        await g.read(2)
        self.assertEqual(3, g.tell())
        await g.read()
        self.assertEqual(g.length, g.tell())

    async def test_multiple_reads(self):
        f = AsyncGridIn(self.db.fs, chunkSize=3)
        await f.write(b"hello world")
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"he", await g.read(2))
        self.assertEqual(b"ll", await g.read(2))
        self.assertEqual(b"o ", await g.read(2))
        self.assertEqual(b"wo", await g.read(2))
        self.assertEqual(b"rl", await g.read(2))
        self.assertEqual(b"d", await g.read(2))
        self.assertEqual(b"", await g.read(2))

    async def test_readline(self):
        f = AsyncGridIn(self.db.fs, chunkSize=5)
        await f.write(
            b"""Hello world,
How are you?
Hope all is well.
Bye"""
        )
        await f.close()

        # Try read(), then readline().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"H", await g.read(1))
        self.assertEqual(b"ello world,\n", await g.readline())
        self.assertEqual(b"How a", await g.readline(5))
        self.assertEqual(b"", await g.readline(0))
        self.assertEqual(b"re you?\n", await g.readline())
        self.assertEqual(b"Hope all is well.\n", await g.readline(1000))
        self.assertEqual(b"Bye", await g.readline())
        self.assertEqual(b"", await g.readline())

        # Try readline() first, then read().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"He", await g.readline(2))
        self.assertEqual(b"l", await g.read(1))
        self.assertEqual(b"lo", await g.readline(2))
        self.assertEqual(b" world,\n", await g.readline())

        # Only readline().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"H", await g.readline(1))
        self.assertEqual(b"e", await g.readline(1))
        self.assertEqual(b"llo world,\n", await g.readline())

    async def test_readlines(self):
        f = AsyncGridIn(self.db.fs, chunkSize=5)
        await f.write(
            b"""Hello world,
How are you?
Hope all is well.
Bye"""
        )
        await f.close()

        # Try read(), then readlines().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"He", await g.read(2))
        self.assertEqual([b"llo world,\n", b"How are you?\n"], await g.readlines(11))
        self.assertEqual([b"Hope all is well.\n", b"Bye"], await g.readlines())
        self.assertEqual([], await g.readlines())

        # Try readline(), then readlines().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"Hello world,\n", await g.readline())
        self.assertEqual([b"How are you?\n", b"Hope all is well.\n"], await g.readlines(13))
        self.assertEqual(b"Bye", await g.readline())
        self.assertEqual([], await g.readlines())

        # Only readlines().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(
            [b"Hello world,\n", b"How are you?\n", b"Hope all is well.\n", b"Bye"],
            await g.readlines(),
        )

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(
            [b"Hello world,\n", b"How are you?\n", b"Hope all is well.\n", b"Bye"],
            await g.readlines(0),
        )

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual([b"Hello world,\n"], await g.readlines(1))
        self.assertEqual([b"How are you?\n"], await g.readlines(12))
        self.assertEqual([b"Hope all is well.\n", b"Bye"], await g.readlines(18))

        # Try readlines() first, then read().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual([b"Hello world,\n"], await g.readlines(1))
        self.assertEqual(b"H", await g.read(1))
        self.assertEqual([b"ow are you?\n", b"Hope all is well.\n"], await g.readlines(29))
        self.assertEqual([b"Bye"], await g.readlines(1))

        # Try readlines() first, then readline().
        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual([b"Hello world,\n"], await g.readlines(1))
        self.assertEqual(b"How are you?\n", await g.readline())
        self.assertEqual([b"Hope all is well.\n"], await g.readlines(17))
        self.assertEqual(b"Bye", await g.readline())

    async def test_iterator(self):
        f = AsyncGridIn(self.db.fs)
        await f.close()
        g = AsyncGridOut(self.db.fs, f._id)
        if _IS_SYNC:
            self.assertEqual([], list(g))
        else:
            self.assertEqual([], await g.to_list())

        f = AsyncGridIn(self.db.fs)
        await f.write(b"hello world\nhere are\nsome lines.")
        await f.close()
        g = AsyncGridOut(self.db.fs, f._id)
        if _IS_SYNC:
            self.assertEqual([b"hello world\n", b"here are\n", b"some lines."], list(g))
        else:
            self.assertEqual([b"hello world\n", b"here are\n", b"some lines."], await g.to_list())

        self.assertEqual(b"", await g.read(5))
        if _IS_SYNC:
            self.assertEqual([], list(g))
        else:
            self.assertEqual([], await g.to_list())

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"hello world\n", await anext(aiter(g)))
        self.assertEqual(b"here", await g.read(4))
        self.assertEqual(b" are\n", await anext(aiter(g)))
        self.assertEqual(b"some lines", await g.read(10))
        self.assertEqual(b".", await anext(aiter(g)))
        with self.assertRaises(StopAsyncIteration):
            await aiter(g).__anext__()

        f = AsyncGridIn(self.db.fs, chunk_size=2)
        await f.write(b"hello world")
        await f.close()
        g = AsyncGridOut(self.db.fs, f._id)
        if _IS_SYNC:
            self.assertEqual([b"hello world"], list(g))
        else:
            self.assertEqual([b"hello world"], await g.to_list())

    async def test_read_unaligned_buffer_size(self):
        in_data = b"This is a text that doesn't quite fit in a single 16-byte chunk."
        f = AsyncGridIn(self.db.fs, chunkSize=16)
        await f.write(in_data)
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        out_data = b""
        while 1:
            s = await g.read(13)
            if not s:
                break
            out_data += s

        self.assertEqual(in_data, out_data)

    async def test_readchunk(self):
        in_data = b"a" * 10
        f = AsyncGridIn(self.db.fs, chunkSize=3)
        await f.write(in_data)
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(3, len(await g.readchunk()))

        self.assertEqual(2, len(await g.read(2)))
        self.assertEqual(1, len(await g.readchunk()))

        self.assertEqual(3, len(await g.read(3)))

        self.assertEqual(1, len(await g.readchunk()))

        self.assertEqual(0, len(await g.readchunk()))

    async def test_write_unicode(self):
        f = AsyncGridIn(self.db.fs)
        with self.assertRaises(TypeError):
            await f.write("foo")

        f = AsyncGridIn(self.db.fs, encoding="utf-8")
        await f.write("foo")
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual(b"foo", await g.read())

        f = AsyncGridIn(self.db.fs, encoding="iso-8859-1")
        await f.write("aé")
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)
        self.assertEqual("aé".encode("iso-8859-1"), await g.read())

    async def test_set_after_close(self):
        f = AsyncGridIn(self.db.fs, _id="foo", bar="baz")

        self.assertEqual("foo", f._id)
        self.assertEqual("baz", f.bar)
        self.assertRaises(AttributeError, getattr, f, "baz")
        self.assertRaises(AttributeError, getattr, f, "uploadDate")

        self.assertRaises(AttributeError, setattr, f, "_id", 5)
        if _IS_SYNC:
            f.bar = "foo"
            f.baz = 5
        else:
            await f.set("bar", "foo")
            await f.set("baz", 5)

        self.assertEqual("foo", f._id)
        self.assertEqual("foo", f.bar)
        self.assertEqual(5, f.baz)
        self.assertRaises(AttributeError, getattr, f, "uploadDate")

        await f.close()

        self.assertEqual("foo", f._id)
        self.assertEqual("foo", f.bar)
        self.assertEqual(5, f.baz)
        self.assertTrue(f.uploadDate)

        self.assertRaises(AttributeError, setattr, f, "_id", 5)
        if _IS_SYNC:
            f.bar = "a"
            f.baz = "b"
        else:
            await f.set("bar", "a")
            await f.set("baz", "b")
        self.assertRaises(AttributeError, setattr, f, "upload_date", 5)

        g = AsyncGridOut(self.db.fs, f._id)
        if not _IS_SYNC:
            await g.open()
        self.assertEqual("a", g.bar)
        self.assertEqual("b", g.baz)
        # Versions 2.0.1 and older saved a _closed field for some reason.
        self.assertRaises(AttributeError, getattr, g, "_closed")

    async def test_context_manager(self):
        contents = b"Imagine this is some important data..."

        async with AsyncGridIn(self.db.fs, filename="important") as infile:
            await infile.write(contents)

        async with AsyncGridOut(self.db.fs, infile._id) as outfile:
            self.assertEqual(contents, await outfile.read())

    async def test_exception_file_non_existence(self):
        contents = b"Imagine this is some important data..."

        with self.assertRaises(ConnectionError):
            async with AsyncGridIn(self.db.fs, filename="important") as infile:
                await infile.write(contents)
                raise ConnectionError("Test exception")

        # Expectation: File chunks are written, entry in files doesn't appear.
        self.assertEqual(
            await self.db.fs.chunks.count_documents({"files_id": infile._id}), infile._chunk_number
        )

        self.assertIsNone(await self.db.fs.files.find_one({"_id": infile._id}))
        self.assertTrue(infile.closed)

    async def test_prechunked_string(self):
        async def write_me(s, chunk_size):
            buf = BytesIO(s)
            infile = AsyncGridIn(self.db.fs)
            while True:
                to_write = buf.read(chunk_size)
                if to_write == b"":
                    break
                await infile.write(to_write)
            await infile.close()
            buf.close()

            outfile = AsyncGridOut(self.db.fs, infile._id)
            data = await outfile.read()
            self.assertEqual(s, data)

        s = b"x" * DEFAULT_CHUNK_SIZE * 4
        # Test with default chunk size
        await write_me(s, DEFAULT_CHUNK_SIZE)
        # Multiple
        await write_me(s, DEFAULT_CHUNK_SIZE * 3)
        # Custom
        await write_me(s, 262300)

    async def test_grid_out_lazy_connect(self):
        fs = self.db.fs
        outfile = AsyncGridOut(fs, file_id=-1)
        with self.assertRaises(NoFile):
            await outfile.read()
        with self.assertRaises(NoFile):
            if not _IS_SYNC:
                await outfile.open()
            outfile.filename

        infile = AsyncGridIn(fs, filename=1)
        await infile.close()

        outfile = AsyncGridOut(fs, infile._id)
        await outfile.read()
        outfile.filename

        outfile = AsyncGridOut(fs, infile._id)
        await outfile.readchunk()

    async def test_grid_in_lazy_connect(self):
        client = self.simple_client("badhost", connect=False, serverSelectionTimeoutMS=10)
        fs = client.db.fs
        infile = AsyncGridIn(fs, file_id=-1, chunk_size=1)
        with self.assertRaises(ServerSelectionTimeoutError):
            await infile.write(b"data")
        with self.assertRaises(ServerSelectionTimeoutError):
            await infile.close()

    async def test_unacknowledged(self):
        # w=0 is prohibited.
        with self.assertRaises(ConfigurationError):
            AsyncGridIn((await self.async_rs_or_single_client(w=0)).pymongo_test.fs)

    async def test_survive_cursor_not_found(self):
        # By default the find command returns 101 documents in the first batch.
        # Use 102 batches to cause a single getMore.
        chunk_size = 1024
        data = b"d" * (102 * chunk_size)
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        db = client.pymongo_test
        async with AsyncGridIn(db.fs, chunk_size=chunk_size) as infile:
            await infile.write(data)

        async with AsyncGridOut(db.fs, infile._id) as outfile:
            self.assertEqual(len(await outfile.readchunk()), chunk_size)

            # Kill the cursor to simulate the cursor timing out on the server
            # when an application spends a long time between two calls to
            # readchunk().
            assert await client.address is not None
            await client._close_cursor_now(
                outfile._chunk_iter._cursor.cursor_id,
                _CursorAddress(await client.address, db.fs.chunks.full_name),  # type: ignore[arg-type]
            )

            # Read the rest of the file without error.
            self.assertEqual(len(await outfile.read()), len(data) - chunk_size)

        # Paranoid, ensure that a getMore was actually sent.
        self.assertIn("getMore", listener.started_command_names())

    @async_client_context.require_sync
    async def test_zip(self):
        zf = BytesIO()
        z = zipfile.ZipFile(zf, "w")
        z.writestr("test.txt", b"hello world")
        z.close()
        zf.seek(0)

        f = AsyncGridIn(self.db.fs, filename="test.zip")
        await f.write(zf)
        await f.close()
        self.assertEqual(1, await self.db.fs.files.count_documents({}))
        self.assertEqual(1, await self.db.fs.chunks.count_documents({}))

        g = AsyncGridOut(self.db.fs, f._id)
        z = zipfile.ZipFile(g)
        self.assertSequenceEqual(z.namelist(), ["test.txt"])
        self.assertEqual(z.read("test.txt"), b"hello world")

    async def test_grid_out_unsupported_operations(self):
        f = AsyncGridIn(self.db.fs, chunkSize=3)
        await f.write(b"hello world")
        await f.close()

        g = AsyncGridOut(self.db.fs, f._id)

        self.assertRaises(io.UnsupportedOperation, g.writelines, [b"some", b"lines"])
        self.assertRaises(io.UnsupportedOperation, g.write, b"some text")
        self.assertRaises(io.UnsupportedOperation, g.fileno)
        self.assertRaises(io.UnsupportedOperation, g.truncate)

        self.assertFalse(g.writable())
        self.assertFalse(g.isatty())


if __name__ == "__main__":
    unittest.main()
