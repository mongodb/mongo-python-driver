# -*- coding: utf-8 -*-
#
# Copyright 2009-2015 MongoDB, Inc.
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

import datetime
import sys
sys.path[0:0] = [""]

from bson.objectid import ObjectId
from bson.py3compat import u, StringIO
from gridfs import GridFS
from gridfs.grid_file import (DEFAULT_CHUNK_SIZE,
                              _SEEK_CUR,
                              _SEEK_END,
                              GridIn,
                              GridOut,
                              GridOutCursor)
from gridfs.errors import NoFile
from pymongo import MongoClient
from pymongo.errors import ConfigurationError, ConnectionFailure
from test import (IntegrationTest,
                  host,
                  port,
                  unittest,
                  qcheck)
from test.utils import rs_or_single_client


class TestGridFileNoConnect(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        client = MongoClient(host, port, connect=False)
        cls.db = client.pymongo_test

    def test_grid_in_custom_opts(self):
        self.assertRaises(TypeError, GridIn, "foo")

        a = GridIn(self.db.fs, _id=5, filename="my_file",
                   contentType="text/html", chunkSize=1000, aliases=["foo"],
                   metadata={"foo": 1, "bar": 2}, bar=3, baz="hello")

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

        b = GridIn(self.db.fs,
                   content_type="text/html", chunk_size=1000, baz=100)
        self.assertEqual("text/html", b.content_type)
        self.assertEqual(1000, b.chunk_size)
        self.assertEqual(100, b.baz)

    def test_grid_out_cursor_options(self):
        self.assertRaises(TypeError, GridOutCursor.__init__, self.db.fs, {},
                          projection={"filename": 1})

        cursor = GridOutCursor(self.db.fs, {})
        cursor_clone = cursor.clone()
        self.assertEqual(cursor_clone.__dict__, cursor.__dict__)

        self.assertRaises(NotImplementedError, cursor.add_option, 0)
        self.assertRaises(NotImplementedError, cursor.remove_option, 0)


class TestGridFile(IntegrationTest):

    def setUp(self):
        self.db.drop_collection('fs.files')
        self.db.drop_collection('fs.chunks')

    def test_basic(self):
        f = GridIn(self.db.fs, filename="test")
        f.write(b"hello world")
        f.close()
        self.assertEqual(1, self.db.fs.files.find().count())
        self.assertEqual(1, self.db.fs.chunks.find().count())

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"hello world", g.read())

        # make sure it's still there...
        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"hello world", g.read())

        f = GridIn(self.db.fs, filename="test")
        f.close()
        self.assertEqual(2, self.db.fs.files.find().count())
        self.assertEqual(1, self.db.fs.chunks.find().count())

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"", g.read())

        # test that reading 0 returns proper type
        self.assertEqual(b"", g.read(0))

    def test_md5(self):
        f = GridIn(self.db.fs)
        f.write(b"hello world\n")
        f.close()
        self.assertEqual("6f5902ac237024bdd0c176cb93063dc4", f.md5)

    def test_alternate_collection(self):
        self.db.alt.files.delete_many({})
        self.db.alt.chunks.delete_many({})

        f = GridIn(self.db.alt)
        f.write(b"hello world")
        f.close()

        self.assertEqual(1, self.db.alt.files.find().count())
        self.assertEqual(1, self.db.alt.chunks.find().count())

        g = GridOut(self.db.alt, f._id)
        self.assertEqual(b"hello world", g.read())

        # test that md5 still works...
        self.assertEqual("5eb63bbbe01eeed093cb22bb8f5acdc3", g.md5)

    def test_grid_in_default_opts(self):
        self.assertRaises(TypeError, GridIn, "foo")

        a = GridIn(self.db.fs)

        self.assertTrue(isinstance(a._id, ObjectId))
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

        self.assertRaises(AttributeError, getattr, a, "md5")
        self.assertRaises(AttributeError, setattr, a, "md5", 5)

        a.close()

        a.forty_two = 42
        self.assertEqual(42, a.forty_two)

        self.assertTrue(isinstance(a._id, ObjectId))
        self.assertRaises(AttributeError, setattr, a, "_id", 5)

        self.assertEqual("my_file", a.filename)
        self.assertEqual("my_file", a.name)

        self.assertEqual("text/html", a.content_type)

        self.assertEqual(0, a.length)
        self.assertRaises(AttributeError, setattr, a, "length", 5)

        self.assertEqual(255 * 1024, a.chunk_size)
        self.assertRaises(AttributeError, setattr, a, "chunk_size", 5)

        self.assertTrue(isinstance(a.upload_date, datetime.datetime))
        self.assertRaises(AttributeError, setattr, a, "upload_date", 5)

        self.assertEqual(["foo"], a.aliases)

        self.assertEqual({"foo": 1}, a.metadata)

        self.assertEqual("d41d8cd98f00b204e9800998ecf8427e", a.md5)
        self.assertRaises(AttributeError, setattr, a, "md5", 5)

        # Make sure custom attributes that were set both before and after
        # a.close() are reflected in b. PYTHON-411.
        b = GridFS(self.db).get_last_version(filename=a.filename)
        self.assertEqual(a.metadata, b.metadata)
        self.assertEqual(a.aliases, b.aliases)
        self.assertEqual(a.forty_two, b.forty_two)

    def test_grid_out_default_opts(self):
        self.assertRaises(TypeError, GridOut, "foo")

        gout = GridOut(self.db.fs, 5)
        with self.assertRaises(NoFile):
            gout.name

        a = GridIn(self.db.fs)
        a.close()

        b = GridOut(self.db.fs, a._id)

        self.assertEqual(a._id, b._id)
        self.assertEqual(0, b.length)
        self.assertEqual(None, b.content_type)
        self.assertEqual(None, b.name)
        self.assertEqual(None, b.filename)
        self.assertEqual(255 * 1024, b.chunk_size)
        self.assertTrue(isinstance(b.upload_date, datetime.datetime))
        self.assertEqual(None, b.aliases)
        self.assertEqual(None, b.metadata)
        self.assertEqual("d41d8cd98f00b204e9800998ecf8427e", b.md5)

        for attr in ["_id", "name", "content_type", "length", "chunk_size",
                     "upload_date", "aliases", "metadata", "md5"]:
            self.assertRaises(AttributeError, setattr, b, attr, 5)

    def test_grid_out_custom_opts(self):
        one = GridIn(self.db.fs, _id=5, filename="my_file",
                   contentType="text/html", chunkSize=1000, aliases=["foo"],
                   metadata={"foo": 1, "bar": 2}, bar=3, baz="hello")
        one.write(b"hello world")
        one.close()

        two = GridOut(self.db.fs, 5)

        self.assertEqual("my_file", two.name)
        self.assertEqual("my_file", two.filename)
        self.assertEqual(5, two._id)
        self.assertEqual(11, two.length)
        self.assertEqual("text/html", two.content_type)
        self.assertEqual(1000, two.chunk_size)
        self.assertTrue(isinstance(two.upload_date, datetime.datetime))
        self.assertEqual(["foo"], two.aliases)
        self.assertEqual({"foo": 1, "bar": 2}, two.metadata)
        self.assertEqual(3, two.bar)
        self.assertEqual("5eb63bbbe01eeed093cb22bb8f5acdc3", two.md5)

        for attr in ["_id", "name", "content_type", "length", "chunk_size",
                     "upload_date", "aliases", "metadata", "md5"]:
            self.assertRaises(AttributeError, setattr, two, attr, 5)

    def test_grid_out_file_document(self):
        one = GridIn(self.db.fs)
        one.write(b"foo bar")
        one.close()

        two = GridOut(self.db.fs, file_document=self.db.fs.files.find_one())
        self.assertEqual(b"foo bar", two.read())

        three = GridOut(self.db.fs, 5, file_document=self.db.fs.files.find_one())
        self.assertEqual(b"foo bar", three.read())

        four = GridOut(self.db.fs, file_document={})
        with self.assertRaises(NoFile):
            four.name

    def test_write_file_like(self):
        one = GridIn(self.db.fs)
        one.write(b"hello world")
        one.close()

        two = GridOut(self.db.fs, one._id)

        three = GridIn(self.db.fs)
        three.write(two)
        three.close()

        four = GridOut(self.db.fs, three._id)
        self.assertEqual(b"hello world", four.read())

        five = GridIn(self.db.fs, chunk_size=2)
        five.write(b"hello")
        buffer = StringIO(b" world")
        five.write(buffer)
        five.write(b" and mongodb")
        five.close()
        self.assertEqual(b"hello world and mongodb",
                         GridOut(self.db.fs, five._id).read())

    def test_write_lines(self):
        a = GridIn(self.db.fs)
        a.writelines([b"hello ", b"world"])
        a.close()

        self.assertEqual(b"hello world", GridOut(self.db.fs, a._id).read())

    def test_close(self):
        f = GridIn(self.db.fs)
        f.close()
        self.assertRaises(ValueError, f.write, "test")
        f.close()

    def test_multi_chunk_file(self):
        random_string = b'a' * (DEFAULT_CHUNK_SIZE + 1000)

        f = GridIn(self.db.fs)
        f.write(random_string)
        f.close()

        self.assertEqual(1, self.db.fs.files.find().count())
        self.assertEqual(2, self.db.fs.chunks.find().count())

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(random_string, g.read())

    def test_small_chunks(self):
        self.files = 0
        self.chunks = 0

        def helper(data):
            f = GridIn(self.db.fs, chunkSize=1)
            f.write(data)
            f.close()

            self.files += 1
            self.chunks += len(data)

            self.assertEqual(self.files, self.db.fs.files.find().count())
            self.assertEqual(self.chunks, self.db.fs.chunks.find().count())

            g = GridOut(self.db.fs, f._id)
            self.assertEqual(data, g.read())

            g = GridOut(self.db.fs, f._id)
            self.assertEqual(data, g.read(10) + g.read(10))
            return True

        qcheck.check_unittest(self, helper,
                              qcheck.gen_string(qcheck.gen_range(0, 20)))

    def test_seek(self):
        f = GridIn(self.db.fs, chunkSize=3)
        f.write(b"hello world")
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"hello world", g.read())
        g.seek(0)
        self.assertEqual(b"hello world", g.read())
        g.seek(1)
        self.assertEqual(b"ello world", g.read())
        self.assertRaises(IOError, g.seek, -1)

        g.seek(-3, _SEEK_END)
        self.assertEqual(b"rld", g.read())
        g.seek(0, _SEEK_END)
        self.assertEqual(b"", g.read())
        self.assertRaises(IOError, g.seek, -100, _SEEK_END)

        g.seek(3)
        g.seek(3, _SEEK_CUR)
        self.assertEqual(b"world", g.read())
        self.assertRaises(IOError, g.seek, -100, _SEEK_CUR)

    def test_tell(self):
        f = GridIn(self.db.fs, chunkSize=3)
        f.write(b"hello world")
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(0, g.tell())
        g.read(0)
        self.assertEqual(0, g.tell())
        g.read(1)
        self.assertEqual(1, g.tell())
        g.read(2)
        self.assertEqual(3, g.tell())
        g.read()
        self.assertEqual(g.length, g.tell())

    def test_multiple_reads(self):
        f = GridIn(self.db.fs, chunkSize=3)
        f.write(b"hello world")
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"he", g.read(2))
        self.assertEqual(b"ll", g.read(2))
        self.assertEqual(b"o ", g.read(2))
        self.assertEqual(b"wo", g.read(2))
        self.assertEqual(b"rl", g.read(2))
        self.assertEqual(b"d", g.read(2))
        self.assertEqual(b"", g.read(2))

    def test_readline(self):
        f = GridIn(self.db.fs, chunkSize=5)
        f.write((b"""Hello world,
How are you?
Hope all is well.
Bye"""))
        f.close()

        # Try read(), then readline().
        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"H", g.read(1))
        self.assertEqual(b"ello world,\n", g.readline())
        self.assertEqual(b"How a", g.readline(5))
        self.assertEqual(b"", g.readline(0))
        self.assertEqual(b"re you?\n", g.readline())
        self.assertEqual(b"Hope all is well.\n", g.readline(1000))
        self.assertEqual(b"Bye", g.readline())
        self.assertEqual(b"", g.readline())

        # Try readline() first, then read().
        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"He", g.readline(2))
        self.assertEqual(b"l", g.read(1))
        self.assertEqual(b"lo", g.readline(2))
        self.assertEqual(b" world,\n", g.readline())

        # Only readline().
        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"H", g.readline(1))
        self.assertEqual(b"e", g.readline(1))
        self.assertEqual(b"llo world,\n", g.readline())

    def test_iterator(self):
        f = GridIn(self.db.fs)
        f.close()
        g = GridOut(self.db.fs, f._id)
        self.assertEqual([], list(g))

        f = GridIn(self.db.fs)
        f.write(b"hello world")
        f.close()
        g = GridOut(self.db.fs, f._id)
        self.assertEqual([b"hello world"], list(g))
        self.assertEqual(b"hello", g.read(5))
        self.assertEqual([b"hello world"], list(g))
        self.assertEqual(b" worl", g.read(5))

        f = GridIn(self.db.fs, chunk_size=2)
        f.write(b"hello world")
        f.close()
        g = GridOut(self.db.fs, f._id)
        self.assertEqual([b"he", b"ll", b"o ",
                          b"wo", b"rl", b"d"], list(g))

    def test_read_unaligned_buffer_size(self):
        in_data = (b"This is a text that doesn't "
                   b"quite fit in a single 16-byte chunk.")
        f = GridIn(self.db.fs, chunkSize=16)
        f.write(in_data)
        f.close()

        g = GridOut(self.db.fs, f._id)
        out_data = b''
        while 1:
            s = g.read(13)
            if not s:
                break
            out_data += s

        self.assertEqual(in_data, out_data)

    def test_readchunk(self):
        in_data = b'a' * 10
        f = GridIn(self.db.fs, chunkSize=3)
        f.write(in_data)
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(3, len(g.readchunk()))

        self.assertEqual(2, len(g.read(2)))
        self.assertEqual(1, len(g.readchunk()))

        self.assertEqual(3, len(g.read(3)))

        self.assertEqual(1, len(g.readchunk()))

        self.assertEqual(0, len(g.readchunk()))

    def test_write_unicode(self):
        f = GridIn(self.db.fs)
        self.assertRaises(TypeError, f.write, u("foo"))

        f = GridIn(self.db.fs, encoding="utf-8")
        f.write(u("foo"))
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(b"foo", g.read())

        f = GridIn(self.db.fs, encoding="iso-8859-1")
        f.write(u("aé"))
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual(u("aé").encode("iso-8859-1"), g.read())

    def test_set_after_close(self):
        f = GridIn(self.db.fs, _id="foo", bar="baz")

        self.assertEqual("foo", f._id)
        self.assertEqual("baz", f.bar)
        self.assertRaises(AttributeError, getattr, f, "baz")
        self.assertRaises(AttributeError, getattr, f, "uploadDate")

        self.assertRaises(AttributeError, setattr, f, "_id", 5)
        f.bar = "foo"
        f.baz = 5

        self.assertEqual("foo", f._id)
        self.assertEqual("foo", f.bar)
        self.assertEqual(5, f.baz)
        self.assertRaises(AttributeError, getattr, f, "uploadDate")

        f.close()

        self.assertEqual("foo", f._id)
        self.assertEqual("foo", f.bar)
        self.assertEqual(5, f.baz)
        self.assertTrue(f.uploadDate)

        self.assertRaises(AttributeError, setattr, f, "_id", 5)
        f.bar = "a"
        f.baz = "b"
        self.assertRaises(AttributeError, setattr, f, "upload_date", 5)

        g = GridOut(self.db.fs, f._id)
        self.assertEqual("a", g.bar)
        self.assertEqual("b", g.baz)
        # Versions 2.0.1 and older saved a _closed field for some reason.
        self.assertRaises(AttributeError, getattr, g, "_closed")

    def test_context_manager(self):
        contents = b"Imagine this is some important data..."

        with GridIn(self.db.fs, filename="important") as infile:
            infile.write(contents)

        with GridOut(self.db.fs, infile._id) as outfile:
            self.assertEqual(contents, outfile.read())

    def test_prechunked_string(self):

        def write_me(s, chunk_size):
            buf = StringIO(s)
            infile = GridIn(self.db.fs)
            while True:
                to_write = buf.read(chunk_size)
                if to_write == b'':
                    break
                infile.write(to_write)
            infile.close()
            buf.close()

            outfile = GridOut(self.db.fs, infile._id)
            data = outfile.read()
            self.assertEqual(s, data)

        s = b'x' * DEFAULT_CHUNK_SIZE * 4
        # Test with default chunk size
        write_me(s, DEFAULT_CHUNK_SIZE)
        # Multiple
        write_me(s, DEFAULT_CHUNK_SIZE * 3)
        # Custom
        write_me(s, 262300)

    def test_grid_out_lazy_connect(self):
        fs = self.db.fs
        outfile = GridOut(fs, file_id=-1)
        self.assertRaises(NoFile, outfile.read)
        self.assertRaises(NoFile, getattr, outfile, 'filename')

        infile = GridIn(fs, filename=1)
        infile.close()

        outfile = GridOut(fs, infile._id)
        outfile.read()
        outfile.filename

        outfile = GridOut(fs, infile._id)
        outfile.readchunk()

    def test_grid_in_lazy_connect(self):
        client = MongoClient('badhost', connect=False,
                             serverSelectionTimeoutMS=10)
        fs = client.db.fs
        infile = GridIn(fs, file_id=-1, chunk_size=1)
        self.assertRaises(ConnectionFailure, infile.write, b'data')
        self.assertRaises(ConnectionFailure, infile.close)

    def test_unacknowledged(self):
        # w=0 is prohibited.
        with self.assertRaises(ConfigurationError):
            GridIn(rs_or_single_client(w=0).pymongo_test.fs)


if __name__ == "__main__":
    unittest.main()
