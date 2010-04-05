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

"""Tests for the grid_file module.
"""

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
import datetime
import os
import sys
import unittest
sys.path[0:0] = [""]

from gridfs.grid_file import (_SEEK_CUR,
                              _SEEK_END,
                              GridIn,
                              GridFile,
                              GridOut)
from gridfs.errors import (NoFile,
                           UnsupportedAPI)
from pymongo.objectid import ObjectId
from test_connection import get_connection
import qcheck


class TestGridFile(unittest.TestCase):

    def setUp(self):
        self.db = get_connection().pymongo_test
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

    def test_basic(self):
        f = GridIn(self.db.fs, filename="test")
        f.write("hello world")
        f.close()
        self.assertEqual(1, self.db.fs.files.find().count())
        self.assertEqual(1, self.db.fs.chunks.find().count())

        g = GridOut(self.db.fs, f._id)
        self.assertEqual("hello world", g.read())

        # make sure it's still there...
        g = GridOut(self.db.fs, f._id)
        self.assertEqual("hello world", g.read())


        f = GridIn(self.db.fs, filename="test")
        f.close()
        self.assertEqual(2, self.db.fs.files.find().count())
        self.assertEqual(1, self.db.fs.chunks.find().count())

        g = GridOut(self.db.fs, f._id)
        self.assertEqual("", g.read())

    def test_md5(self):
        f = GridIn(self.db.fs)
        f.write("hello world\n")
        f.close()
        self.assertEqual("6f5902ac237024bdd0c176cb93063dc4", f.md5)

    def test_alternate_collection(self):
        self.db.alt.files.remove({})
        self.db.alt.chunks.remove({})

        f = GridIn(self.db.alt)
        f.write("hello world")
        f.close()

        self.assertEqual(1, self.db.alt.files.find().count())
        self.assertEqual(1, self.db.alt.chunks.find().count())

        g = GridOut(self.db.alt, f._id)
        self.assertEqual("hello world", g.read())

        # test that md5 still works...
        self.assertEqual("5eb63bbbe01eeed093cb22bb8f5acdc3", g.md5)

    def test_grid_file(self):
        self.assertRaises(UnsupportedAPI, GridFile)

    def test_grid_in_default_opts(self):
        self.assertRaises(TypeError, GridIn, "foo")

        a = GridIn(self.db.fs)

        self.assert_(isinstance(a._id, ObjectId))
        self.assertRaises(AttributeError, setattr, a, "_id", 5)

        self.assertEqual(None, a.name)
        a.name = "my_file"
        self.assertEqual("my_file", a.name)

        self.assertEqual(None, a.content_type)
        a.content_type = "text/html"
        self.assertEqual("text/html", a.content_type)

        self.assertRaises(AttributeError, getattr, a, "length")
        self.assertRaises(AttributeError, setattr, a, "length", 5)

        self.assertEqual(256 * 1024, a.chunk_size)
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

        self.assert_(isinstance(a._id, ObjectId))
        self.assertRaises(AttributeError, setattr, a, "_id", 5)

        self.assertEqual("my_file", a.name)
        self.assertRaises(AttributeError, setattr, a, "name", "foo")

        self.assertEqual("text/html", a.content_type)
        self.assertRaises(AttributeError, setattr, a, "content_type", "foo")

        self.assertEqual(0, a.length)
        self.assertRaises(AttributeError, setattr, a, "length", 5)

        self.assertEqual(256 * 1024, a.chunk_size)
        self.assertRaises(AttributeError, setattr, a, "chunk_size", 5)

        self.assert_(isinstance(a.upload_date, datetime.datetime))
        self.assertRaises(AttributeError, setattr, a, "upload_date", 5)

        self.assertEqual(["foo"], a.aliases)
        self.assertRaises(AttributeError, setattr, a, "aliases", [])

        self.assertEqual({"foo": 1}, a.metadata)
        self.assertRaises(AttributeError, setattr, a, "metadata", {})

        self.assertEqual("d41d8cd98f00b204e9800998ecf8427e", a.md5)
        self.assertRaises(AttributeError, setattr, a, "md5", 5)

    def test_grid_in_custom_opts(self):
        self.assertRaises(TypeError, GridIn, "foo")

        a = GridIn(self.db.fs, _id=5, filename="my_file",
                   contentType="text/html", chunkSize=1000, aliases=["foo"],
                   metadata={"foo": 1, "bar": 2}, bar=3, baz="hello")

        self.assertEqual(5, a._id)
        self.assertEqual("my_file", a.name)
        self.assertEqual("text/html", a.content_type)
        self.assertEqual(1000, a.chunk_size)
        self.assertEqual(["foo"], a.aliases)
        self.assertEqual({"foo": 1, "bar": 2}, a.metadata)
        self.assertEqual(3, a.bar)
        self.assertEqual("hello", a.baz)
        self.assertRaises(AttributeError, getattr, a, "mike")

        b = GridIn(self.db.fs, content_type="text/html", chunk_size=1000, baz=100)
        self.assertEqual("text/html", b.content_type)
        self.assertEqual(1000, b.chunk_size)
        self.assertEqual(100, b.baz)

    def test_grid_out_default_opts(self):
        self.assertRaises(TypeError, GridOut, "foo")

        self.assertRaises(NoFile, GridOut, self.db.fs, 5)

        a = GridIn(self.db.fs)
        a.close()

        b = GridOut(self.db.fs, a._id)

        self.assertEqual(a._id, b._id)
        self.assertEqual(0, b.length)
        self.assertEqual(None, b.content_type)
        self.assertEqual(256 * 1024, b.chunk_size)
        self.assert_(isinstance(b.upload_date, datetime.datetime))
        self.assertEqual(None, b.aliases)
        self.assertEqual(None, b.metadata)
        self.assertEqual("d41d8cd98f00b204e9800998ecf8427e", b.md5)

        for attr in ["_id", "name", "content_type", "length", "chunk_size",
                     "upload_date", "aliases", "metadata", "md5"]:
            self.assertRaises(AttributeError, setattr, b, attr, 5)

    def test_grid_out_custom_opts(self):
        a = GridIn(self.db.fs, _id=5, filename="my_file",
                   contentType="text/html", chunkSize=1000, aliases=["foo"],
                   metadata={"foo": 1, "bar": 2}, bar=3, baz="hello")
        a.write("hello world")
        a.close()

        b = GridOut(self.db.fs, 5)

        self.assertEqual(5, b._id)
        self.assertEqual(11, b.length)
        self.assertEqual("text/html", b.content_type)
        self.assertEqual(1000, b.chunk_size)
        self.assert_(isinstance(b.upload_date, datetime.datetime))
        self.assertEqual(["foo"], b.aliases)
        self.assertEqual({"foo": 1, "bar": 2}, b.metadata)
        self.assertEqual(3, b.bar)
        self.assertEqual("5eb63bbbe01eeed093cb22bb8f5acdc3", b.md5)

        for attr in ["_id", "name", "content_type", "length", "chunk_size",
                     "upload_date", "aliases", "metadata", "md5"]:
            self.assertRaises(AttributeError, setattr, b, attr, 5)

    def test_write_file_like(self):
        a = GridIn(self.db.fs)
        a.write("hello world")
        a.close()

        b = GridOut(self.db.fs, a._id)

        c = GridIn(self.db.fs)
        c.write(b)
        c.close()

        d = GridOut(self.db.fs, c._id)
        self.assertEqual("hello world", d.read())

        e = GridIn(self.db.fs, chunk_size=2)
        e.write("hello")
        buffer = StringIO(" world")
        e.write(buffer)
        e.write(" and mongodb")
        e.close()
        self.assertEqual("hello world and mongodb",
                         GridOut(self.db.fs, e._id).read())

    def test_write_lines(self):
        a = GridIn(self.db.fs)
        a.writelines(["hello ", "world"])
        a.close()

        self.assertEqual("hello world", GridOut(self.db.fs, a._id).read())

    def test_close(self):
        f = GridIn(self.db.fs)
        f.close()
        self.assertRaises(ValueError, f.write, "test")
        f.close()

    def test_multi_chunk_file(self):
        random_string = qcheck.gen_string(qcheck.lift(300000))()

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
        f.write("hello world")
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual("hello world", g.read())
        g.seek(0)
        self.assertEqual("hello world", g.read())
        g.seek(1)
        self.assertEqual("ello world", g.read())
        self.assertRaises(IOError, g.seek, -1)

        g.seek(-3, _SEEK_END)
        self.assertEqual("rld", g.read())
        g.seek(0, _SEEK_END)
        self.assertEqual("", g.read())
        self.assertRaises(IOError, g.seek, -100, _SEEK_END)

        g.seek(3)
        g.seek(3, _SEEK_CUR)
        self.assertEqual("world", g.read())
        self.assertRaises(IOError, g.seek, -100, _SEEK_CUR)

    def test_tell(self):
        f = GridIn(self.db.fs, chunkSize=3)
        f.write("hello world")
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
        f.write("hello world")
        f.close()

        g = GridOut(self.db.fs, f._id)
        self.assertEqual("he", g.read(2))
        self.assertEqual("ll", g.read(2))
        self.assertEqual("o ", g.read(2))
        self.assertEqual("wo", g.read(2))
        self.assertEqual("rl", g.read(2))
        self.assertEqual("d", g.read(2))
        self.assertEqual("", g.read(2))

    def test_iterator(self):
        f = GridIn(self.db.fs)
        f.close()
        g = GridOut(self.db.fs, f._id)
        self.assertEqual([], list(g))

        f = GridIn(self.db.fs)
        f.write("hello world")
        f.close()
        g = GridOut(self.db.fs, f._id)
        self.assertEqual(["hello world"], list(g))
        self.assertEqual("hello", g.read(5))
        self.assertEqual(["hello world"], list(g))
        self.assertEqual(" worl", g.read(5))

        f = GridIn(self.db.fs, chunk_size=2)
        f.write("hello world")
        f.close()
        g = GridOut(self.db.fs, f._id)
        self.assertEqual(["he", "ll", "o ", "wo", "rl", "d"], list(g))

    def test_read_chunks_unaligned_buffer_size(self):
        in_data = "This is a text that doesn't quite fit in a single 16-byte chunk."
        f = GridIn(self.db.fs, chunkSize=16)
        f.write(in_data)
        f.close()

        g = GridOut(self.db.fs, f._id)
        out_data = ''
        while 1:
            s = g.read(13)
            if not s:
                break
            out_data += s

        self.assertEqual(in_data, out_data)

if __name__ == "__main__":
    unittest.main()
