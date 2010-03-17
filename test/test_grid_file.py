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

import unittest
import datetime
import os
import sys
sys.path[0:0] = [""]

import qcheck
from test_connection import get_connection
from gridfs.grid_file import GridFile, _SEEK_END, _SEEK_CUR


class TestGridFile(unittest.TestCase):

    def setUp(self):
        self.db = get_connection().pymongo_test

    def test_basic(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        self.assertEqual(self.db.fs.files.find().count(), 0)
        self.assertEqual(self.db.fs.chunks.find().count(), 0)
        file = GridFile({"filename": "test"}, self.db, "w")
        file.write("hello world")
        file.close()

        self.assertEqual(self.db.fs.files.find().count(), 1)
        self.assertEqual(self.db.fs.chunks.find().count(), 1)

        file = GridFile({"filename": "test"}, self.db)
        self.assertEqual(file.read(), "hello world")
        file.close()

        # make sure it's still there...
        file = GridFile({"filename": "test"}, self.db)
        self.assertEqual(file.read(), "hello world")
        file.close()

        file = GridFile({"filename": "test"}, self.db, "w")
        file.close()

        self.assertEqual(self.db.fs.files.find().count(), 1)
        self.assertEqual(self.db.fs.chunks.find().count(), 0)

        file = GridFile({"filename": "test"}, self.db)
        self.assertEqual(file.read(), "")
        file.close()

    def test_md5(self):
        file = GridFile({"filename": "test"}, self.db, "w")
        file.write("hello world\n")
        file.close()

        file = GridFile({"filename": "test"}, self.db)
        self.assertEqual(file.md5, "6f5902ac237024bdd0c176cb93063dc4")
        file.close()

    def test_alternate_collection(self):
        self.db.pymongo_test.files.remove({})
        self.db.pymongo_test.chunks.remove({})

        self.assertEqual(self.db.pymongo_test.files.find().count(), 0)
        self.assertEqual(self.db.pymongo_test.chunks.find().count(), 0)
        file = GridFile({"filename": "test"}, self.db, "w",
                        collection="pymongo_test")
        file.write("hello world")
        file.close()

        self.assertEqual(self.db.pymongo_test.files.find().count(), 1)
        self.assertEqual(self.db.pymongo_test.chunks.find().count(), 1)

        file = GridFile({"filename": "test"}, self.db,
                        collection="pymongo_test")
        self.assertEqual(file.read(), "hello world")
        file.close()

        # test that md5 still works...
        self.assertEqual(file.md5, "5eb63bbbe01eeed093cb22bb8f5acdc3")

        # make sure it's still there...
        file = GridFile({"filename": "test"}, self.db,
                        collection="pymongo_test")
        self.assertEqual(file.read(), "hello world")
        file.close()

        file = GridFile({"filename": "test"}, self.db, "w",
                        collection="pymongo_test")
        file.close()

        self.assertEqual(self.db.pymongo_test.files.find().count(), 1)
        self.assertEqual(self.db.pymongo_test.chunks.find().count(), 0)

        file = GridFile({"filename": "test"}, self.db,
                        collection="pymongo_test")
        self.assertEqual(file.read(), "")
        file.close()

    def test_create_grid_file(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        # just write a blank file so that reads on {} don't fail
        file = GridFile({"filename": "test"}, self.db, "w")
        file.close()

        self.assertRaises(TypeError, GridFile, "hello", self.db)
        self.assertRaises(TypeError, GridFile, None, self.db)
        self.assertRaises(TypeError, GridFile, 5, self.db)

        self.assertRaises(TypeError, GridFile, {}, "hello")
        self.assertRaises(TypeError, GridFile, {}, None)
        self.assertRaises(TypeError, GridFile, {}, 5)
        GridFile({}, self.db).close()

        self.assertRaises(TypeError, GridFile, {}, self.db, None)
        self.assertRaises(TypeError, GridFile, {}, self.db, 5)
        self.assertRaises(TypeError, GridFile, {}, self.db, [])
        self.assertRaises(ValueError, GridFile, {}, self.db, "m")
        self.assertRaises(ValueError, GridFile, {}, self.db, u"m")
        GridFile({}, self.db, "r").close()
        GridFile({}, self.db, u"r").close()
        GridFile({}, self.db, "w").close()
        GridFile({}, self.db, u"w").close()

        self.assertRaises(TypeError, GridFile, {}, self.db, "r", None)
        self.assertRaises(TypeError, GridFile, {}, self.db, "r", 5)
        self.assertRaises(TypeError, GridFile, {}, self.db, "r", [])

        self.assertRaises(IOError, GridFile, {"filename": "mike"}, self.db)
        GridFile({"filename": "test"}, self.db).close()

    def test_properties(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test"}, self.db, "w")
        self.assertEqual(file.mode, "w")
        self.failIf(file.closed)
        file.close()
        self.assert_(file.closed)

        self.assertRaises(IOError, GridFile, {"filename": "mike"}, self.db)
        a = GridFile({"filename": "test"}, self.db)

        self.assertEqual(a.mode, "r")
        self.failIf(a.closed)

        self.assertEqual(a.length, 0)
        self.assertEqual(a.content_type, None)
        self.assertEqual(a.name, "test")
        self.assertEqual(a.chunk_size, 256000)
        self.assert_(isinstance(a.upload_date, datetime.datetime))
        self.assertEqual(a.aliases, None)
        self.assertEqual(a.metadata, None)
        self.assertEqual(a.md5, "d41d8cd98f00b204e9800998ecf8427e")

        a.content_type = "something"
        self.assertEqual(a.content_type, "something")

        def set_length():
            a.length = 10
        self.assertRaises(AttributeError, set_length)

        def set_chunk_size():
            a.chunk_size = 100
        self.assertRaises(AttributeError, set_chunk_size)

        def set_upload_date():
            a.upload_date = datetime.datetime.utcnow()
        self.assertRaises(AttributeError, set_upload_date)

        a.aliases = ["hello", "world"]
        self.assertEqual(a.aliases, ["hello", "world"])

        a.metadata = {"something": "else"}
        self.assertEqual(a.metadata, {"something": "else"})

        def set_name():
            a.name = "hello"
        self.assertRaises(AttributeError, set_name)

        def set_md5():
            a.md5 = "what"
        self.assertRaises(AttributeError, set_md5)

        a.close()

    def test_rename(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test"}, self.db, "w")
        file.close()

        self.assertRaises(IOError, GridFile, {"filename": "mike"}, self.db)
        a = GridFile({"filename": "test"}, self.db)

        a.rename("mike")
        self.assertEqual("mike", a.name)
        a.close()

        self.assertRaises(IOError, GridFile, {"filename": "test"}, self.db)
        GridFile({"filename": "mike"}, self.db).close()

    def test_flush_close(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test"}, self.db, "w")
        file.flush()
        file.close()
        file.close()
        self.assertRaises(ValueError, file.write, "test")

        file = GridFile({}, self.db)
        self.assertEqual(file.read(), "")
        file.close()

        file = GridFile({"filename": "test"}, self.db, "w")
        file.write("mike")
        file.flush()
        file.write("test")
        file.flush()
        file.write("huh")
        file.flush()
        file.flush()
        file.close()
        file.close()
        self.assertRaises(ValueError, file.write, "test")
        file = GridFile({}, self.db)
        self.assertEqual(file.read(), "miketesthuh")
        file.close()

    def test_overwrite(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test"}, self.db, "w")
        file.write("test")
        file.close()

        file = GridFile({"filename": "test"}, self.db, "w")
        file.write("mike")
        file.close()

        f = GridFile({}, self.db)
        self.assertEqual(f.read(), "mike")
        f.close()

    def test_multi_chunk_file(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        random_string = qcheck.gen_string(qcheck.lift(300000))()

        file = GridFile({"filename": "test"}, self.db, "w")
        file.write(random_string)
        file.close()

        self.assertEqual(self.db.fs.files.find().count(), 1)
        self.assertEqual(self.db.fs.chunks.find().count(), 2)

        f = GridFile({}, self.db)
        self.assertEqual(f.read(), random_string)
        f.close()

    def test_small_chunks(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        self.files = 0
        self.chunks = 0

        def helper(data):
            filename = qcheck.gen_printable_string(qcheck.lift(20))()

            f = GridFile({"filename": filename, "chunkSize": 1}, self.db, "w")
            f.write(data)
            f.close()

            self.files += 1
            self.chunks += len(data)

            self.assertEqual(self.db.fs.files.find().count(), self.files)
            self.assertEqual(self.db.fs.chunks.find().count(), self.chunks)

            f = GridFile({"filename": filename}, self.db)
            self.assertEqual(f.read(), data)
            f.close()

            f = GridFile({"filename": filename}, self.db)
            self.assertEqual(f.read(10) + f.read(10), data)
            f.close()
            return True

        qcheck.check_unittest(self, helper,
                              qcheck.gen_string(qcheck.gen_range(0, 20)))

    def test_seek(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test", "chunkSize": 3}, self.db, "w")
        file.write("hello world")
        self.assertRaises(ValueError, file.seek, 0)
        file.close()

        file = GridFile({"filename": "test"}, self.db, "r")
        self.assertEqual(file.read(), "hello world")
        file.seek(0)
        self.assertEqual(file.read(), "hello world")
        file.seek(1)
        self.assertEqual(file.read(), "ello world")
        self.assertRaises(IOError, file.seek, -1)

        file.seek(-3, _SEEK_END)
        self.assertEqual(file.read(), "rld")
        file.seek(0, _SEEK_END)
        self.assertEqual(file.read(), "")
        self.assertRaises(IOError, file.seek, -100, _SEEK_END)

        file.seek(3)
        file.seek(3, _SEEK_CUR)
        self.assertEqual(file.read(), "world")
        self.assertRaises(IOError, file.seek, -100, _SEEK_CUR)

        file.close()

    def test_tell(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test", "chunkSize": 3}, self.db, "w")
        file.write("hello world")
        self.assertRaises(ValueError, file.tell)
        file.close()

        file = GridFile({"filename": "test"}, self.db, "r")
        self.assertEqual(file.tell(), 0)
        file.read(0)
        self.assertEqual(file.tell(), 0)
        file.read(1)
        self.assertEqual(file.tell(), 1)
        file.read(2)
        self.assertEqual(file.tell(), 3)
        file.read()
        self.assertEqual(file.tell(), file.length)

        file.close()

    def test_modes(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test"}, self.db, "w")
        self.assertRaises(ValueError, file.read)
        file.write("hello")
        file.close()
        self.assertRaises(ValueError, file.read)
        self.assertRaises(ValueError, file.write, "hello")

        file = GridFile({"filename": "test"}, self.db, "r")
        self.assertRaises(ValueError, file.write, "hello")
        file.read()
        file.close()
        self.assertRaises(ValueError, file.read)
        self.assertRaises(ValueError, file.write, "hello")

    def test_multiple_reads(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        file = GridFile({"filename": "test"}, self.db, "w")
        file.write("hello world")
        file.close()

        file = GridFile({"filename": "test"}, self.db, "r")
        self.assertEqual(file.read(2), "he")
        self.assertEqual(file.read(2), "ll")
        self.assertEqual(file.read(2), "o ")
        self.assertEqual(file.read(2), "wo")
        self.assertEqual(file.read(2), "rl")
        self.assertEqual(file.read(2), "d")
        self.assertEqual(file.read(2), "")
        file.close()

    def test_spec_with_id(self):
        # This was raising a TypeError at one point - make sure it doesn't
        file = GridFile({"_id": "foobar", "filename": "foobar"}, self.db, "w")
        file.close()

    def test_read_chunks_unaligned_buffer_size(self):
        self.db.fs.files.remove({})
        self.db.fs.chunks.remove({})

        in_data = "This is a text that doesn't quite fit in a single 16-byte chunk."
        f = GridFile({"filename":"test", "chunkSize":16}, self.db, "w")
        f.write(in_data)
        f.close()

        f = GridFile({"filename":"test"}, self.db)
        out_data = ''
        while 1:
            s = f.read(13)
            if not s:
                break
            out_data += s
        f.close()

        self.assertEqual(in_data, out_data)

    def test_id(self):
        file = GridFile({"_id": "test"}, self.db, "w")
        self.assertEqual("test", file._id)


if __name__ == "__main__":
    unittest.main()
