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

"""Tests for the gridfs package.
"""
import datetime
import sys
import threading
import time
import unittest
import warnings

sys.path[0:0] = [""]

import gridfs

from bson.binary import Binary
from bson.py3compat import b, StringIO
from gridfs.errors import CorruptGridFile, FileExists, NoFile
from pymongo.errors import ConnectionFailure
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference
from test import skip_restricted_localhost
from test.test_client import get_client
from test.test_replica_set_client import TestReplicaSetClientBase
from test.utils import catch_warnings, joinall


setUpModule = skip_restricted_localhost


class JustWrite(threading.Thread):

    def __init__(self, fs, n):
        threading.Thread.__init__(self)
        self.fs = fs
        self.n = n
        self.setDaemon(True)

    def run(self):
        for _ in range(self.n):
            file = self.fs.new_file(filename="test")
            file.write(b("hello"))
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
            assert data == b("hello")


class TestGridfs(unittest.TestCase):

    def setUp(self):
        self.db = get_client().pymongo_test
        self.db.drop_collection("fs.files")
        self.db.drop_collection("fs.chunks")
        self.db.drop_collection("alt.files")
        self.db.drop_collection("alt.chunks")
        self.fs = gridfs.GridFS(self.db)
        self.alt = gridfs.GridFS(self.db, "alt")

    def tearDown(self):
        self.db = self.fs = self.alt = None

    def test_gridfs(self):
        self.assertRaises(TypeError, gridfs.GridFS, "foo")
        self.assertRaises(TypeError, gridfs.GridFS, self.db, 5)

    def test_basic(self):
        oid = self.fs.put(b("hello world"))
        self.assertEqual(b("hello world"), self.fs.get(oid).read())
        self.assertEqual(1, self.db.fs.files.count())
        self.assertEqual(1, self.db.fs.chunks.count())

        self.fs.delete(oid)
        self.assertRaises(NoFile, self.fs.get, oid)
        self.assertEqual(0, self.db.fs.files.count())
        self.assertEqual(0, self.db.fs.chunks.count())

        self.assertRaises(NoFile, self.fs.get, "foo")
        oid = self.fs.put(b("hello world"), _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual(b("hello world"), self.fs.get("foo").read())

    def test_list(self):
        self.assertEqual([], self.fs.list())
        self.fs.put(b("hello world"))
        self.assertEqual([], self.fs.list())

        # PYTHON-598: in server versions before 2.5.x, creating an index on
        # filename, uploadDate causes list() to include None.
        self.fs.get_last_version()
        self.assertEqual([], self.fs.list())

        self.fs.put(b(""), filename="mike")
        self.fs.put(b("foo"), filename="test")
        self.fs.put(b(""), filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]),
                         set(self.fs.list()))

    def test_empty_file(self):
        oid = self.fs.put(b(""))
        self.assertEqual(b(""), self.fs.get(oid).read())
        self.assertEqual(1, self.db.fs.files.count())
        self.assertEqual(0, self.db.fs.chunks.count())

        raw = self.db.fs.files.find_one()
        self.assertEqual(0, raw["length"])
        self.assertEqual(oid, raw["_id"])
        self.assertTrue(isinstance(raw["uploadDate"], datetime.datetime))
        self.assertEqual(255 * 1024, raw["chunkSize"])
        self.assertTrue(isinstance(raw["md5"], basestring))

    def test_corrupt_chunk(self):
        files_id = self.fs.put(b('foobar'))
        self.db.fs.chunks.update({'files_id': files_id},
                                 {'$set': {'data': Binary(b('foo'), 0)}})
        try:
            out = self.fs.get(files_id)
            self.assertRaises(CorruptGridFile, out.read)

            out = self.fs.get(files_id)
            self.assertRaises(CorruptGridFile, out.readline)
        finally:
            self.fs.delete(files_id)

    def test_alt_collection(self):
        oid = self.alt.put(b("hello world"))
        self.assertEqual(b("hello world"), self.alt.get(oid).read())
        self.assertEqual(1, self.db.alt.files.count())
        self.assertEqual(1, self.db.alt.chunks.count())

        self.alt.delete(oid)
        self.assertRaises(NoFile, self.alt.get, oid)
        self.assertEqual(0, self.db.alt.files.count())
        self.assertEqual(0, self.db.alt.chunks.count())

        self.assertRaises(NoFile, self.alt.get, "foo")
        oid = self.alt.put(b("hello world"), _id="foo")
        self.assertEqual("foo", oid)
        self.assertEqual(b("hello world"), self.alt.get("foo").read())

        self.alt.put(b(""), filename="mike")
        self.alt.put(b("foo"), filename="test")
        self.alt.put(b(""), filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]),
                         set(self.alt.list()))

    def test_threaded_reads(self):
        self.fs.put(b("hello"), _id="test")

        threads = []
        results = []
        for i in range(10):
            threads.append(JustRead(self.fs, 10, results))
            threads[i].start()

        joinall(threads)

        self.assertEqual(
            100 * [b('hello')],
            results
        )

    def test_threaded_writes(self):
        threads = []
        for i in range(10):
            threads.append(JustWrite(self.fs, 10))
            threads[i].start()

        joinall(threads)

        f = self.fs.get_last_version("test")
        self.assertEqual(f.read(), b("hello"))

        # Should have created 100 versions of 'test' file
        self.assertEqual(
            100,
            self.db.fs.files.find({'filename':'test'}).count()
        )

    def test_get_last_version(self):
        one = self.fs.put(b("foo"), filename="test")
        time.sleep(0.01)
        two = self.fs.new_file(filename="test")
        two.write(b("bar"))
        two.close()
        time.sleep(0.01)
        two = two._id
        three = self.fs.put(b("baz"), filename="test")

        self.assertEqual(b("baz"), self.fs.get_last_version("test").read())
        self.fs.delete(three)
        self.assertEqual(b("bar"), self.fs.get_last_version("test").read())
        self.fs.delete(two)
        self.assertEqual(b("foo"), self.fs.get_last_version("test").read())
        self.fs.delete(one)
        self.assertRaises(NoFile, self.fs.get_last_version, "test")

    def test_get_last_version_with_metadata(self):
        one = self.fs.put(b("foo"), filename="test", author="author")
        time.sleep(0.01)
        two = self.fs.put(b("bar"), filename="test", author="author")

        self.assertEqual(b("bar"), self.fs.get_last_version(author="author").read())
        self.fs.delete(two)
        self.assertEqual(b("foo"), self.fs.get_last_version(author="author").read())
        self.fs.delete(one)

        one = self.fs.put(b("foo"), filename="test", author="author1")
        time.sleep(0.01)
        two = self.fs.put(b("bar"), filename="test", author="author2")

        self.assertEqual(b("foo"), self.fs.get_last_version(author="author1").read())
        self.assertEqual(b("bar"), self.fs.get_last_version(author="author2").read())
        self.assertEqual(b("bar"), self.fs.get_last_version(filename="test").read())

        self.assertRaises(NoFile, self.fs.get_last_version, author="author3")
        self.assertRaises(NoFile, self.fs.get_last_version, filename="nottest", author="author1")

        self.fs.delete(one)
        self.fs.delete(two)

    def test_get_version(self):
        self.fs.put(b("foo"), filename="test")
        time.sleep(0.01)
        self.fs.put(b("bar"), filename="test")
        time.sleep(0.01)
        self.fs.put(b("baz"), filename="test")
        time.sleep(0.01)

        self.assertEqual(b("foo"), self.fs.get_version("test", 0).read())
        self.assertEqual(b("bar"), self.fs.get_version("test", 1).read())
        self.assertEqual(b("baz"), self.fs.get_version("test", 2).read())

        self.assertEqual(b("baz"), self.fs.get_version("test", -1).read())
        self.assertEqual(b("bar"), self.fs.get_version("test", -2).read())
        self.assertEqual(b("foo"), self.fs.get_version("test", -3).read())

        self.assertRaises(NoFile, self.fs.get_version, "test", 3)
        self.assertRaises(NoFile, self.fs.get_version, "test", -4)

    def test_get_version_with_metadata(self):
        one = self.fs.put(b("foo"), filename="test", author="author1")
        time.sleep(0.01)
        two = self.fs.put(b("bar"), filename="test", author="author1")
        time.sleep(0.01)
        three = self.fs.put(b("baz"), filename="test", author="author2")

        self.assertEqual(b("foo"), self.fs.get_version(filename="test", author="author1", version=-2).read())
        self.assertEqual(b("bar"), self.fs.get_version(filename="test", author="author1", version=-1).read())
        self.assertEqual(b("foo"), self.fs.get_version(filename="test", author="author1", version=0).read())
        self.assertEqual(b("bar"), self.fs.get_version(filename="test", author="author1", version=1).read())
        self.assertEqual(b("baz"), self.fs.get_version(filename="test", author="author2", version=0).read())
        self.assertEqual(b("baz"), self.fs.get_version(filename="test", version=-1).read())
        self.assertEqual(b("baz"), self.fs.get_version(filename="test", version=2).read())

        self.assertRaises(NoFile, self.fs.get_version, filename="test", author="author3")
        self.assertRaises(NoFile, self.fs.get_version, filename="test", author="author1", version=2)

        self.fs.delete(one)
        self.fs.delete(two)
        self.fs.delete(three)

    def test_put_filelike(self):
        oid = self.fs.put(StringIO(b("hello world")), chunk_size=1)
        self.assertEqual(11, self.db.fs.chunks.count())
        self.assertEqual(b("hello world"), self.fs.get(oid).read())

    def test_file_exists(self):
        db = get_client(w=1).pymongo_test
        fs = gridfs.GridFS(db)

        oid = fs.put(b("hello"))
        self.assertRaises(FileExists, fs.put, b("world"), _id=oid)

        one = fs.new_file(_id=123)
        one.write(b("some content"))
        one.close()

        two = fs.new_file(_id=123)
        self.assertRaises(FileExists, two.write, b('x' * 262146))

    def test_exists(self):
        oid = self.fs.put(b("hello"))
        self.assertTrue(self.fs.exists(oid))
        self.assertTrue(self.fs.exists({"_id": oid}))
        self.assertTrue(self.fs.exists(_id=oid))

        self.assertFalse(self.fs.exists(filename="mike"))
        self.assertFalse(self.fs.exists("mike"))

        oid = self.fs.put(b("hello"), filename="mike", foo=12)
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
        self.assertRaises(TypeError, self.fs.put, u"hello")

        oid = self.fs.put(u"hello", encoding="utf-8")
        self.assertEqual(b("hello"), self.fs.get(oid).read())
        self.assertEqual("utf-8", self.fs.get(oid).encoding)

        oid = self.fs.put(u"aé", encoding="iso-8859-1")
        self.assertEqual(u"aé".encode("iso-8859-1"), self.fs.get(oid).read())
        self.assertEqual("iso-8859-1", self.fs.get(oid).encoding)

    def test_missing_length_iter(self):
        # Test fix that guards against PHP-237
        self.fs.put(b(""), filename="empty")
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
            file.write(b("hello"))
            file.close()

        c.end_request()

        self.assertEqual(
            n,
            self.db.fs.files.find({'filename':'test'}).count()
        )

    def test_gridfs_request(self):
        self.assertFalse(self.db.connection.in_request())
        self.fs.put(b("hello world"))
        # Request started and ended by put(), we're back to original state
        self.assertFalse(self.db.connection.in_request())

    def test_gridfs_lazy_connect(self):
        client = MongoClient('badhost', _connect=False)
        db = client.db
        self.assertRaises(ConnectionFailure, gridfs.GridFS, db)

        fs = gridfs.GridFS(db, _connect=False)
        f = fs.new_file()  # Still no connection.
        self.assertRaises(ConnectionFailure, f.close)

    def test_gridfs_find(self):
        self.fs.put(b("test2"), filename="two")
        time.sleep(0.01)
        self.fs.put(b("test2+"), filename="two")
        time.sleep(0.01)
        self.fs.put(b("test1"), filename="one")
        time.sleep(0.01)
        self.fs.put(b("test2++"), filename="two")
        self.assertEqual(3, self.fs.find({"filename":"two"}).count())
        self.assertEqual(4, self.fs.find().count())
        cursor = self.fs.find(timeout=False).sort("uploadDate", -1).skip(1).limit(2)
        # 2to3 hint...
        gout = cursor.next()
        self.assertEqual(b("test1"), gout.read())
        cursor.rewind()
        gout = cursor.next()
        self.assertEqual(b("test1"), gout.read())
        gout = cursor.next()
        self.assertEqual(b("test2+"), gout.read())
        self.assertRaises(StopIteration, cursor.next)
        cursor.close()
        self.assertRaises(TypeError, self.fs.find, {}, {"_id": True})

    def test_gridfs_backport(self):
        # Test "filter" works and has precedence over "spec".
        self.fs.put(b("test2"), filename="two")
        time.sleep(0.01)
        self.fs.put(b("test2+"), filename="two")
        time.sleep(0.01)
        self.fs.put(b("test1"), filename="one")
        time.sleep(0.01)
        self.fs.put(b("test2++"), filename="two")
        fn1 = self.fs.find(spec={"filename":"two"}).count()
        fn2 = self.fs.find(filter={"filename":"two"}).count()

        fn3 = self.fs.find(spec={"filename":"one"},
                           filter={"filename":"two"}).count()
        self.assertEqual(3, fn1)
        self.assertEqual(fn1, fn2)
        self.assertEqual(fn2, fn3)

        # Test "no_cursor_timeout" doesn't break anything.
        cursor = self.fs.find(no_cursor_timeout=True).sort(
            "uploadDate", -1).skip(1).limit(2)
        # 2to3 hint...
        gout = cursor.next()
        self.assertEqual(b("test1"), gout.read())
        cursor.rewind()
        gout = cursor.next()
        self.assertEqual(b("test1"), gout.read())
        gout = cursor.next()
        self.assertEqual(b("test2+"), gout.read())
        self.assertRaises(StopIteration, cursor.next)
        cursor.close()
        self.assertRaises(TypeError, self.fs.find, {}, {"_id": True})


    def test_gridfs_find_one(self):
        self.assertEqual(None, self.fs.find_one())

        id1 = self.fs.put(b('test1'), filename='file1')
        self.assertEqual(b('test1'), self.fs.find_one().read())

        id2 = self.fs.put(b('test2'), filename='file2', meta='data')
        self.assertEqual(b('test1'), self.fs.find_one(id1).read())
        self.assertEqual(b('test2'), self.fs.find_one(id2).read())

        self.assertEqual(b('test1'),
                         self.fs.find_one({'filename': 'file1'}).read())

        self.assertEqual('data', self.fs.find_one(id2).meta)

    def test_grid_in_non_int_chunksize(self):
        # Lua, and perhaps other buggy GridFS clients, store size as a float.
        data = b('data')
        self.fs.put(data, filename='f')
        self.db.fs.files.update({'filename': 'f'},
                                {'$set': {'chunkSize': 100.0}})

        self.assertEqual(data, self.fs.get_version('f').read())


class TestGridfsReplicaSet(TestReplicaSetClientBase):
    def test_gridfs_replica_set(self):
        rsc = self._get_client(
            w=self.w, wtimeout=30000,
            read_preference=ReadPreference.SECONDARY)

        try:
            fs = gridfs.GridFS(rsc.pymongo_test)
            oid = fs.put(b('foo'))
            content = fs.get(oid).read()
            self.assertEqual(b('foo'), content)
        finally:
            rsc.close()

    def test_gridfs_secondary(self):
        primary_host, primary_port = self.primary
        primary_connection = MongoClient(primary_host, primary_port)

        secondary_host, secondary_port = self.secondaries[0]
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            for secondary_connection in [
                MongoClient(secondary_host, secondary_port, slave_okay=True),
                MongoClient(secondary_host, secondary_port,
                            read_preference=ReadPreference.SECONDARY),
            ]:
                primary_connection.pymongo_test.drop_collection("fs.files")
                primary_connection.pymongo_test.drop_collection("fs.chunks")

                # Should detect it's connected to secondary and not attempt to
                # create index
                fs = gridfs.GridFS(secondary_connection.pymongo_test)

                # This won't detect secondary, raises error
                self.assertRaises(ConnectionFailure, fs.put, b('foo'))
        finally:
            ctx.exit()

    def test_gridfs_secondary_lazy(self):
        # Should detect it's connected to secondary and not attempt to
        # create index.
        secondary_host, secondary_port = self.secondaries[0]
        client = MongoClient(
            secondary_host, secondary_port,
            read_preference=ReadPreference.SECONDARY,
            _connect=False)

        # Still no connection.
        fs = gridfs.GridFS(client.test_gridfs_secondary_lazy, _connect=False)

        # Connects, doesn't create index.
        self.assertRaises(NoFile, fs.get_last_version)
        self.assertRaises(ConnectionFailure, fs.put, 'data')

    def tearDown(self):
        rsc = self._get_client()
        rsc.pymongo_test.drop_collection('fs.files')
        rsc.pymongo_test.drop_collection('fs.chunks')
        rsc.close()


if __name__ == "__main__":
    unittest.main()
