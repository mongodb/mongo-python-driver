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

"""Test the bson module."""

import collections
import datetime
import os
import re
import sys
import tempfile
import uuid

sys.path[0:0] = [""]

import bson
from bson import (BSON,
                  decode,
                  decode_all,
                  decode_file_iter,
                  decode_iter,
                  encode,
                  EPOCH_AWARE,
                  is_valid,
                  Regex)
from bson.binary import Binary, UUIDLegacy
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.dbref import DBRef
from bson.py3compat import abc, iteritems, PY3, StringIO, text_type
from bson.son import SON
from bson.timestamp import Timestamp
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.tz_util import (FixedOffset,
                          utc)

from test import qcheck, SkipTest, unittest
from test.utils import ExceptionCatchingThread

if PY3:
    long = int


class NotADict(abc.MutableMapping):
    """Non-dict type that implements the mapping protocol."""

    def __init__(self, initial=None):
        if not initial:
            self._dict = {}
        else:
            self._dict = initial

    def __iter__(self):
        return iter(self._dict)

    def __getitem__(self, item):
        return self._dict[item]

    def __delitem__(self, item):
        del self._dict[item]

    def __setitem__(self, item, value):
        self._dict[item] = value

    def __len__(self):
        return len(self._dict)

    def __eq__(self, other):
        if isinstance(other, abc.Mapping):
            return all(self.get(k) == other.get(k) for k in self)
        return NotImplemented

    def __repr__(self):
        return "NotADict(%s)" % repr(self._dict)


class DSTAwareTimezone(datetime.tzinfo):

    def __init__(self, offset, name, dst_start_month, dst_end_month):
        self.__offset = offset
        self.__dst_start_month = dst_start_month
        self.__dst_end_month = dst_end_month
        self.__name = name

    def _is_dst(self, dt):
        return self.__dst_start_month <= dt.month <= self.__dst_end_month

    def utcoffset(self, dt):
        return datetime.timedelta(minutes=self.__offset) + self.dst(dt)

    def dst(self, dt):
        if self._is_dst(dt):
            return datetime.timedelta(hours=1)
        return datetime.timedelta(0)

    def tzname(self, dt):
        return self.__name


class TestBSON(unittest.TestCase):
    def assertInvalid(self, data):
        self.assertRaises(InvalidBSON, decode, data)

    def check_encode_then_decode(self, doc_class=dict, decoder=decode,
                                 encoder=encode):

        # Work around http://bugs.jython.org/issue1728
        if sys.platform.startswith('java'):
            doc_class = SON

        def helper(doc):
            self.assertEqual(doc, (decoder(encoder(doc_class(doc)))))
            self.assertEqual(doc, decoder(encoder(doc)))

        helper({})
        helper({"test": u"hello"})
        self.assertTrue(isinstance(decoder(encoder(
            {"hello": "world"}))["hello"], text_type))
        helper({"mike": -10120})
        helper({"long": Int64(10)})
        helper({"really big long": 2147483648})
        helper({u"hello": 0.0013109})
        helper({"something": True})
        helper({"false": False})
        helper({"an array": [1, True, 3.8, u"world"]})
        helper({"an object": doc_class({"test": u"something"})})
        helper({"a binary": Binary(b"test", 100)})
        helper({"a binary": Binary(b"test", 128)})
        helper({"a binary": Binary(b"test", 254)})
        helper({"another binary": Binary(b"test", 2)})
        helper(SON([(u'test dst', datetime.datetime(1993, 4, 4, 2))]))
        helper(SON([(u'test negative dst',
                     datetime.datetime(1, 1, 1, 1, 1, 1))]))
        helper({"big float": float(10000000000)})
        helper({"ref": DBRef("coll", 5)})
        helper({"ref": DBRef("coll", 5, foo="bar", bar=4)})
        helper({"ref": DBRef("coll", 5, "foo")})
        helper({"ref": DBRef("coll", 5, "foo", foo="bar")})
        helper({"ref": Timestamp(1, 2)})
        helper({"foo": MinKey()})
        helper({"foo": MaxKey()})
        helper({"$field": Code("function(){ return true; }")})
        helper({"$field": Code("return function(){ return x; }", scope={'x': False})})

        def encode_then_decode(doc):
            return doc_class(doc) == decoder(encode(doc), CodecOptions(
                document_class=doc_class))

        qcheck.check_unittest(self, encode_then_decode,
                              qcheck.gen_mongo_dict(3))

    def test_encode_then_decode(self):
        self.check_encode_then_decode()

    def test_encode_then_decode_any_mapping(self):
        self.check_encode_then_decode(doc_class=NotADict)

    def test_encode_then_decode_legacy(self):
        self.check_encode_then_decode(
            encoder=BSON.encode,
            decoder=lambda *args: BSON(args[0]).decode(*args[1:]))

    def test_encode_then_decode_any_mapping_legacy(self):
        self.check_encode_then_decode(
            doc_class=NotADict, encoder=BSON.encode,
            decoder=lambda *args: BSON(args[0]).decode(*args[1:]))

    def test_encoding_defaultdict(self):
        dct = collections.defaultdict(dict, [('foo', 'bar')])
        encode(dct)
        self.assertEqual(dct, collections.defaultdict(dict, [('foo', 'bar')]))

    def test_basic_validation(self):
        self.assertRaises(TypeError, is_valid, 100)
        self.assertRaises(TypeError, is_valid, u"test")
        self.assertRaises(TypeError, is_valid, 10.4)

        self.assertInvalid(b"test")

        # the simplest valid BSON document
        self.assertTrue(is_valid(b"\x05\x00\x00\x00\x00"))
        self.assertTrue(is_valid(BSON(b"\x05\x00\x00\x00\x00")))

        # failure cases
        self.assertInvalid(b"\x04\x00\x00\x00\x00")
        self.assertInvalid(b"\x05\x00\x00\x00\x01")
        self.assertInvalid(b"\x05\x00\x00\x00")
        self.assertInvalid(b"\x05\x00\x00\x00\x00\x00")
        self.assertInvalid(b"\x07\x00\x00\x00\x02a\x00\x78\x56\x34\x12")
        self.assertInvalid(b"\x09\x00\x00\x00\x10a\x00\x05\x00")
        self.assertInvalid(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
        self.assertInvalid(b"\x13\x00\x00\x00\x02foo\x00"
                           b"\x04\x00\x00\x00bar\x00\x00")
        self.assertInvalid(b"\x18\x00\x00\x00\x03foo\x00\x0f\x00\x00"
                           b"\x00\x10bar\x00\xff\xff\xff\x7f\x00\x00")
        self.assertInvalid(b"\x15\x00\x00\x00\x03foo\x00\x0c"
                           b"\x00\x00\x00\x08bar\x00\x01\x00\x00")
        self.assertInvalid(b"\x1c\x00\x00\x00\x03foo\x00"
                           b"\x12\x00\x00\x00\x02bar\x00"
                           b"\x05\x00\x00\x00baz\x00\x00\x00")
        self.assertInvalid(b"\x10\x00\x00\x00\x02a\x00"
                           b"\x04\x00\x00\x00abc\xff\x00")

    def test_bad_string_lengths(self):
        self.assertInvalid(
            b"\x0c\x00\x00\x00\x02\x00"
            b"\x00\x00\x00\x00\x00\x00")
        self.assertInvalid(
            b"\x12\x00\x00\x00\x02\x00"
            b"\xff\xff\xff\xfffoobar\x00\x00")
        self.assertInvalid(
            b"\x0c\x00\x00\x00\x0e\x00"
            b"\x00\x00\x00\x00\x00\x00")
        self.assertInvalid(
            b"\x12\x00\x00\x00\x0e\x00"
            b"\xff\xff\xff\xfffoobar\x00\x00")
        self.assertInvalid(
            b"\x18\x00\x00\x00\x0c\x00"
            b"\x00\x00\x00\x00\x00RY\xb5j"
            b"\xfa[\xd8A\xd6X]\x99\x00")
        self.assertInvalid(
            b"\x1e\x00\x00\x00\x0c\x00"
            b"\xff\xff\xff\xfffoobar\x00"
            b"RY\xb5j\xfa[\xd8A\xd6X]\x99\x00")
        self.assertInvalid(
            b"\x0c\x00\x00\x00\r\x00"
            b"\x00\x00\x00\x00\x00\x00")
        self.assertInvalid(
            b"\x0c\x00\x00\x00\r\x00"
            b"\xff\xff\xff\xff\x00\x00")
        self.assertInvalid(
            b"\x1c\x00\x00\x00\x0f\x00"
            b"\x15\x00\x00\x00\x00\x00"
            b"\x00\x00\x00\x0c\x00\x00"
            b"\x00\x02\x00\x01\x00\x00"
            b"\x00\x00\x00\x00")
        self.assertInvalid(
            b"\x1c\x00\x00\x00\x0f\x00"
            b"\x15\x00\x00\x00\xff\xff"
            b"\xff\xff\x00\x0c\x00\x00"
            b"\x00\x02\x00\x01\x00\x00"
            b"\x00\x00\x00\x00")
        self.assertInvalid(
            b"\x1c\x00\x00\x00\x0f\x00"
            b"\x15\x00\x00\x00\x01\x00"
            b"\x00\x00\x00\x0c\x00\x00"
            b"\x00\x02\x00\x00\x00\x00"
            b"\x00\x00\x00\x00")
        self.assertInvalid(
            b"\x1c\x00\x00\x00\x0f\x00"
            b"\x15\x00\x00\x00\x01\x00"
            b"\x00\x00\x00\x0c\x00\x00"
            b"\x00\x02\x00\xff\xff\xff"
            b"\xff\x00\x00\x00")

    def test_random_data_is_not_bson(self):
        qcheck.check_unittest(self, qcheck.isnt(is_valid),
                              qcheck.gen_string(qcheck.gen_range(0, 40)))

    def test_basic_decode(self):
        self.assertEqual({"test": u"hello world"},
                         decode(b"\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C"
                                b"\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F"
                                b"\x72\x6C\x64\x00\x00"))
        self.assertEqual([{"test": u"hello world"}, {}],
                         decode_all(b"\x1B\x00\x00\x00\x0E\x74\x65\x73\x74"
                                    b"\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C"
                                    b"\x6f\x20\x77\x6F\x72\x6C\x64\x00\x00"
                                    b"\x05\x00\x00\x00\x00"))
        self.assertEqual([{"test": u"hello world"}, {}],
                         list(decode_iter(
                            b"\x1B\x00\x00\x00\x0E\x74\x65\x73\x74"
                            b"\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C"
                            b"\x6f\x20\x77\x6F\x72\x6C\x64\x00\x00"
                            b"\x05\x00\x00\x00\x00")))
        self.assertEqual([{"test": u"hello world"}, {}],
                         list(decode_file_iter(StringIO(
                            b"\x1B\x00\x00\x00\x0E\x74\x65\x73\x74"
                            b"\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C"
                            b"\x6f\x20\x77\x6F\x72\x6C\x64\x00\x00"
                            b"\x05\x00\x00\x00\x00"))))

    def test_decode_all_buffer_protocol(self):
        docs = [{'foo': 'bar'}, {}]
        bs = b"".join(map(encode, docs))
        self.assertEqual(docs, decode_all(bytearray(bs)))
        self.assertEqual(docs, decode_all(memoryview(bs)))
        self.assertEqual(docs, decode_all(memoryview(b'1' + bs + b'1')[1:-1]))
        if PY3:
            import array
            import mmap
            self.assertEqual(docs, decode_all(array.array('B', bs)))
            with mmap.mmap(-1, len(bs)) as mm:
                mm.write(bs)
                mm.seek(0)
                self.assertEqual(docs, decode_all(mm))

    def test_decode_buffer_protocol(self):
        doc = {'foo': 'bar'}
        bs = encode(doc)
        self.assertEqual(doc, decode(bs))
        self.assertEqual(doc, decode(bytearray(bs)))
        self.assertEqual(doc, decode(memoryview(bs)))
        self.assertEqual(doc, decode(memoryview(b'1' + bs + b'1')[1:-1]))
        if PY3:
            import array
            import mmap
            self.assertEqual(doc, decode(array.array('B', bs)))
            with mmap.mmap(-1, len(bs)) as mm:
                mm.write(bs)
                mm.seek(0)
                self.assertEqual(doc, decode(mm))

    def test_invalid_decodes(self):
        # Invalid object size (not enough bytes in document for even
        # an object size of first object.
        # NOTE: decode_all and decode_iter don't care, not sure if they should?
        self.assertRaises(InvalidBSON, list,
                          decode_file_iter(StringIO(b"\x1B")))

        bad_bsons = [
            # An object size that's too small to even include the object size,
            # but is correctly encoded, along with a correct EOO (and no data).
            b"\x01\x00\x00\x00\x00",
            # One object, but with object size listed smaller than it is in the
            # data.
            (b"\x1A\x00\x00\x00\x0E\x74\x65\x73\x74"
             b"\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C"
             b"\x6f\x20\x77\x6F\x72\x6C\x64\x00\x00"
             b"\x05\x00\x00\x00\x00"),
            # One object, missing the EOO at the end.
            (b"\x1B\x00\x00\x00\x0E\x74\x65\x73\x74"
             b"\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C"
             b"\x6f\x20\x77\x6F\x72\x6C\x64\x00\x00"
             b"\x05\x00\x00\x00"),
            # One object, sized correctly, with a spot for an EOO, but the EOO
            # isn't 0x00.
            (b"\x1B\x00\x00\x00\x0E\x74\x65\x73\x74"
             b"\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C"
             b"\x6f\x20\x77\x6F\x72\x6C\x64\x00\x00"
             b"\x05\x00\x00\x00\xFF"),
        ]
        for i, data in enumerate(bad_bsons):
            msg = "bad_bson[{}]".format(i)
            with self.assertRaises(InvalidBSON, msg=msg):
                decode_all(data)
            with self.assertRaises(InvalidBSON, msg=msg):
                list(decode_iter(data))
            with self.assertRaises(InvalidBSON, msg=msg):
                list(decode_file_iter(StringIO(data)))
            with tempfile.TemporaryFile() as scratch:
                scratch.write(data)
                scratch.seek(0, os.SEEK_SET)
                with self.assertRaises(InvalidBSON, msg=msg):
                    list(decode_file_iter(scratch))

    def test_data_timestamp(self):
        self.assertEqual({"test": Timestamp(4, 20)},
                         decode(b"\x13\x00\x00\x00\x11\x74\x65\x73\x74\x00\x14"
                                b"\x00\x00\x00\x04\x00\x00\x00\x00"))

    def test_basic_encode(self):
        self.assertRaises(TypeError, encode, 100)
        self.assertRaises(TypeError, encode, "hello")
        self.assertRaises(TypeError, encode, None)
        self.assertRaises(TypeError, encode, [])

        self.assertEqual(encode({}), BSON(b"\x05\x00\x00\x00\x00"))
        self.assertEqual(encode({}), b"\x05\x00\x00\x00\x00")
        self.assertEqual(encode({"test": u"hello world"}),
                         b"\x1B\x00\x00\x00\x02\x74\x65\x73\x74\x00\x0C\x00"
                         b"\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C"
                         b"\x64\x00\x00")
        self.assertEqual(encode({u"mike": 100}),
                         b"\x0F\x00\x00\x00\x10\x6D\x69\x6B\x65\x00\x64\x00"
                         b"\x00\x00\x00")
        self.assertEqual(encode({"hello": 1.5}),
                         b"\x14\x00\x00\x00\x01\x68\x65\x6C\x6C\x6F\x00\x00"
                         b"\x00\x00\x00\x00\x00\xF8\x3F\x00")
        self.assertEqual(encode({"true": True}),
                         b"\x0C\x00\x00\x00\x08\x74\x72\x75\x65\x00\x01\x00")
        self.assertEqual(encode({"false": False}),
                         b"\x0D\x00\x00\x00\x08\x66\x61\x6C\x73\x65\x00\x00"
                         b"\x00")
        self.assertEqual(encode({"empty": []}),
                         b"\x11\x00\x00\x00\x04\x65\x6D\x70\x74\x79\x00\x05"
                         b"\x00\x00\x00\x00\x00")
        self.assertEqual(encode({"none": {}}),
                         b"\x10\x00\x00\x00\x03\x6E\x6F\x6E\x65\x00\x05\x00"
                         b"\x00\x00\x00\x00")
        self.assertEqual(encode({"test": Binary(b"test", 0)}),
                         b"\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00"
                         b"\x00\x00\x00\x74\x65\x73\x74\x00")
        self.assertEqual(encode({"test": Binary(b"test", 2)}),
                         b"\x18\x00\x00\x00\x05\x74\x65\x73\x74\x00\x08\x00"
                         b"\x00\x00\x02\x04\x00\x00\x00\x74\x65\x73\x74\x00")
        self.assertEqual(encode({"test": Binary(b"test", 128)}),
                         b"\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00"
                         b"\x00\x00\x80\x74\x65\x73\x74\x00")
        self.assertEqual(encode({"test": None}),
                         b"\x0B\x00\x00\x00\x0A\x74\x65\x73\x74\x00\x00")
        self.assertEqual(encode({"date": datetime.datetime(2007, 1, 8,
                                                                0, 30, 11)}),
                         b"\x13\x00\x00\x00\x09\x64\x61\x74\x65\x00\x38\xBE"
                         b"\x1C\xFF\x0F\x01\x00\x00\x00")
        self.assertEqual(encode({"regex": re.compile(b"a*b",
                                                          re.IGNORECASE)}),
                         b"\x12\x00\x00\x00\x0B\x72\x65\x67\x65\x78\x00\x61"
                         b"\x2A\x62\x00\x69\x00\x00")
        self.assertEqual(encode({"$where": Code("test")}),
                         b"\x16\x00\x00\x00\r$where\x00\x05\x00\x00\x00test"
                         b"\x00\x00")
        self.assertEqual(encode({"$field":
                         Code("function(){ return true;}", scope=None)}),
                         b"+\x00\x00\x00\r$field\x00\x1a\x00\x00\x00"
                         b"function(){ return true;}\x00\x00")
        self.assertEqual(encode({"$field":
                          Code("return function(){ return x; }",
                            scope={'x': False})}),
                         b"=\x00\x00\x00\x0f$field\x000\x00\x00\x00\x1f\x00"
                         b"\x00\x00return function(){ return x; }\x00\t\x00"
                         b"\x00\x00\x08x\x00\x00\x00\x00")
        unicode_empty_scope = Code(u"function(){ return 'héllo';}", {})
        self.assertEqual(encode({'$field': unicode_empty_scope}),
                         b"8\x00\x00\x00\x0f$field\x00+\x00\x00\x00\x1e\x00"
                         b"\x00\x00function(){ return 'h\xc3\xa9llo';}\x00\x05"
                         b"\x00\x00\x00\x00\x00")
        a = ObjectId(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B")
        self.assertEqual(encode({"oid": a}),
                         b"\x16\x00\x00\x00\x07\x6F\x69\x64\x00\x00\x01\x02"
                         b"\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x00")
        self.assertEqual(encode({"ref": DBRef("coll", a)}),
                         b"\x2F\x00\x00\x00\x03ref\x00\x25\x00\x00\x00\x02"
                         b"$ref\x00\x05\x00\x00\x00coll\x00\x07$id\x00\x00"
                         b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x00"
                         b"\x00")

    def test_unknown_type(self):
        # Repr value differs with major python version
        part = "type %r for fieldname 'foo'" % (b'\x14',)
        docs = [
            b'\x0e\x00\x00\x00\x14foo\x00\x01\x00\x00\x00\x00',
            (b'\x16\x00\x00\x00\x04foo\x00\x0c\x00\x00\x00\x140'
             b'\x00\x01\x00\x00\x00\x00\x00'),
            (b' \x00\x00\x00\x04bar\x00\x16\x00\x00\x00\x030\x00\x0e\x00\x00'
             b'\x00\x14foo\x00\x01\x00\x00\x00\x00\x00\x00')]
        for bs in docs:
            try:
                decode(bs)
            except Exception as exc:
                self.assertTrue(isinstance(exc, InvalidBSON))
                self.assertTrue(part in str(exc))
            else:
                self.fail("Failed to raise an exception.")

    def test_dbpointer(self):
        # *Note* - DBPointer and DBRef are *not* the same thing. DBPointer
        # is a deprecated BSON type. DBRef is a convention that does not
        # exist in the BSON spec, meant to replace DBPointer. PyMongo does
        # not support creation of the DBPointer type, but will decode
        # DBPointer to DBRef.

        bs = (b"\x18\x00\x00\x00\x0c\x00\x01\x00\x00"
              b"\x00\x00RY\xb5j\xfa[\xd8A\xd6X]\x99\x00")

        self.assertEqual({'': DBRef('', ObjectId('5259b56afa5bd841d6585d99'))},
                         decode(bs))

    def test_bad_dbref(self):
        ref_only = {'ref': {'$ref': 'collection'}}
        id_only = {'ref': {'$id': ObjectId()}}

        self.assertEqual(DBRef('collection', id=None),
                         decode(encode(ref_only))['ref'])
        self.assertEqual(id_only, decode(encode(id_only)))

    def test_bytes_as_keys(self):
        doc = {b"foo": 'bar'}
        # Since `bytes` are stored as Binary you can't use them
        # as keys in python 3.x. Using binary data as a key makes
        # no sense in BSON anyway and little sense in python.
        if PY3:
            self.assertRaises(InvalidDocument, encode, doc)
        else:
            self.assertTrue(encode(doc))

    def test_datetime_encode_decode(self):
        # Negative timestamps
        dt1 = datetime.datetime(1, 1, 1, 1, 1, 1, 111000)
        dt2 = decode(encode({"date": dt1}))["date"]
        self.assertEqual(dt1, dt2)

        dt1 = datetime.datetime(1959, 6, 25, 12, 16, 59, 999000)
        dt2 = decode(encode({"date": dt1}))["date"]
        self.assertEqual(dt1, dt2)

        # Positive timestamps
        dt1 = datetime.datetime(9999, 12, 31, 23, 59, 59, 999000)
        dt2 = decode(encode({"date": dt1}))["date"]
        self.assertEqual(dt1, dt2)

        dt1 = datetime.datetime(2011, 6, 14, 10, 47, 53, 444000)
        dt2 = decode(encode({"date": dt1}))["date"]
        self.assertEqual(dt1, dt2)

    def test_large_datetime_truncation(self):
        # Ensure that a large datetime is truncated correctly.
        dt1 = datetime.datetime(9999, 1, 1, 1, 1, 1, 999999)
        dt2 = decode(encode({"date": dt1}))["date"]
        self.assertEqual(dt2.microsecond, 999000)
        self.assertEqual(dt2.second, dt1.second)

    def test_aware_datetime(self):
        aware = datetime.datetime(1993, 4, 4, 2,
                                  tzinfo=FixedOffset(555, "SomeZone"))
        as_utc = (aware - aware.utcoffset()).replace(tzinfo=utc)
        self.assertEqual(datetime.datetime(1993, 4, 3, 16, 45, tzinfo=utc),
                         as_utc)
        after = decode(encode({"date": aware}), CodecOptions(tz_aware=True))[
            "date"]
        self.assertEqual(utc, after.tzinfo)
        self.assertEqual(as_utc, after)

    def test_local_datetime(self):
        # Timezone -60 minutes of UTC, with DST between April and July.
        tz = DSTAwareTimezone(60, "sixty-minutes", 4, 7)

        # It's not DST.
        local = datetime.datetime(year=2025, month=12, hour=2, day=1,
                                  tzinfo=tz)
        options = CodecOptions(tz_aware=True, tzinfo=tz)
        # Encode with this timezone, then decode to UTC.
        encoded = encode({'date': local}, codec_options=options)
        self.assertEqual(local.replace(hour=1, tzinfo=None),
                         decode(encoded)['date'])

        # It's DST.
        local = datetime.datetime(year=2025, month=4, hour=1, day=1,
                                  tzinfo=tz)
        encoded = encode({'date': local}, codec_options=options)
        self.assertEqual(local.replace(month=3, day=31, hour=23, tzinfo=None),
                         decode(encoded)['date'])

        # Encode UTC, then decode in a different timezone.
        encoded = encode({'date': local.replace(tzinfo=utc)})
        decoded = decode(encoded, options)['date']
        self.assertEqual(local.replace(hour=3), decoded)
        self.assertEqual(tz, decoded.tzinfo)

        # Test round-tripping.
        self.assertEqual(
            local, decode(encode(
                {'date': local}, codec_options=options), options)['date'])

        # Test around the Unix Epoch.
        epochs = (
            EPOCH_AWARE,
            EPOCH_AWARE.astimezone(FixedOffset(120, 'one twenty')),
            EPOCH_AWARE.astimezone(FixedOffset(-120, 'minus one twenty'))
        )
        utc_co = CodecOptions(tz_aware=True)
        for epoch in epochs:
            doc = {'epoch': epoch}
            # We always retrieve datetimes in UTC unless told to do otherwise.
            self.assertEqual(
                EPOCH_AWARE,
                decode(encode(doc), codec_options=utc_co)['epoch'])
            # Round-trip the epoch.
            local_co = CodecOptions(tz_aware=True, tzinfo=epoch.tzinfo)
            self.assertEqual(
                epoch,
                decode(encode(doc), codec_options=local_co)['epoch'])

    def test_naive_decode(self):
        aware = datetime.datetime(1993, 4, 4, 2,
                                  tzinfo=FixedOffset(555, "SomeZone"))
        naive_utc = (aware - aware.utcoffset()).replace(tzinfo=None)
        self.assertEqual(datetime.datetime(1993, 4, 3, 16, 45), naive_utc)
        after = decode(encode({"date": aware}))["date"]
        self.assertEqual(None, after.tzinfo)
        self.assertEqual(naive_utc, after)

    def test_dst(self):
        d = {"x": datetime.datetime(1993, 4, 4, 2)}
        self.assertEqual(d, decode(encode(d)))

    def test_bad_encode(self):
        if not PY3:
            # Python3 treats this as a unicode string which won't raise
            # an exception. If we passed the string as bytes instead we
            # still wouldn't get an error since we store bytes as BSON
            # binary subtype 0.
            self.assertRaises(InvalidStringData, encode,
                              {"lalala": '\xf4\xe0\xf0\xe1\xc0 Color Touch'})
        # Work around what seems like a regression in python 3.5.0.
        # See http://bugs.python.org/issue25222
        if sys.version_info[:2] < (3, 5):
            evil_list = {'a': []}
            evil_list['a'].append(evil_list)
            evil_dict = {}
            evil_dict['a'] = evil_dict
            for evil_data in [evil_dict, evil_list]:
                self.assertRaises(Exception, encode, evil_data)

    def test_overflow(self):
        self.assertTrue(encode({"x": long(9223372036854775807)}))
        self.assertRaises(OverflowError, encode,
                          {"x": long(9223372036854775808)})

        self.assertTrue(encode({"x": long(-9223372036854775808)}))
        self.assertRaises(OverflowError, encode,
                          {"x": long(-9223372036854775809)})

    def test_small_long_encode_decode(self):
        encoded1 = encode({'x': 256})
        decoded1 = decode(encoded1)['x']
        self.assertEqual(256, decoded1)
        self.assertEqual(type(256), type(decoded1))

        encoded2 = encode({'x': Int64(256)})
        decoded2 = decode(encoded2)['x']
        expected = Int64(256)
        self.assertEqual(expected, decoded2)
        self.assertEqual(type(expected), type(decoded2))

        self.assertNotEqual(type(decoded1), type(decoded2))

    def test_tuple(self):
        self.assertEqual({"tuple": [1, 2]},
                          decode(encode({"tuple": (1, 2)})))

    def test_uuid(self):
        id = uuid.uuid4()
        transformed_id = decode(encode({"id": id}))["id"]

        self.assertTrue(isinstance(transformed_id, uuid.UUID))
        self.assertEqual(id, transformed_id)
        self.assertNotEqual(uuid.uuid4(), transformed_id)

    def test_uuid_legacy(self):

        id = uuid.uuid4()
        legacy = UUIDLegacy(id)
        self.assertEqual(3, legacy.subtype)
        transformed = decode(encode({"uuid": legacy}))["uuid"]
        self.assertTrue(isinstance(transformed, uuid.UUID))
        self.assertEqual(id, transformed)
        self.assertNotEqual(UUIDLegacy(uuid.uuid4()), UUIDLegacy(transformed))

    # The C extension was segfaulting on unicode RegExs, so we have this test
    # that doesn't really test anything but the lack of a segfault.
    def test_unicode_regex(self):
        regex = re.compile(u'revisi\xf3n')
        decode(encode({"regex": regex}))

    def test_non_string_keys(self):
        self.assertRaises(InvalidDocument, encode, {8.9: "test"})

    def test_utf8(self):
        w = {u"aéあ": u"aéあ"}
        self.assertEqual(w, decode(encode(w)))

        # b'a\xe9' == u"aé".encode("iso-8859-1")
        iso8859_bytes = b'a\xe9'
        y = {"hello": iso8859_bytes}
        if PY3:
            # Stored as BSON binary subtype 0.
            out = decode(encode(y))
            self.assertTrue(isinstance(out['hello'], bytes))
            self.assertEqual(out['hello'], iso8859_bytes)
        else:
            # Python 2.
            try:
                encode(y)
            except InvalidStringData as e:
                self.assertTrue(repr(iso8859_bytes) in str(e))

            # The next two tests only make sense in python 2.x since
            # you can't use `bytes` type as document keys in python 3.x.
            x = {u"aéあ".encode("utf-8"): u"aéあ".encode("utf-8")}
            self.assertEqual(w, decode(encode(x)))

            z = {iso8859_bytes: "hello"}
            self.assertRaises(InvalidStringData, encode, z)

    def test_null_character(self):
        doc = {"a": "\x00"}
        self.assertEqual(doc, decode(encode(doc)))

        # This test doesn't make much sense in Python2
        # since {'a': '\x00'} == {'a': u'\x00'}.
        # Decoding here actually returns {'a': '\x00'}
        doc = {"a": u"\x00"}
        self.assertEqual(doc, decode(encode(doc)))

        self.assertRaises(InvalidDocument, encode, {b"\x00": "a"})
        self.assertRaises(InvalidDocument, encode, {u"\x00": "a"})

        self.assertRaises(InvalidDocument, encode,
                          {"a": re.compile(b"ab\x00c")})
        self.assertRaises(InvalidDocument, encode,
                          {"a": re.compile(u"ab\x00c")})

    def test_move_id(self):
        self.assertEqual(b"\x19\x00\x00\x00\x02_id\x00\x02\x00\x00\x00a\x00"
                         b"\x02a\x00\x02\x00\x00\x00a\x00\x00",
                         encode(SON([("a", "a"), ("_id", "a")])))

        self.assertEqual(b"\x2c\x00\x00\x00"
                         b"\x02_id\x00\x02\x00\x00\x00b\x00"
                         b"\x03b\x00"
                         b"\x19\x00\x00\x00\x02a\x00\x02\x00\x00\x00a\x00"
                         b"\x02_id\x00\x02\x00\x00\x00a\x00\x00\x00",
                         encode(SON([("b",
                                           SON([("a", "a"), ("_id", "a")])),
                                          ("_id", "b")])))

    def test_dates(self):
        doc = {"early": datetime.datetime(1686, 5, 5),
               "late": datetime.datetime(2086, 5, 5)}
        try:
            self.assertEqual(doc, decode(encode(doc)))
        except ValueError:
            # Ignore ValueError when no C ext, since it's probably
            # a problem w/ 32-bit Python - we work around this in the
            # C ext, though.
            if bson.has_c():
                raise

    def test_custom_class(self):
        self.assertIsInstance(decode(encode({})), dict)
        self.assertNotIsInstance(decode(encode({})), SON)
        self.assertIsInstance(
            decode(encode({}), CodecOptions(document_class=SON)), SON)

        self.assertEqual(
            1, decode(encode({"x": 1}), CodecOptions(document_class=SON))["x"])

        x = encode({"x": [{"y": 1}]})
        self.assertIsInstance(
            decode(x, CodecOptions(document_class=SON))["x"][0], SON)

    def test_subclasses(self):
        # make sure we can serialize subclasses of native Python types.
        class _myint(int):
            pass

        class _myfloat(float):
            pass

        class _myunicode(text_type):
            pass

        d = {'a': _myint(42), 'b': _myfloat(63.9),
             'c': _myunicode('hello world')
            }
        d2 = decode(encode(d))
        for key, value in iteritems(d2):
            orig_value = d[key]
            orig_type = orig_value.__class__.__bases__[0]
            self.assertEqual(type(value), orig_type)
            self.assertEqual(value, orig_type(value))

    def test_ordered_dict(self):
        try:
            from collections import OrderedDict
        except ImportError:
            raise SkipTest("No OrderedDict")
        d = OrderedDict([("one", 1), ("two", 2), ("three", 3), ("four", 4)])
        self.assertEqual(
            d, decode(encode(d), CodecOptions(document_class=OrderedDict)))

    def test_bson_regex(self):
        # Invalid Python regex, though valid PCRE.
        bson_re1 = Regex(r'[\w-\.]')
        self.assertEqual(r'[\w-\.]', bson_re1.pattern)
        self.assertEqual(0, bson_re1.flags)

        doc1 = {'r': bson_re1}
        doc1_bson = (
            b'\x11\x00\x00\x00'              # document length
            b'\x0br\x00[\\w-\\.]\x00\x00'    # r: regex
            b'\x00')                         # document terminator

        self.assertEqual(doc1_bson, encode(doc1))
        self.assertEqual(doc1, decode(doc1_bson))

        # Valid Python regex, with flags.
        re2 = re.compile(u'.*', re.I | re.M | re.S | re.U | re.X)
        bson_re2 = Regex(u'.*', re.I | re.M | re.S | re.U | re.X)

        doc2_with_re = {'r': re2}
        doc2_with_bson_re = {'r': bson_re2}
        doc2_bson = (
            b"\x11\x00\x00\x00"           # document length
            b"\x0br\x00.*\x00imsux\x00"   # r: regex
            b"\x00")                      # document terminator

        self.assertEqual(doc2_bson, encode(doc2_with_re))
        self.assertEqual(doc2_bson, encode(doc2_with_bson_re))

        self.assertEqual(re2.pattern, decode(doc2_bson)['r'].pattern)
        self.assertEqual(re2.flags, decode(doc2_bson)['r'].flags)

    def test_regex_from_native(self):
        self.assertEqual('.*', Regex.from_native(re.compile('.*')).pattern)
        self.assertEqual(0, Regex.from_native(re.compile(b'')).flags)

        regex = re.compile(b'', re.I | re.L | re.M | re.S | re.X)
        self.assertEqual(
            re.I | re.L | re.M | re.S | re.X,
            Regex.from_native(regex).flags)

        unicode_regex = re.compile('', re.U)
        self.assertEqual(re.U, Regex.from_native(unicode_regex).flags)

    def test_regex_hash(self):
        self.assertRaises(TypeError, hash, Regex('hello'))

    def test_regex_comparison(self):
        re1 = Regex('a')
        re2 = Regex('b')
        self.assertNotEqual(re1, re2)
        re1 = Regex('a', re.I)
        re2 = Regex('a', re.M)
        self.assertNotEqual(re1, re2)
        re1 = Regex('a', re.I)
        re2 = Regex('a', re.I)
        self.assertEqual(re1, re2)

    def test_exception_wrapping(self):
        # No matter what exception is raised while trying to decode BSON,
        # the final exception always matches InvalidBSON.

        # {'s': '\xff'}, will throw attempting to decode utf-8.
        bad_doc = b'\x0f\x00\x00\x00\x02s\x00\x03\x00\x00\x00\xff\x00\x00\x00'

        with self.assertRaises(InvalidBSON) as context:
            decode_all(bad_doc)

        self.assertIn("codec can't decode byte 0xff",
                      str(context.exception))

    def test_minkey_maxkey_comparison(self):
        # MinKey's <, <=, >, >=, !=, and ==.
        self.assertTrue(MinKey() < None)
        self.assertTrue(MinKey() < 1)
        self.assertTrue(MinKey() <= 1)
        self.assertTrue(MinKey() <= MinKey())
        self.assertFalse(MinKey() > None)
        self.assertFalse(MinKey() > 1)
        self.assertFalse(MinKey() >= 1)
        self.assertTrue(MinKey() >= MinKey())
        self.assertTrue(MinKey() != 1)
        self.assertFalse(MinKey() == 1)
        self.assertTrue(MinKey() == MinKey())

        # MinKey compared to MaxKey.
        self.assertTrue(MinKey() < MaxKey())
        self.assertTrue(MinKey() <= MaxKey())
        self.assertFalse(MinKey() > MaxKey())
        self.assertFalse(MinKey() >= MaxKey())
        self.assertTrue(MinKey() != MaxKey())
        self.assertFalse(MinKey() == MaxKey())

        # MaxKey's <, <=, >, >=, !=, and ==.
        self.assertFalse(MaxKey() < None)
        self.assertFalse(MaxKey() < 1)
        self.assertFalse(MaxKey() <= 1)
        self.assertTrue(MaxKey() <= MaxKey())
        self.assertTrue(MaxKey() > None)
        self.assertTrue(MaxKey() > 1)
        self.assertTrue(MaxKey() >= 1)
        self.assertTrue(MaxKey() >= MaxKey())
        self.assertTrue(MaxKey() != 1)
        self.assertFalse(MaxKey() == 1)
        self.assertTrue(MaxKey() == MaxKey())

        # MaxKey compared to MinKey.
        self.assertFalse(MaxKey() < MinKey())
        self.assertFalse(MaxKey() <= MinKey())
        self.assertTrue(MaxKey() > MinKey())
        self.assertTrue(MaxKey() >= MinKey())
        self.assertTrue(MaxKey() != MinKey())
        self.assertFalse(MaxKey() == MinKey())

    def test_minkey_maxkey_hash(self):
        self.assertEqual(hash(MaxKey()), hash(MaxKey()))
        self.assertEqual(hash(MinKey()), hash(MinKey()))
        self.assertNotEqual(hash(MaxKey()), hash(MinKey()))

    def test_timestamp_comparison(self):
        # Timestamp is initialized with time, inc. Time is the more
        # significant comparand.
        self.assertTrue(Timestamp(1, 0) < Timestamp(2, 17))
        self.assertTrue(Timestamp(2, 0) > Timestamp(1, 0))
        self.assertTrue(Timestamp(1, 7) <= Timestamp(2, 0))
        self.assertTrue(Timestamp(2, 0) >= Timestamp(1, 1))
        self.assertTrue(Timestamp(2, 0) <= Timestamp(2, 0))
        self.assertTrue(Timestamp(2, 0) >= Timestamp(2, 0))
        self.assertFalse(Timestamp(1, 0) > Timestamp(2, 0))

        # Comparison by inc.
        self.assertTrue(Timestamp(1, 0) < Timestamp(1, 1))
        self.assertTrue(Timestamp(1, 1) > Timestamp(1, 0))
        self.assertTrue(Timestamp(1, 0) <= Timestamp(1, 0))
        self.assertTrue(Timestamp(1, 0) <= Timestamp(1, 1))
        self.assertFalse(Timestamp(1, 0) >= Timestamp(1, 1))
        self.assertTrue(Timestamp(1, 0) >= Timestamp(1, 0))
        self.assertTrue(Timestamp(1, 1) >= Timestamp(1, 0))
        self.assertFalse(Timestamp(1, 1) <= Timestamp(1, 0))
        self.assertTrue(Timestamp(1, 0) <= Timestamp(1, 0))
        self.assertFalse(Timestamp(1, 0) > Timestamp(1, 0))

    def test_timestamp_highorder_bits(self):
        doc = {'a': Timestamp(0xFFFFFFFF, 0xFFFFFFFF)}
        doc_bson = (b'\x10\x00\x00\x00'
                    b'\x11a\x00\xff\xff\xff\xff\xff\xff\xff\xff'
                    b'\x00')
        self.assertEqual(doc_bson, encode(doc))
        self.assertEqual(doc, decode(doc_bson))

    def test_bad_id_keys(self):
        self.assertRaises(InvalidDocument, encode,
                          {"_id": {"$bad": 123}}, True)
        self.assertRaises(InvalidDocument, encode,
                          {"_id": {'$oid': "52d0b971b3ba219fdeb4170e"}}, True)
        encode({"_id": {'$oid': "52d0b971b3ba219fdeb4170e"}})

    def test_bson_encode_thread_safe(self):

        def target(i):
            for j in range(1000):
                my_int = type('MyInt_%s_%s' % (i, j), (int,), {})
                bson.encode({'my_int': my_int()})

        threads = [ExceptionCatchingThread(target=target, args=(i,))
                   for i in range(3)]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        for t in threads:
            self.assertIsNone(t.exc)

    def test_raise_invalid_document(self):
        class Wrapper(object):
            def __init__(self, val):
                self.val = val

            def __repr__(self):
                return repr(self.val)

        self.assertEqual('1', repr(Wrapper(1)))
        with self.assertRaisesRegex(
                InvalidDocument,
                "cannot encode object: 1, of type: " + repr(Wrapper)):
            encode({'t': Wrapper(1)})


class TestCodecOptions(unittest.TestCase):
    def test_document_class(self):
        self.assertRaises(TypeError, CodecOptions, document_class=object)
        self.assertIs(SON, CodecOptions(document_class=SON).document_class)

    def test_tz_aware(self):
        self.assertRaises(TypeError, CodecOptions, tz_aware=1)
        self.assertFalse(CodecOptions().tz_aware)
        self.assertTrue(CodecOptions(tz_aware=True).tz_aware)

    def test_uuid_representation(self):
        self.assertRaises(ValueError, CodecOptions, uuid_representation=7)
        self.assertRaises(ValueError, CodecOptions, uuid_representation=2)

    def test_tzinfo(self):
        self.assertRaises(TypeError, CodecOptions, tzinfo='pacific')
        tz = FixedOffset(42, 'forty-two')
        self.assertRaises(ValueError, CodecOptions, tzinfo=tz)
        self.assertEqual(tz, CodecOptions(tz_aware=True, tzinfo=tz).tzinfo)

    def test_codec_options_repr(self):
        r = ("CodecOptions(document_class=dict, tz_aware=False, "
             "uuid_representation=UuidRepresentation.PYTHON_LEGACY, "
             "unicode_decode_error_handler='strict', "
             "tzinfo=None, type_registry=TypeRegistry(type_codecs=[], "
             "fallback_encoder=None))")
        self.assertEqual(r, repr(CodecOptions()))

    def test_decode_all_defaults(self):
        # Test decode_all()'s default document_class is dict and tz_aware is
        # False. The default uuid_representation is PYTHON_LEGACY but this
        # decodes same as STANDARD, so all this test proves about UUID decoding
        # is that it's not CSHARP_LEGACY or JAVA_LEGACY.
        doc = {'sub_document': {},
               'uuid': uuid.uuid4(),
               'dt': datetime.datetime.utcnow()}

        decoded = bson.decode_all(bson.encode(doc))[0]
        self.assertIsInstance(decoded['sub_document'], dict)
        self.assertEqual(decoded['uuid'], doc['uuid'])
        self.assertIsNone(decoded['dt'].tzinfo)

    def test_unicode_decode_error_handler(self):
        enc = encode({"keystr": "foobar"})

        # Test handling of bad key value.
        invalid_key = enc[:7] + b'\xe9' + enc[8:]
        replaced_key = b'ke\xe9str'.decode('utf-8', 'replace')
        ignored_key = b'ke\xe9str'.decode('utf-8', 'ignore')

        dec = decode(invalid_key,
                     CodecOptions(unicode_decode_error_handler="replace"))
        self.assertEqual(dec, {replaced_key: u"foobar"})

        dec = decode(invalid_key,
                     CodecOptions(unicode_decode_error_handler="ignore"))
        self.assertEqual(dec, {ignored_key: u"foobar"})

        self.assertRaises(InvalidBSON, decode, invalid_key, CodecOptions(
            unicode_decode_error_handler="strict"))
        self.assertRaises(InvalidBSON, decode, invalid_key, CodecOptions())
        self.assertRaises(InvalidBSON, decode, invalid_key)

        # Test handing of bad string value.
        invalid_val = BSON(enc[:18] + b'\xe9' + enc[19:])
        replaced_val = b'fo\xe9bar'.decode('utf-8', 'replace')
        ignored_val = b'fo\xe9bar'.decode('utf-8', 'ignore')

        dec = decode(invalid_val,
                     CodecOptions(unicode_decode_error_handler="replace"))
        self.assertEqual(dec, {u"keystr": replaced_val})

        dec = decode(invalid_val,
                     CodecOptions(unicode_decode_error_handler="ignore"))
        self.assertEqual(dec, {u"keystr": ignored_val})

        self.assertRaises(InvalidBSON, decode, invalid_val, CodecOptions(
            unicode_decode_error_handler="strict"))
        self.assertRaises(InvalidBSON, decode, invalid_val, CodecOptions())
        self.assertRaises(InvalidBSON, decode, invalid_val)

        # Test handing bad key + bad value.
        invalid_both = enc[:7] + b'\xe9' + enc[8:18] + b'\xe9' + enc[19:]

        dec = decode(invalid_both,
                     CodecOptions(unicode_decode_error_handler="replace"))
        self.assertEqual(dec, {replaced_key: replaced_val})

        dec = decode(invalid_both,
                     CodecOptions(unicode_decode_error_handler="ignore"))
        self.assertEqual(dec, {ignored_key: ignored_val})

        self.assertRaises(InvalidBSON, decode, invalid_both, CodecOptions(
            unicode_decode_error_handler="strict"))
        self.assertRaises(InvalidBSON, decode, invalid_both, CodecOptions())
        self.assertRaises(InvalidBSON, decode, invalid_both)

        # Test handling bad error mode.
        dec = decode(enc,
                     CodecOptions(unicode_decode_error_handler="junk"))
        self.assertEqual(dec, {"keystr": "foobar"})

        self.assertRaises(InvalidBSON, decode, invalid_both, CodecOptions(
            unicode_decode_error_handler="junk"))


if __name__ == "__main__":
    unittest.main()
