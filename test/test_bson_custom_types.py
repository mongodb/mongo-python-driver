# Copyright 2019-present MongoDB, Inc.
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

"""Test support for callbacks to encode/decode custom types."""

import sys
import tempfile
from decimal import Decimal
from random import random

sys.path[0:0] = [""]

from bson import (BSON,
                  Decimal128,
                  decode_all,
                  decode_file_iter,
                  decode_iter,
                  _dict_to_bson,
                  _bson_to_dict)
from bson.codec_options import CodecOptions, TypeCodecBase, TypeRegistry

from test import unittest


class DecimalCodec(TypeCodecBase):
    @property
    def bson_type(self):
        return Decimal128

    @property
    def python_type(self):
        return Decimal

    def transform_bson(self, value):
        return value.to_decimal()

    def transform_python(self, value):
        return Decimal128(value)


class TestCustomPythonTypeToBSON(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        type_registry = TypeRegistry(DecimalCodec())
        codec_options = CodecOptions(type_registry=type_registry)
        cls.codecopts = codec_options

    def test_encode_decode_roundtrip(self):
        document = {'average': Decimal('56.47')}
        bsonbytes = BSON().encode(document, codec_options=self.codecopts)
        rt_document = BSON(bsonbytes).decode(codec_options=self.codecopts)
        self.assertEqual(document, rt_document)

    def test_decode_all(self):
        documents = []
        for dec in range(3):
            documents.append({'average': Decimal('56.4%s' % (dec,))})

        bsonstream = bytes()
        for doc in documents:
            bsonstream += BSON.encode(doc, codec_options=self.codecopts)

        self.assertEqual(
            decode_all(bsonstream, self.codecopts), documents)

    def test__bson_to_dict(self):
        document = {'average': Decimal('56.47')}
        rawbytes = BSON.encode(document, codec_options=self.codecopts)
        decoded_document = _bson_to_dict(rawbytes, self.codecopts)
        self.assertEqual(document, decoded_document)

    def test__dict_to_bson(self):
        document = {'average': Decimal('56.47')}
        rawbytes = BSON.encode(document, codec_options=self.codecopts)
        encoded_document = _dict_to_bson(document, False, self.codecopts)
        self.assertEqual(encoded_document, rawbytes)

    def _generate_multidocument_bson_stream(self):
        inp_num = [str(random() * 100)[:4] for _ in range(10)]
        docs = [{'n': Decimal128(dec)} for dec in inp_num]
        edocs = [{'n': Decimal(dec)} for dec in inp_num]
        bsonstream = b""
        for doc in docs:
            bsonstream += BSON.encode(doc)
        return edocs, bsonstream

    def test_decode_iter(self):
        expected, bson_data = self._generate_multidocument_bson_stream()
        for expected_doc, decoded_doc in zip(
                expected, decode_iter(bson_data, self.codecopts)):
            self.assertEqual(expected_doc, decoded_doc)

    def test_decode_file_iter(self):
        expected, bson_data = self._generate_multidocument_bson_stream()
        fileobj = tempfile.TemporaryFile()
        fileobj.write(bson_data)
        fileobj.seek(0)

        for expected_doc, decoded_doc in zip(
                expected, decode_file_iter(fileobj, self.codecopts)):
            self.assertEqual(expected_doc, decoded_doc)

        fileobj.close()



if __name__ == "__main__":
    unittest.main()
