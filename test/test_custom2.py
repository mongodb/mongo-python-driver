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
import re
import sys
import uuid

sys.path[0:0] = [""]

import bson
from bson import (BSON, BSONDocumentWriter, BSONDocumentReader, BSONCodecABC,
                  CustomDocumentClassCodecBase, decode_all, decode_file_iter,
                  decode_iter, EPOCH_AWARE, is_valid, Regex, BSONTypes)
from bson.binary import Binary, UUIDLegacy
from bson.code import Code
from bson.codec_options import CodecOptions, DEFAULT_CODEC_OPTIONS
from bson.custom_bson import (
    CustomBSONTypeABC, CustomDocumentClassABC)
from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.dbref import DBRef
from bson.py3compat import abc, iteritems, PY3, StringIO, text_type
from bson.son import SON
from bson.timestamp import Timestamp
from bson.tz_util import FixedOffset
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.tz_util import (FixedOffset,
                          utc)

from test import unittest, IntegrationTest
from test.utils import rs_or_single_client

if PY3:
    long = int


class AddressField(CustomBSONTypeABC, object):
    def __init__(self, street, city, zip):
        self.street = street
        self.city = city
        self.zip = zip

    def to_bson(self, key, writer):
        # FIXME: get rid of `key` argument if possible
        writer.start_document(key)
        writer.write_string('street', self.street)
        writer.write_string('city', self.city)
        writer.write_string('ZIP', self.zip)
        writer.end_document()


class AddressDocument(CustomDocumentClassABC, object):
    def __init__(self, street=None, city=None, zip=None, pk=None):
        self.street = street
        self.city = city
        self.zip = zip
        self.pk = pk

    def get_pk(self):
        return self.pk

    def set_pk(self, pk):
        self.pk = pk

    def to_bson(self, writer):
        writer.start_document()
        writer.write_objectid('_id', self.pk)
        writer.write_string('street', self.street)
        writer.write_string('city', self.city)
        writer.write_string('ZIP', self.zip)
        writer.end_document()


class AddressDocumentWithDecoder(AddressDocument):
    @classmethod
    def from_bson(cls, reader):
        kwargs = {}
        reader.start_document()
        for entry in reader:
            kwargs[entry.name.lower()] = entry.value
        reader.end_document()
        kwargs["pk"] = kwargs.pop("_id")
        return cls(**kwargs)


class TestCustomFieldEncoding(IntegrationTest):
    def test_encoding_custom_field(self):
        name = 'John Doe'
        address = AddressField("100 Forest Ave.", "Palo Alto", "95136")

        client = rs_or_single_client()
        coll = client.test_db.get_collection('test_collection')
        coll.drop()

        result = coll.insert_one(
            {'name': name, 'address': address})

        expected_document = {
            '_id': result.inserted_id,
            'name': 'John Doe',
            'address': {
                'street': "100 Forest Ave.",
                'city': "Palo Alto",
                'ZIP': "95136"
            },
        }

        self.assertEqual(expected_document, coll.find_one())


class TestCustomDocumentClassEncoding(IntegrationTest):
    def test_encoding_custom_type(self):
        address = AddressDocument("200 Forest Ave.", "Palo Alto", "95136")

        client = rs_or_single_client(document_class=AddressDocument)
        coll = client.test_db.get_collection('test_collection')
        coll.drop()

        result = coll.insert_one(address)

        expected_document = {
            '_id': result.inserted_id,
            'street': "200 Forest Ave.",
            'city': "Palo Alto",
            'ZIP': "95136",
        }

        self.assertEqual(expected_document, coll.find_one())


class TestCustomDocumentClassDecoding(IntegrationTest):
    def test_decoding_custom_type(self):
        address = AddressDocumentWithDecoder("200 Forest Ave.", "Palo Alto", "95136")

        client = rs_or_single_client(document_class=AddressDocumentWithDecoder)
        coll = client.test_db.get_collection('test_collection')
        coll.drop()

        coll.insert_one(address)
        raddres = coll.find_one()

        for attr in ["pk", "street", "city", "zip"]:
            self.assertEqual(
                getattr(address, attr), getattr(raddres, attr))


if __name__ == "__main__":
    unittest.main()
