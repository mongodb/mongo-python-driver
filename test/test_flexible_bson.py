# -*- coding: utf-8 -*-
#
# Copyright 2018-present MongoDB, Inc.
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

"""Test the flexible bson encoder/decoder."""

import sys

sys.path[0:0] = [""]

from bson.codecs import BSONCodecBase
from bson.codec_options import CodecOptions

from test import unittest
from test.utils import rs_or_single_client

import ipdb as pdb


class MyAddressTypeNoID(object):
    def __init__(self, street=None, city=None, zip=None,):
        self.street = street
        self.city = city
        self.zip = zip

    def method1(self):
        pass

    def method2(self):
        pass


class MyAddressTypeNoIDCodec(BSONCodecBase):
    def to_bson(self, obj, writer):
        writer.start_document()
        writer.write_name_value('street', obj.street)
        writer.write_name_value('city', obj.city)
        writer.write_name_value('ZIP', obj.zip)
        writer.end_document()


class TestEncodingCustomFields(unittest.TestCase):
    def test_basic(self):
        #pdb.set_trace()
        doc = {
            'name': 'Jane Doe',
            'address': MyAddressTypeNoID(
                '100 Forest Ave.', 'Palo Alto', '95136')}

        codec_opts = CodecOptions()
        codec_opts.register_custom_type(
            MyAddressTypeNoID, MyAddressTypeNoIDCodec())

        client = rs_or_single_client()
        coll = client.test_db.get_collection(
            'test_collection', codec_options=codec_opts)
        coll.drop()

        result = coll.insert_one(doc)

        expected_document = {
            '_id': result.inserted_id,
            'name': 'Jane Doe',
            'address': {
                'street': '100 Forest Ave.',
                'city': 'Palo Alto',
                'ZIP': '95136'}}

        self.assertEqual(expected_document, coll.find_one())


if __name__ == "__main__":
    unittest.main()
