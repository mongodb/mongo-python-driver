# Copyright 2015-present MongoDB, Inc.
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

import datetime
import uuid

from bson import decode, encode
from bson.binary import Binary, JAVA_LEGACY
from bson.codec_options import CodecOptions
from bson.errors import InvalidBSON
from bson.raw_bson import RawBSONDocument, DEFAULT_RAW_BSON_OPTIONS
from bson.son import SON
from test import client_context, unittest
from test.test_client import IntegrationTest


class TestRawBSONDocument(IntegrationTest):

    # {u'_id': ObjectId('556df68b6e32ab21a95e0785'),
    #  u'name': u'Sherlock',
    #  u'addresses': [{u'street': u'Baker Street'}]}
    bson_string = (
        b'Z\x00\x00\x00\x07_id\x00Um\xf6\x8bn2\xab!\xa9^\x07\x85\x02name\x00\t'
        b'\x00\x00\x00Sherlock\x00\x04addresses\x00&\x00\x00\x00\x030\x00\x1e'
        b'\x00\x00\x00\x02street\x00\r\x00\x00\x00Baker Street\x00\x00\x00\x00'
    )
    document = RawBSONDocument(bson_string)

    @classmethod
    def setUpClass(cls):
        super(TestRawBSONDocument, cls).setUpClass()
        cls.client = client_context.client

    def tearDown(self):
        if client_context.connected:
            self.client.pymongo_test.test_raw.drop()

    def test_decode(self):
        self.assertEqual('Sherlock', self.document['name'])
        first_address = self.document['addresses'][0]
        self.assertIsInstance(first_address, RawBSONDocument)
        self.assertEqual('Baker Street', first_address['street'])

    def test_raw(self):
        self.assertEqual(self.bson_string, self.document.raw)

    def test_empty_doc(self):
        doc = RawBSONDocument(encode({}))
        with self.assertRaises(KeyError):
            doc['does-not-exist']

    def test_invalid_bson_sequence(self):
        bson_byte_sequence = encode({'a': 1})+encode({})
        with self.assertRaisesRegex(InvalidBSON, 'invalid object length'):
            RawBSONDocument(bson_byte_sequence)

    def test_invalid_bson_eoo(self):
        invalid_bson_eoo = encode({'a': 1})[:-1] + b'\x01'
        with self.assertRaisesRegex(InvalidBSON, 'bad eoo'):
            RawBSONDocument(invalid_bson_eoo)

    @client_context.require_connection
    def test_round_trip(self):
        db = self.client.get_database(
            'pymongo_test',
            codec_options=CodecOptions(document_class=RawBSONDocument))
        db.test_raw.insert_one(self.document)
        result = db.test_raw.find_one(self.document['_id'])
        self.assertIsInstance(result, RawBSONDocument)
        self.assertEqual(dict(self.document.items()), dict(result.items()))

    @client_context.require_connection
    def test_round_trip_raw_uuid(self):
        coll = self.client.get_database('pymongo_test').test_raw
        uid = uuid.uuid4()
        doc = {'_id': 1,
               'bin4': Binary(uid.bytes, 4),
               'bin3': Binary(uid.bytes, 3)}
        raw = RawBSONDocument(encode(doc))
        coll.insert_one(raw)
        self.assertEqual(coll.find_one(), {'_id': 1, 'bin4': uid, 'bin3': uid})

        # Test that the raw bytes haven't changed.
        raw_coll = coll.with_options(codec_options=DEFAULT_RAW_BSON_OPTIONS)
        self.assertEqual(raw_coll.find_one(), raw)

    def test_with_codec_options(self):
        # {u'date': datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
        #  u'_id': UUID('026fab8f-975f-4965-9fbf-85ad874c60ff')}
        # encoded with JAVA_LEGACY uuid representation.
        bson_string = (
            b'-\x00\x00\x00\x05_id\x00\x10\x00\x00\x00\x03eI_\x97\x8f\xabo\x02'
            b'\xff`L\x87\xad\x85\xbf\x9f\tdate\x00\x8a\xd6\xb9\xbaM'
            b'\x01\x00\x00\x00'
        )
        document = RawBSONDocument(
            bson_string,
            codec_options=CodecOptions(uuid_representation=JAVA_LEGACY,
                                       document_class=RawBSONDocument))

        self.assertEqual(uuid.UUID('026fab8f-975f-4965-9fbf-85ad874c60ff'),
                         document['_id'])

    @client_context.require_connection
    def test_round_trip_codec_options(self):
        doc = {
            'date': datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
            '_id': uuid.UUID('026fab8f-975f-4965-9fbf-85ad874c60ff')
        }
        db = self.client.pymongo_test
        coll = db.get_collection(
            'test_raw',
            codec_options=CodecOptions(uuid_representation=JAVA_LEGACY))
        coll.insert_one(doc)
        raw_java_legacy = CodecOptions(uuid_representation=JAVA_LEGACY,
                                       document_class=RawBSONDocument)
        coll = db.get_collection('test_raw', codec_options=raw_java_legacy)
        self.assertEqual(
            RawBSONDocument(encode(doc, codec_options=raw_java_legacy)),
            coll.find_one())

    @client_context.require_connection
    def test_raw_bson_document_embedded(self):
        doc = {'embedded': self.document}
        db = self.client.pymongo_test
        db.test_raw.insert_one(doc)
        result = db.test_raw.find_one()
        self.assertEqual(decode(self.document.raw), result['embedded'])

        # Make sure that CodecOptions are preserved.
        # {'embedded': [
        #   {u'date': datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
        #    u'_id': UUID('026fab8f-975f-4965-9fbf-85ad874c60ff')}
        # ]}
        # encoded with JAVA_LEGACY uuid representation.
        bson_string = (
            b'D\x00\x00\x00\x04embedded\x005\x00\x00\x00\x030\x00-\x00\x00\x00'
            b'\tdate\x00\x8a\xd6\xb9\xbaM\x01\x00\x00\x05_id\x00\x10\x00\x00'
            b'\x00\x03eI_\x97\x8f\xabo\x02\xff`L\x87\xad\x85\xbf\x9f\x00\x00'
            b'\x00'
        )
        rbd = RawBSONDocument(
            bson_string,
            codec_options=CodecOptions(uuid_representation=JAVA_LEGACY,
                                       document_class=RawBSONDocument))

        db.test_raw.drop()
        db.test_raw.insert_one(rbd)
        result = db.get_collection('test_raw', codec_options=CodecOptions(
            uuid_representation=JAVA_LEGACY)).find_one()
        self.assertEqual(rbd['embedded'][0]['_id'],
                         result['embedded'][0]['_id'])

    @client_context.require_connection
    def test_write_response_raw_bson(self):
        coll = self.client.get_database(
            'pymongo_test',
            codec_options=CodecOptions(document_class=RawBSONDocument)).test_raw

        # No Exceptions raised while handling write response.
        coll.insert_one(self.document)
        coll.delete_one(self.document)
        coll.insert_many([self.document])
        coll.delete_many(self.document)
        coll.update_one(self.document, {'$set': {'a': 'b'}}, upsert=True)
        coll.update_many(self.document, {'$set': {'b': 'c'}})

    def test_preserve_key_ordering(self):
        keyvaluepairs = [('a', 1), ('b', 2), ('c', 3),]
        rawdoc = RawBSONDocument(encode(SON(keyvaluepairs)))

        for rkey, elt in zip(rawdoc, keyvaluepairs):
            self.assertEqual(rkey, elt[0])
