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
from __future__ import annotations

import copy
import gc
import pickle
import sys
import unittest
import uuid

from test import UnitTest

sys.path[0:0] = [""]

from bson import Code, DBRef, decode, decode_all, encode
from bson.binary import JAVA_LEGACY
from bson.codec_options import CodecOptions
from bson.errors import InvalidBSON
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument
from bson.son import SON


class TestRawBSONDocument(UnitTest):
    # {'_id': ObjectId('556df68b6e32ab21a95e0785'),
    #  'name': 'Sherlock',
    #  'addresses': [{'street': 'Baker Street'}]}
    bson_string = (
        b"Z\x00\x00\x00\x07_id\x00Um\xf6\x8bn2\xab!\xa9^\x07\x85\x02name\x00\t"
        b"\x00\x00\x00Sherlock\x00\x04addresses\x00&\x00\x00\x00\x030\x00\x1e"
        b"\x00\x00\x00\x02street\x00\r\x00\x00\x00Baker Street\x00\x00\x00\x00"
    )
    document = RawBSONDocument(bson_string)

    def test_decode(self):
        self.assertEqual("Sherlock", self.document["name"])
        first_address = self.document["addresses"][0]
        self.assertIsInstance(first_address, RawBSONDocument)
        self.assertEqual("Baker Street", first_address["street"])

    def test_raw(self):
        self.assertEqual(self.bson_string, self.document.raw)

    def test_large_subdocument_zero_copy_view(self):
        # Subdocuments at least 4KiB large are exposed as read-only
        # memoryview slices of the parent buffer instead of bytes copies
        # (PYTHON-3419).
        doc = RawBSONDocument(encode({"small": {"n": 1}, "big": {"payload": "x" * 8000}}))
        self.assertIsInstance(doc["small"].raw, bytes)
        big = doc["big"]
        self.assertIsInstance(big.raw, memoryview)
        self.assertTrue(big.raw.readonly)
        self.assertEqual(encode({"payload": "x" * 8000}), bytes(big.raw))
        self.assertEqual("x" * 8000, big["payload"])

    def test_large_subdocument_view_keeps_buffer_alive(self):
        # The view must hold its own reference to the backing buffer: with
        # every other reference dropped and the heap churned, the
        # subdocument must still read valid memory.
        expected = encode({"payload": "z" * 8000, "n": 42})
        subdoc = RawBSONDocument(encode({"big": {"payload": "z" * 8000, "n": 42}}))["big"]
        gc.collect()
        churn = [bytearray(8192) for _ in range(100)]
        self.assertEqual(42, subdoc["n"])
        self.assertEqual(expected, bytes(subdoc.raw))
        del churn

    def test_decode_whole_buffer_passthrough(self):
        # A document spanning the entire buffer is passed through as-is
        # regardless of size: no copy and no view.
        data = encode({"payload": "x" * 8000})
        doc = decode(data, DEFAULT_RAW_BSON_OPTIONS)
        self.assertIs(data, doc.raw)

    def test_decode_all_zero_copy_views(self):
        # Large documents in a multi-document stream are views of the
        # stream buffer; a lone document spanning the whole buffer is
        # passed through as-is.
        one = encode({"payload": "w" * 8000})
        docs = decode_all(one * 3, DEFAULT_RAW_BSON_OPTIONS)
        self.assertEqual(3, len(docs))
        for doc in docs:
            self.assertIsInstance(doc.raw, memoryview)
            self.assertEqual(one, bytes(doc.raw))
        self.assertEqual("w" * 8000, docs[0]["payload"])
        (single,) = decode_all(one, DEFAULT_RAW_BSON_OPTIONS)
        self.assertIsInstance(single.raw, bytes)

    def test_view_of_mutable_buffer_is_readonly(self):
        one = encode({"payload": "v" * 8000})
        docs = decode_all(bytearray(one * 2), DEFAULT_RAW_BSON_OPTIONS)
        raw = docs[0].raw
        self.assertIsInstance(raw, memoryview)
        self.assertTrue(raw.readonly)
        self.assertEqual("v" * 8000, docs[0]["payload"])

    def test_reencode_view_backed_document(self):
        inner = {"payload": "x" * 8000}
        subdoc = RawBSONDocument(encode({"big": inner}))["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        self.assertEqual(encode({"again": inner}), encode({"again": subdoc}))
        top = encode(subdoc)
        self.assertIsInstance(top, bytes)
        self.assertEqual(encode(inner), top)

    def test_pickle_view_backed_document(self):
        # Pickling serializes the raw BSON as bytes and drops the inflation
        # cache, so documents holding memoryview slices stay picklable
        # (PYTHON-3419).
        doc = RawBSONDocument(encode({"big": {"payload": "x" * 8000}}))
        subdoc = doc["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        for original in (doc, subdoc):
            unpickled = pickle.loads(pickle.dumps(original))
            self.assertIsInstance(unpickled.raw, bytes)
            self.assertEqual(original, unpickled)
            self.assertEqual(dict(original.items()), dict(unpickled.items()))

    def test_deepcopy_view_backed_document(self):
        subdoc = RawBSONDocument(encode({"big": {"payload": "y" * 8000}}))["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        copied = copy.deepcopy(subdoc)
        self.assertIsInstance(copied.raw, bytes)
        self.assertEqual(subdoc, copied)
        self.assertEqual("y" * 8000, copied["payload"])

    def test_empty_doc(self):
        doc = RawBSONDocument(encode({}))
        with self.assertRaises(KeyError):
            doc["does-not-exist"]

    def test_invalid_bson_sequence(self):
        bson_byte_sequence = encode({"a": 1}) + encode({})
        with self.assertRaisesRegex(InvalidBSON, "invalid object length"):
            RawBSONDocument(bson_byte_sequence)

    def test_invalid_bson_eoo(self):
        invalid_bson_eoo = encode({"a": 1})[:-1] + b"\x01"
        with self.assertRaisesRegex(InvalidBSON, "bad eoo"):
            RawBSONDocument(invalid_bson_eoo)

    def test_with_codec_options(self):
        # {'date': datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
        #  '_id': UUID('026fab8f-975f-4965-9fbf-85ad874c60ff')}
        # encoded with JAVA_LEGACY uuid representation.
        bson_string = (
            b"-\x00\x00\x00\x05_id\x00\x10\x00\x00\x00\x03eI_\x97\x8f\xabo\x02"
            b"\xff`L\x87\xad\x85\xbf\x9f\tdate\x00\x8a\xd6\xb9\xbaM"
            b"\x01\x00\x00\x00"
        )
        document = RawBSONDocument(
            bson_string,
            codec_options=CodecOptions(
                uuid_representation=JAVA_LEGACY, document_class=RawBSONDocument
            ),
        )

        self.assertEqual(uuid.UUID("026fab8f-975f-4965-9fbf-85ad874c60ff"), document["_id"])

    def test_preserve_key_ordering(self):
        keyvaluepairs = [
            ("a", 1),
            ("b", 2),
            ("c", 3),
        ]
        rawdoc = RawBSONDocument(encode(SON(keyvaluepairs)))

        for rkey, elt in zip(rawdoc, keyvaluepairs):
            self.assertEqual(rkey, elt[0])

    def test_contains_code_with_scope(self):
        doc = RawBSONDocument(encode({"value": Code("x=1", scope={})}))

        self.assertEqual(decode(encode(doc)), {"value": Code("x=1", {})})
        self.assertEqual(doc["value"].scope, RawBSONDocument(encode({})))

    def test_contains_dbref(self):
        doc = RawBSONDocument(encode({"value": DBRef("test", "id")}))
        raw = {"$ref": "test", "$id": "id"}
        raw_encoded = encode(decode(encode(raw)))

        self.assertEqual(decode(encode(doc)), {"value": DBRef("test", "id")})
        self.assertEqual(doc["value"].raw, raw_encoded)


if __name__ == "__main__":
    unittest.main()
