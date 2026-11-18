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
import threading
import unittest
import uuid

from test import UnitTest

sys.path[0:0] = [""]

from bson import Code, DBRef, decode, decode_all, encode, has_c
from bson.binary import JAVA_LEGACY
from bson.codec_options import CodecOptions
from bson.errors import InvalidBSON
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument
from bson.son import SON

# {'_id': ObjectId('556df68b6e32ab21a95e0785'),
#  'name': 'Sherlock',
#  'addresses': [{'street': 'Baker Street'}]}
TEST_RAW_BSON = (
    b"Z\x00\x00\x00\x07_id\x00Um\xf6\x8bn2\xab!\xa9^\x07\x85\x02name\x00\t"
    b"\x00\x00\x00Sherlock\x00\x04addresses\x00&\x00\x00\x00\x030\x00\x1e"
    b"\x00\x00\x00\x02street\x00\r\x00\x00\x00Baker Street\x00\x00\x00\x00"
)

N_THREADS = 16
N_ITERS = 200
BIG_PAYLOAD = "x" * 8000


def _make_shared_doc_bytes() -> bytes:
    return encode(
        {
            "small": {"n": 1},
            "big": {"payload": BIG_PAYLOAD, "n": 42},
            "arr": [{"payload": BIG_PAYLOAD, "idx": i} for i in range(3)],
        }
    )


class _TaggedRawBSONDocument(RawBSONDocument):
    """RawBSONDocument subclass with a different __init__ signature and
    extra instance state, stored in __dict__.
    """

    def __init__(self, bson_bytes, tag, codec_options=None):
        super().__init__(bson_bytes, codec_options)
        self.tag = tag


class _SlottedRawBSONDocument(RawBSONDocument):
    """RawBSONDocument subclass with extra state stored in its own slot."""

    __slots__ = ("tag",)

    def __init__(self, bson_bytes, tag, codec_options=None):
        super().__init__(bson_bytes, codec_options)
        self.tag = tag


class TestRawBSONDocument(UnitTest):
    bson_string = TEST_RAW_BSON
    document = RawBSONDocument(bson_string)

    def test_decode(self):
        self.assertEqual("Sherlock", self.document["name"])
        first_address = self.document["addresses"][0]
        self.assertIsInstance(first_address, RawBSONDocument)
        self.assertEqual("Baker Street", first_address["street"])

    def test_raw(self):
        self.assertEqual(self.bson_string, self.document.raw)

    def test_large_subdocument_zero_copy_view(self):
        doc = RawBSONDocument(encode({"small": {"n": 1}, "big": {"payload": "x" * 8000}}))
        self.assertIsInstance(doc["small"].raw, bytes)
        big = doc["big"]
        self.assertIsInstance(big.raw, memoryview)
        self.assertTrue(big.raw.readonly)
        self.assertEqual(encode({"payload": "x" * 8000}), bytes(big.raw))
        self.assertEqual("x" * 8000, big["payload"])

    def test_large_subdocument_view_keeps_buffer_alive(self):
        expected = encode({"payload": "z" * 8000, "n": 42})
        subdoc = RawBSONDocument(encode({"big": {"payload": "z" * 8000, "n": 42}}))["big"]
        gc.collect()
        churn = [bytearray(8192) for _ in range(100)]
        self.assertEqual(42, subdoc["n"])
        self.assertEqual(expected, bytes(subdoc.raw))
        del churn

    def test_decode_whole_buffer_passthrough(self):
        data = encode({"payload": "x" * 8000})
        doc = decode(data, DEFAULT_RAW_BSON_OPTIONS)
        self.assertIs(data, doc.raw)

    def test_decode_all_zero_copy_views(self):
        one = encode({"payload": "w" * 8000})
        docs = decode_all(one * 3, DEFAULT_RAW_BSON_OPTIONS)
        self.assertEqual(3, len(docs))
        for doc in docs:
            self.assertIsInstance(doc.raw, memoryview)
            self.assertEqual(one, bytes(doc.raw))
        self.assertEqual("w" * 8000, docs[0]["payload"])
        (single,) = decode_all(one, DEFAULT_RAW_BSON_OPTIONS)
        self.assertIsInstance(single.raw, bytes)

    def test_mutable_buffer_input_copied(self):
        one = encode({"payload": "v" * 8000})
        buf = bytearray(one * 2)
        docs = decode_all(buf, DEFAULT_RAW_BSON_OPTIONS)
        for doc in docs:
            self.assertIsInstance(doc.raw, bytes)
        buf[:] = bytes(len(buf))
        self.assertEqual("v" * 8000, docs[0]["payload"])
        self.assertEqual(one, docs[1].raw)

    def test_reencode_view_backed_document(self):
        inner = {"payload": "x" * 8000}
        subdoc = RawBSONDocument(encode({"big": inner}))["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        self.assertEqual(encode({"again": inner}), encode({"again": subdoc}))
        top = encode(subdoc)
        self.assertIsInstance(top, bytes)
        self.assertEqual(encode(inner), top)

    @unittest.skipUnless(has_c(), "tests the C extension")
    def test_c_encode_rejects_non_bytes_raw(self):
        # The C encoder accepts only bytes and memoryview .raw values:
        # other buffer types (e.g. a mutable bytearray) raise TypeError.
        class _ByteArrayRaw(RawBSONDocument):
            @property
            def raw(self):
                return bytearray(super().raw)

        doc = _ByteArrayRaw(encode({"a": 1}))
        with self.assertRaisesRegex(TypeError, "must be bytes or memoryview"):
            encode({"sub": doc})

    def test_pickle_view_backed_document(self):
        doc = RawBSONDocument(encode({"big": {"payload": "x" * 8000}}))
        subdoc = doc["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        for original in (doc, subdoc):
            unpickled = pickle.loads(pickle.dumps(original))
            self.assertIsInstance(unpickled.raw, bytes)
            self.assertEqual(original, unpickled)
            self.assertEqual(dict(original.items()), dict(unpickled.items()))

    def test_decode_mutable_buffer_copied(self):
        for decode_one in (
            lambda buf: decode(buf, DEFAULT_RAW_BSON_OPTIONS),
            lambda buf: decode_all(buf, DEFAULT_RAW_BSON_OPTIONS)[0],
        ):
            buf = bytearray(encode({"a": 1}))
            doc = decode_one(buf)
            self.assertIsInstance(doc.raw, bytes)
            buf[-5] = 99
            self.assertEqual(1, doc["a"])

    def test_buffer_input_copied(self):
        big = encode({"payload": "x" * 8000})
        buf = bytearray(big + encode({"a": 1}))
        docs = decode_all(memoryview(buf), DEFAULT_RAW_BSON_OPTIONS)
        big_raw = docs[0].raw
        self.assertIsInstance(big_raw, memoryview)
        buf[:] = bytes(len(buf))
        self.assertEqual(big, bytes(big_raw))

    def test_pickle_deepcopy_subclass(self):
        raw_bytes = encode({"payload": "x" * 8000})
        for cls in (_TaggedRawBSONDocument, _SlottedRawBSONDocument):
            original = cls(raw_bytes, "tag-value")
            for roundtrip in (lambda doc: pickle.loads(pickle.dumps(doc)), copy.deepcopy):
                duplicate = roundtrip(original)
                self.assertIsInstance(duplicate, cls)
                self.assertEqual(original, duplicate)
                self.assertEqual("tag-value", duplicate.tag)
                self.assertIsInstance(duplicate.raw, bytes)

    def test_repr_view_backed_document(self):
        subdoc = RawBSONDocument(encode({"big": {"payload": "x" * 8000}}))["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        self.assertIn(repr(bytes(subdoc.raw)), repr(subdoc))

    @unittest.skipUnless(has_c(), "tests the C extension")
    def test_element_to_dict_error_does_not_pin_buffer(self):
        from bson import _cbson  # type:ignore[attr-defined]

        # An array whose first element is a large subdocument (creates a
        # zero-copy view) and whose second element has an invalid type byte.
        data = encode({"arr": [{"payload": "x" * 8000}, 1]})
        marker = b"\x101\x00"  # type 0x10, key "1"
        data = data.replace(marker, b"\xee1\x00")
        refcount = sys.getrefcount(data)
        for _ in range(5):
            with self.assertRaises(InvalidBSON):
                _cbson._element_to_dict(data, 4, len(data) - 1, DEFAULT_RAW_BSON_OPTIONS, False)
        self.assertEqual(refcount, sys.getrefcount(data))

    def test_deepcopy_view_backed_document(self):
        subdoc = RawBSONDocument(encode({"big": {"payload": "y" * 8000}}))["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        copied = copy.deepcopy(subdoc)
        self.assertIsInstance(copied.raw, bytes)
        self.assertEqual(subdoc, copied)
        self.assertEqual("y" * 8000, copied["payload"])

    def test_inflate_from_memoryview(self):
        doc = RawBSONDocument(memoryview(self.bson_string))
        self.assertEqual("Sherlock", doc["name"])
        first_address = doc["addresses"][0]
        self.assertIsInstance(first_address, RawBSONDocument)
        self.assertEqual("Baker Street", first_address["street"])

    def test_inflate_view_backed_document_detached(self):
        payload = {"payload": "x" * 8000}
        # outer's memoryview is the only reference to the raw document buffer
        outer = RawBSONDocument(encode({"outer": {"inner": payload}}))["outer"]
        self.assertIsInstance(outer.raw, memoryview)
        inner = outer["inner"]
        self.assertIsInstance(inner.raw, memoryview)
        self.assertEqual(encode(payload), bytes(inner.raw))
        self.assertEqual("x" * 8000, inner["payload"])

    def test_inflate_into_non_dict_mapping(self):
        from bson import _raw_to_dict

        data = encode(SON([("b", 2), ("a", 1)]))
        result = _raw_to_dict(data, 4, len(data) - 1, DEFAULT_RAW_BSON_OPTIONS, SON())
        self.assertIsInstance(result, SON)
        self.assertEqual([("b", 2), ("a", 1)], list(result.items()))

    def test_invalid_element_type_detected_on_inflation(self):
        invalid_type = bytearray(encode({"a": 1}))
        invalid_type[4] = 0x14  # Not a valid BSON type marker.
        doc = RawBSONDocument(bytes(invalid_type))
        with self.assertRaisesRegex(InvalidBSON, "Detected unknown BSON type"):
            doc["a"]

    def test_misaligned_elements_detected_on_inflation(self):
        # {"a": 1} plus a stray byte before the end-of-object, with a matching size.
        misaligned = b"\x0d\x00\x00\x00\x10a\x00\x01\x00\x00\x00\x05\x00"
        doc = RawBSONDocument(misaligned)
        with self.assertRaisesRegex(InvalidBSON, "bad object or element length"):
            doc["a"]

    @unittest.skipUnless(has_c(), "tests the C extension")
    def test_raw_to_dict_error_does_not_leak(self):
        from bson import _cbson  # type: ignore[attr-defined]

        # An invalid type byte partway through the document exercises the
        # error path after some elements have already been decoded.
        data = encode({"big": {"payload": "x" * 8000}, "a": 1})
        marker = b"\x10a\x00"  # type 0x10, key "a"
        data = data.replace(marker, b"\xeea\x00")
        refcount = sys.getrefcount(data)
        for _ in range(5):
            with self.assertRaises(InvalidBSON):
                _cbson._raw_to_dict(data, 4, len(data) - 1, DEFAULT_RAW_BSON_OPTIONS, {}, False)
        self.assertEqual(refcount, sys.getrefcount(data))

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

        for rkey, elt in zip(rawdoc, keyvaluepairs, strict=True):
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


class TestRawBSONDocumentConcurrency(unittest.TestCase):
    """Concurrency and buffer-lifetime regression tests for zero-copy
    RawBSONDocument; also run under the sanitizer CI tasks (see
    SANITIZER_FILES in .evergreen/scripts/setup_tests.py).
    """

    def test_concurrent_reads_on_shared_document(self):
        doc_bytes = _make_shared_doc_bytes()
        shared_doc = RawBSONDocument(doc_bytes)

        # Confirm we're actually exercising the zero-copy memoryview path,
        # not accidentally testing a byte-copy fallback.
        big_check = shared_doc["big"]
        self.assertIsInstance(big_check.raw, memoryview)
        self.assertTrue(big_check.raw.readonly)

        errors: list[BaseException] = []
        errors_lock = threading.Lock()

        def worker() -> None:
            try:
                for _ in range(N_ITERS):
                    big = shared_doc["big"]
                    self.assertEqual(big["payload"], BIG_PAYLOAD)
                    self.assertEqual(big["n"], 42)
                    self.assertIsInstance(big.raw, memoryview)

                    arr = shared_doc["arr"]
                    for j, item in enumerate(arr):
                        self.assertEqual(item["idx"], j)
                        self.assertEqual(item["payload"], BIG_PAYLOAD)

                    small = shared_doc["small"]
                    self.assertEqual(small["n"], 1)

                    # Re-encodes a view-backed subdocument while other
                    # threads may be reading the same underlying buffer.
                    reencoded = encode({"again": big})
                    self.assertIsInstance(reencoded, bytes)

                    multi = decode_all(doc_bytes * 2, DEFAULT_RAW_BSON_OPTIONS)
                    self.assertEqual(len(multi), 2)
                    for d in multi:
                        self.assertIsInstance(d["big"].raw, memoryview)
            except BaseException as exc:
                with errors_lock:
                    errors.append(exc)
                raise

        threads = [threading.Thread(target=worker) for _ in range(N_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])

    def test_buffer_survives_after_source_bytes_are_dropped(self):
        # Slice a bytearray, wrap it in a RawBSONDocument, then mutate and
        # drop the original buffer to check for a dangling view.
        encoded = _make_shared_doc_bytes()
        for _ in range(50):
            buf = bytearray(encoded) + b"\x00" * 10
            view = memoryview(buf)[: len(encoded)]
            raw = RawBSONDocument(view)
            _ = raw["big"]
            _ = dict(raw["small"])
            _ = list(raw["arr"])
            copied = dict(raw)
            del raw
            del view
            buf[:] = b"\xff" * len(buf)
            del buf
            gc.collect()
            self.assertEqual(copied["small"]["n"], 1)


class _FakeBulkWriteContext:
    """Stand-in for pymongo.message._BulkWriteContext.

    The C batched-message builders only read a few numeric size attributes
    off the context object.
    """

    max_bson_size = 16 * 1024 * 1024
    max_write_batch_size = 100000
    max_message_size = 48 * 1024 * 1024
    max_split_size = 16 * 1024 * 1024


class TestBatchedMessageBuilderConcurrency(unittest.TestCase):
    def test_concurrent_batched_message_building(self):
        try:
            from pymongo import _cmessage
        except ImportError:
            self.skipTest("pymongo._cmessage C extension is not built")

        has_op_msg = hasattr(_cmessage, "_encode_batched_op_msg")
        has_write_cmd = hasattr(_cmessage, "_encode_batched_write_command")
        if not (has_op_msg or has_write_cmd):
            self.skipTest("no batched message builder found on pymongo._cmessage")

        ns = "db.coll"
        docs = [{"_id": i, "payload": "y" * 100} for i in range(50)]
        command = {"insert": "coll", "ordered": True}
        ctx = _FakeBulkWriteContext()
        insert_op = 0  # pymongo.message._INSERT

        errors: list[BaseException] = []
        errors_lock = threading.Lock()

        def worker() -> None:
            try:
                for _ in range(N_ITERS):
                    if has_op_msg:
                        _cmessage._encode_batched_op_msg(
                            insert_op, command, docs, True, DEFAULT_RAW_BSON_OPTIONS, ctx
                        )
                    if has_write_cmd:
                        _cmessage._encode_batched_write_command(
                            ns, insert_op, command, docs, DEFAULT_RAW_BSON_OPTIONS, ctx
                        )
            except BaseException as exc:
                with errors_lock:
                    errors.append(exc)
                raise

        threads = [threading.Thread(target=worker) for _ in range(N_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
