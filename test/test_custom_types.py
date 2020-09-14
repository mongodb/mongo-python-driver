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

import datetime
import sys
import tempfile
from collections import OrderedDict
from decimal import Decimal
from random import random

sys.path[0:0] = [""]

from bson import (Decimal128,
                  decode,
                  decode_all,
                  decode_file_iter,
                  decode_iter,
                  encode,
                  RE_TYPE,
                  _BUILT_IN_TYPES,
                  _dict_to_bson,
                  _bson_to_dict)
from bson.code import Code
from bson.codec_options import (CodecOptions, TypeCodec, TypeDecoder,
                                TypeEncoder, TypeRegistry)
from bson.errors import InvalidDocument
from bson.int64 import Int64
from bson.raw_bson import RawBSONDocument
from bson.py3compat import text_type

from gridfs import GridIn, GridOut

from pymongo.collection import ReturnDocument
from pymongo.errors import DuplicateKeyError
from pymongo.message import _CursorAddress

from test import client_context, unittest
from test.test_client import IntegrationTest
from test.utils import ignore_deprecations, rs_client


class DecimalEncoder(TypeEncoder):
    @property
    def python_type(self):
        return Decimal

    def transform_python(self, value):
        return Decimal128(value)


class DecimalDecoder(TypeDecoder):
    @property
    def bson_type(self):
        return Decimal128

    def transform_bson(self, value):
        return value.to_decimal()


class DecimalCodec(DecimalDecoder, DecimalEncoder):
    pass


DECIMAL_CODECOPTS = CodecOptions(
    type_registry=TypeRegistry([DecimalCodec()]))


class UndecipherableInt64Type(object):
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.value == other.value
        # Does not compare equal to integers.
        return False


class UndecipherableIntDecoder(TypeDecoder):
    bson_type = Int64
    def transform_bson(self, value):
        return UndecipherableInt64Type(value)


class UndecipherableIntEncoder(TypeEncoder):
    python_type = UndecipherableInt64Type
    def transform_python(self, value):
        return Int64(value.value)


UNINT_DECODER_CODECOPTS = CodecOptions(
    type_registry=TypeRegistry([UndecipherableIntDecoder(), ]))


UNINT_CODECOPTS = CodecOptions(type_registry=TypeRegistry(
    [UndecipherableIntDecoder(), UndecipherableIntEncoder()]))


class UppercaseTextDecoder(TypeDecoder):
    bson_type = text_type
    def transform_bson(self, value):
        return value.upper()


UPPERSTR_DECODER_CODECOPTS = CodecOptions(type_registry=TypeRegistry(
    [UppercaseTextDecoder(),]))


def type_obfuscating_decoder_factory(rt_type):
    class ResumeTokenToNanDecoder(TypeDecoder):
        bson_type = rt_type
        def transform_bson(self, value):
            return "NaN"
    return ResumeTokenToNanDecoder


class CustomBSONTypeTests(object):
    def roundtrip(self, doc):
        bsonbytes = encode(doc, codec_options=self.codecopts)
        rt_document = decode(bsonbytes, codec_options=self.codecopts)
        self.assertEqual(doc, rt_document)

    def test_encode_decode_roundtrip(self):
        self.roundtrip({'average': Decimal('56.47')})
        self.roundtrip({'average': {'b': Decimal('56.47')}})
        self.roundtrip({'average': [Decimal('56.47')]})
        self.roundtrip({'average': [[Decimal('56.47')]]})
        self.roundtrip({'average': [{'b': Decimal('56.47')}]})

    def test_decode_all(self):
        documents = []
        for dec in range(3):
            documents.append({'average': Decimal('56.4%s' % (dec,))})

        bsonstream = bytes()
        for doc in documents:
            bsonstream += encode(doc, codec_options=self.codecopts)

        self.assertEqual(
            decode_all(bsonstream, self.codecopts), documents)

    def test__bson_to_dict(self):
        document = {'average': Decimal('56.47')}
        rawbytes = encode(document, codec_options=self.codecopts)
        decoded_document = _bson_to_dict(rawbytes, self.codecopts)
        self.assertEqual(document, decoded_document)

    def test__dict_to_bson(self):
        document = {'average': Decimal('56.47')}
        rawbytes = encode(document, codec_options=self.codecopts)
        encoded_document = _dict_to_bson(document, False, self.codecopts)
        self.assertEqual(encoded_document, rawbytes)

    def _generate_multidocument_bson_stream(self):
        inp_num = [str(random() * 100)[:4] for _ in range(10)]
        docs = [{'n': Decimal128(dec)} for dec in inp_num]
        edocs = [{'n': Decimal(dec)} for dec in inp_num]
        bsonstream = b""
        for doc in docs:
            bsonstream += encode(doc)
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


class TestCustomPythonBSONTypeToBSONMonolithicCodec(CustomBSONTypeTests,
                                                    unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.codecopts = DECIMAL_CODECOPTS


class TestCustomPythonBSONTypeToBSONMultiplexedCodec(CustomBSONTypeTests,
                                                     unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        codec_options = CodecOptions(
            type_registry=TypeRegistry((DecimalEncoder(), DecimalDecoder())))
        cls.codecopts = codec_options


class TestBSONFallbackEncoder(unittest.TestCase):
    def _get_codec_options(self, fallback_encoder):
        type_registry = TypeRegistry(fallback_encoder=fallback_encoder)
        return CodecOptions(type_registry=type_registry)

    def test_simple(self):
        codecopts = self._get_codec_options(lambda x: Decimal128(x))
        document = {'average': Decimal('56.47')}
        bsonbytes = encode(document, codec_options=codecopts)

        exp_document = {'average': Decimal128('56.47')}
        exp_bsonbytes = encode(exp_document)
        self.assertEqual(bsonbytes, exp_bsonbytes)

    def test_erroring_fallback_encoder(self):
        codecopts = self._get_codec_options(lambda _: 1/0)

        # fallback converter should not be invoked when encoding known types.
        encode(
            {'a': 1, 'b': Decimal128('1.01'), 'c': {'arr': ['abc', 3.678]}},
            codec_options=codecopts)

        # expect an error when encoding a custom type.
        document = {'average': Decimal('56.47')}
        with self.assertRaises(ZeroDivisionError):
            encode(document, codec_options=codecopts)

    def test_noop_fallback_encoder(self):
        codecopts = self._get_codec_options(lambda x: x)
        document = {'average': Decimal('56.47')}
        with self.assertRaises(InvalidDocument):
            encode(document, codec_options=codecopts)

    def test_type_unencodable_by_fallback_encoder(self):
        def fallback_encoder(value):
            try:
                return Decimal128(value)
            except:
                raise TypeError("cannot encode type %s" % (type(value)))
        codecopts = self._get_codec_options(fallback_encoder)
        document = {'average': Decimal}
        with self.assertRaises(TypeError):
            encode(document, codec_options=codecopts)


class TestBSONTypeEnDeCodecs(unittest.TestCase):
    def test_instantiation(self):
        msg = "Can't instantiate abstract class"
        def run_test(base, attrs, fail):
            codec = type('testcodec', (base,), attrs)
            if fail:
                with self.assertRaisesRegex(TypeError, msg):
                    codec()
            else:
                codec()

        class MyType(object):
            pass

        run_test(TypeEncoder, {'python_type': MyType,}, fail=True)
        run_test(TypeEncoder, {'transform_python': lambda s, x: x}, fail=True)
        run_test(TypeEncoder, {'transform_python': lambda s, x: x,
                               'python_type': MyType}, fail=False)

        run_test(TypeDecoder, {'bson_type': Decimal128, }, fail=True)
        run_test(TypeDecoder, {'transform_bson': lambda s, x: x}, fail=True)
        run_test(TypeDecoder, {'transform_bson': lambda s, x: x,
                               'bson_type': Decimal128}, fail=False)

        run_test(TypeCodec, {'bson_type': Decimal128,
                             'python_type': MyType}, fail=True)
        run_test(TypeCodec, {'transform_bson': lambda s, x: x,
                             'transform_python': lambda s, x: x}, fail=True)
        run_test(TypeCodec, {'python_type': MyType,
                             'transform_python': lambda s, x: x,
                             'transform_bson': lambda s, x: x,
                             'bson_type': Decimal128}, fail=False)

    def test_type_checks(self):
        self.assertTrue(issubclass(TypeCodec, TypeEncoder))
        self.assertTrue(issubclass(TypeCodec, TypeDecoder))
        self.assertFalse(issubclass(TypeDecoder, TypeEncoder))
        self.assertFalse(issubclass(TypeEncoder, TypeDecoder))


class TestBSONCustomTypeEncoderAndFallbackEncoderTandem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class TypeA(object):
            def __init__(self, x):
                self.value = x

        class TypeB(object):
            def __init__(self, x):
                self.value = x

        # transforms A, and only A into B
        def fallback_encoder_A2B(value):
            assert isinstance(value, TypeA)
            return TypeB(value.value)

        # transforms A, and only A into something encodable
        def fallback_encoder_A2BSON(value):
            assert isinstance(value, TypeA)
            return value.value

        # transforms B into something encodable
        class B2BSON(TypeEncoder):
            python_type = TypeB
            def transform_python(self, value):
                return value.value

        # transforms A into B
        # technically, this isn't a proper type encoder as the output is not
        # BSON-encodable.
        class A2B(TypeEncoder):
            python_type = TypeA
            def transform_python(self, value):
                return TypeB(value.value)

        # transforms B into A
        # technically, this isn't a proper type encoder as the output is not
        # BSON-encodable.
        class B2A(TypeEncoder):
            python_type = TypeB
            def transform_python(self, value):
                return TypeA(value.value)

        cls.TypeA = TypeA
        cls.TypeB = TypeB
        cls.fallback_encoder_A2B = staticmethod(fallback_encoder_A2B)
        cls.fallback_encoder_A2BSON = staticmethod(fallback_encoder_A2BSON)
        cls.B2BSON = B2BSON
        cls.B2A = B2A
        cls.A2B = A2B

    def test_encode_fallback_then_custom(self):
        codecopts = CodecOptions(type_registry=TypeRegistry(
            [self.B2BSON()], fallback_encoder=self.fallback_encoder_A2B))
        testdoc = {'x': self.TypeA(123)}
        expected_bytes = encode({'x': 123})

        self.assertEqual(encode(testdoc, codec_options=codecopts),
                         expected_bytes)

    def test_encode_custom_then_fallback(self):
        codecopts = CodecOptions(type_registry=TypeRegistry(
            [self.B2A()], fallback_encoder=self.fallback_encoder_A2BSON))
        testdoc = {'x': self.TypeB(123)}
        expected_bytes = encode({'x': 123})

        self.assertEqual(encode(testdoc, codec_options=codecopts),
                         expected_bytes)

    def test_chaining_encoders_fails(self):
        codecopts = CodecOptions(type_registry=TypeRegistry(
            [self.A2B(), self.B2BSON()]))

        with self.assertRaises(InvalidDocument):
            encode({'x': self.TypeA(123)}, codec_options=codecopts)

    def test_infinite_loop_exceeds_max_recursion_depth(self):
        codecopts = CodecOptions(type_registry=TypeRegistry(
            [self.B2A()], fallback_encoder=self.fallback_encoder_A2B))

        # Raises max recursion depth exceeded error
        with self.assertRaises(RuntimeError):
            encode({'x': self.TypeA(100)}, codec_options=codecopts)


class TestTypeRegistry(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class MyIntType(object):
            def __init__(self, x):
                assert isinstance(x, int)
                self.x = x

        class MyStrType(object):
            def __init__(self, x):
                assert isinstance(x, str)
                self.x = x

        class MyIntCodec(TypeCodec):
            @property
            def python_type(self):
                return MyIntType

            @property
            def bson_type(self):
                return int

            def transform_python(self, value):
                return value.x

            def transform_bson(self, value):
                return MyIntType(value)

        class MyStrCodec(TypeCodec):
            @property
            def python_type(self):
                return MyStrType

            @property
            def bson_type(self):
                return str

            def transform_python(self, value):
                return value.x

            def transform_bson(self, value):
                return MyStrType(value)

        def fallback_encoder(value):
            return value

        cls.types = (MyIntType, MyStrType)
        cls.codecs = (MyIntCodec, MyStrCodec)
        cls.fallback_encoder = fallback_encoder

    def test_simple(self):
        codec_instances = [codec() for codec in self.codecs]
        def assert_proper_initialization(type_registry, codec_instances):
            self.assertEqual(type_registry._encoder_map, {
                self.types[0]: codec_instances[0].transform_python,
                self.types[1]: codec_instances[1].transform_python})
            self.assertEqual(type_registry._decoder_map, {
                int: codec_instances[0].transform_bson,
                str: codec_instances[1].transform_bson})
            self.assertEqual(
                type_registry._fallback_encoder, self.fallback_encoder)

        type_registry = TypeRegistry(codec_instances, self.fallback_encoder)
        assert_proper_initialization(type_registry, codec_instances)

        type_registry = TypeRegistry(
            fallback_encoder=self.fallback_encoder, type_codecs=codec_instances)
        assert_proper_initialization(type_registry, codec_instances)

        # Ensure codec list held by the type registry doesn't change if we
        # mutate the initial list.
        codec_instances_copy = list(codec_instances)
        codec_instances.pop(0)
        self.assertListEqual(
            type_registry._TypeRegistry__type_codecs, codec_instances_copy)

    def test_simple_separate_codecs(self):
        class MyIntEncoder(TypeEncoder):
            python_type = self.types[0]

            def transform_python(self, value):
                return value.x

        class MyIntDecoder(TypeDecoder):
            bson_type = int

            def transform_bson(self, value):
                return self.types[0](value)

        codec_instances = [MyIntDecoder(), MyIntEncoder()]
        type_registry = TypeRegistry(codec_instances)

        self.assertEqual(
            type_registry._encoder_map,
            {MyIntEncoder.python_type: codec_instances[1].transform_python})
        self.assertEqual(
            type_registry._decoder_map,
            {MyIntDecoder.bson_type: codec_instances[0].transform_bson})

    def test_initialize_fail(self):
        err_msg = ("Expected an instance of TypeEncoder, TypeDecoder, "
                   "or TypeCodec, got .* instead")
        with self.assertRaisesRegex(TypeError, err_msg):
            TypeRegistry(self.codecs)

        with self.assertRaisesRegex(TypeError, err_msg):
            TypeRegistry([type('AnyType', (object,), {})()])

        err_msg = "fallback_encoder %r is not a callable" % (True,)
        with self.assertRaisesRegex(TypeError, err_msg):
            TypeRegistry([], True)

        err_msg = "fallback_encoder %r is not a callable" % ('hello',)
        with self.assertRaisesRegex(TypeError, err_msg):
            TypeRegistry(fallback_encoder='hello')

    def test_type_registry_repr(self):
        codec_instances = [codec() for codec in self.codecs]
        type_registry = TypeRegistry(codec_instances)
        r = ("TypeRegistry(type_codecs=%r, fallback_encoder=%r)" % (
            codec_instances, None))
        self.assertEqual(r, repr(type_registry))

    def test_type_registry_eq(self):
        codec_instances = [codec() for codec in self.codecs]
        self.assertEqual(
            TypeRegistry(codec_instances), TypeRegistry(codec_instances))

        codec_instances_2 = [codec() for codec in self.codecs]
        self.assertNotEqual(
            TypeRegistry(codec_instances), TypeRegistry(codec_instances_2))

    def test_builtin_types_override_fails(self):
        def run_test(base, attrs):
            msg = (r"TypeEncoders cannot change how built-in types "
                   r"are encoded \(encoder .* transforms type .*\)")
            for pytype in _BUILT_IN_TYPES:
                attrs.update({'python_type': pytype,
                              'transform_python': lambda x: x})
                codec = type('testcodec', (base, ), attrs)
                codec_instance = codec()
                with self.assertRaisesRegex(TypeError, msg):
                    TypeRegistry([codec_instance,])

                # Test only some subtypes as not all can be subclassed.
                if pytype in [bool, type(None), RE_TYPE,]:
                    continue

                class MyType(pytype):
                    pass
                attrs.update({'python_type': MyType,
                              'transform_python': lambda x: x})
                codec = type('testcodec', (base, ), attrs)
                codec_instance = codec()
                with self.assertRaisesRegex(TypeError, msg):
                    TypeRegistry([codec_instance,])

        run_test(TypeEncoder, {})
        run_test(TypeCodec, {'bson_type': Decimal128,
                             'transform_bson': lambda x: x})


class TestCollectionWCustomType(IntegrationTest):
    def setUp(self):
        self.db.test.drop()

    def tearDown(self):
        self.db.test.drop()

    def test_command_errors_w_custom_type_decoder(self):
        db = self.db
        test_doc = {'_id': 1, 'data': 'a'}
        test = db.get_collection('test',
                                 codec_options=UNINT_DECODER_CODECOPTS)

        result = test.insert_one(test_doc)
        self.assertEqual(result.inserted_id, test_doc['_id'])
        with self.assertRaises(DuplicateKeyError):
            test.insert_one(test_doc)

    def test_find_w_custom_type_decoder(self):
        db = self.db
        input_docs = [
            {'x': Int64(k)} for k in [1, 2, 3]]
        for doc in input_docs:
            db.test.insert_one(doc)

        test = db.get_collection(
            'test', codec_options=UNINT_DECODER_CODECOPTS)
        for doc in test.find({}, batch_size=1):
            self.assertIsInstance(doc['x'], UndecipherableInt64Type)

    def test_find_w_custom_type_decoder_and_document_class(self):
        def run_test(doc_cls):
            db = self.db
            input_docs = [
                {'x': Int64(k)} for k in [1, 2, 3]]
            for doc in input_docs:
                db.test.insert_one(doc)

            test = db.get_collection('test', codec_options=CodecOptions(
                type_registry=TypeRegistry([UndecipherableIntDecoder()]),
                document_class=doc_cls))
            for doc in test.find({}, batch_size=1):
                self.assertIsInstance(doc, doc_cls)
                self.assertIsInstance(doc['x'], UndecipherableInt64Type)

        for doc_cls in [RawBSONDocument, OrderedDict]:
            run_test(doc_cls)

    @client_context.require_version_max(4, 1, 0, -1)
    def test_group_w_custom_type(self):
        db = self.db
        test = db.get_collection('test', codec_options=UNINT_CODECOPTS)
        test.insert_many([
            {'sku': 'a', 'qty': UndecipherableInt64Type(2)},
            {'sku': 'b', 'qty': UndecipherableInt64Type(5)},
            {'sku': 'a', 'qty': UndecipherableInt64Type(1)}])

        self.assertEqual([{'sku': 'b', 'qty': UndecipherableInt64Type(5)},],
                         test.group(["sku", "qty"], {"sku": "b"}, {},
                                    "function (obj, prev) { }"))

    def test_aggregate_w_custom_type_decoder(self):
        db = self.db
        db.test.insert_many([
            {'status': 'in progress', 'qty': Int64(1)},
            {'status': 'complete', 'qty': Int64(10)},
            {'status': 'in progress', 'qty': Int64(1)},
            {'status': 'complete', 'qty': Int64(10)},
            {'status': 'in progress', 'qty': Int64(1)},])
        test = db.get_collection(
            'test', codec_options=UNINT_DECODER_CODECOPTS)

        pipeline = [
            {'$match': {'status': 'complete'}},
            {'$group': {'_id': "$status", 'total_qty': {"$sum": "$qty"}}},]
        result = test.aggregate(pipeline)

        res = list(result)[0]
        self.assertEqual(res['_id'], 'complete')
        self.assertIsInstance(res['total_qty'], UndecipherableInt64Type)
        self.assertEqual(res['total_qty'].value, 20)

    def test_distinct_w_custom_type(self):
        self.db.drop_collection("test")

        test = self.db.get_collection('test', codec_options=UNINT_CODECOPTS)
        values = [
            UndecipherableInt64Type(1),
            UndecipherableInt64Type(2),
            UndecipherableInt64Type(3),
            {"b": UndecipherableInt64Type(3)}]
        test.insert_many({"a": val} for val in values)

        self.assertEqual(values, test.distinct("a"))

    def test_map_reduce_w_custom_type(self):
        test = self.db.get_collection(
            'test', codec_options=UPPERSTR_DECODER_CODECOPTS)

        test.insert_many([
            {'_id': 1, 'sku': 'abcd', 'qty': 1},
            {'_id': 2, 'sku': 'abcd', 'qty': 2},
            {'_id': 3, 'sku': 'abcd', 'qty': 3}])

        map = Code("function () {"
                   "  emit(this.sku, this.qty);"
                   "}")
        reduce = Code("function (key, values) {"
                      "  return Array.sum(values);"
                      "}")

        result = test.map_reduce(map, reduce, out={'inline': 1})
        self.assertTrue(isinstance(result, dict))
        self.assertTrue('results' in result)
        self.assertEqual(result['results'][0], {'_id': 'ABCD', 'value': 6})

        result = test.inline_map_reduce(map, reduce)
        self.assertTrue(isinstance(result, list))
        self.assertEqual(1, len(result))
        self.assertEqual(result[0]["_id"], 'ABCD')

        full_result = test.inline_map_reduce(map, reduce,
                                             full_response=True)
        result = full_result['results']
        self.assertTrue(isinstance(result, list))
        self.assertEqual(1, len(result))
        self.assertEqual(result[0]["_id"], 'ABCD')

    def test_find_one_and__w_custom_type_decoder(self):
        db = self.db
        c = db.get_collection('test', codec_options=UNINT_DECODER_CODECOPTS)
        c.insert_one({'_id': 1, 'x': Int64(1)})

        doc = c.find_one_and_update({'_id': 1}, {'$inc': {'x': 1}},
                                    return_document=ReturnDocument.AFTER)
        self.assertEqual(doc['_id'], 1)
        self.assertIsInstance(doc['x'], UndecipherableInt64Type)
        self.assertEqual(doc['x'].value, 2)

        doc = c.find_one_and_replace({'_id': 1}, {'x': Int64(3), 'y': True},
                                     return_document=ReturnDocument.AFTER)
        self.assertEqual(doc['_id'], 1)
        self.assertIsInstance(doc['x'], UndecipherableInt64Type)
        self.assertEqual(doc['x'].value, 3)
        self.assertEqual(doc['y'], True)

        doc = c.find_one_and_delete({'y': True})
        self.assertEqual(doc['_id'], 1)
        self.assertIsInstance(doc['x'], UndecipherableInt64Type)
        self.assertEqual(doc['x'].value, 3)
        self.assertIsNone(c.find_one())

    @ignore_deprecations
    def test_find_and_modify_w_custom_type_decoder(self):
        db = self.db
        c = db.get_collection('test', codec_options=UNINT_DECODER_CODECOPTS)
        c.insert_one({'_id': 1, 'x': Int64(1)})

        doc = c.find_and_modify({'_id': 1}, {'$inc': {'x': Int64(10)}})
        self.assertEqual(doc['_id'], 1)
        self.assertIsInstance(doc['x'], UndecipherableInt64Type)
        self.assertEqual(doc['x'].value, 1)

        doc = c.find_one()
        self.assertEqual(doc['_id'], 1)
        self.assertIsInstance(doc['x'], UndecipherableInt64Type)
        self.assertEqual(doc['x'].value, 11)


class TestGridFileCustomType(IntegrationTest):
    def setUp(self):
        self.db.drop_collection('fs.files')
        self.db.drop_collection('fs.chunks')

    def test_grid_out_custom_opts(self):
        db = self.db.with_options(codec_options=UPPERSTR_DECODER_CODECOPTS)
        one = GridIn(db.fs, _id=5, filename="my_file",
                     contentType="text/html", chunkSize=1000, aliases=["foo"],
                     metadata={"foo": 'red', "bar": 'blue'}, bar=3,
                     baz="hello")
        one.write(b"hello world")
        one.close()

        two = GridOut(db.fs, 5)

        self.assertEqual("my_file", two.name)
        self.assertEqual("my_file", two.filename)
        self.assertEqual(5, two._id)
        self.assertEqual(11, two.length)
        self.assertEqual("text/html", two.content_type)
        self.assertEqual(1000, two.chunk_size)
        self.assertTrue(isinstance(two.upload_date, datetime.datetime))
        self.assertEqual(["foo"], two.aliases)
        self.assertEqual({"foo": 'red', "bar": 'blue'}, two.metadata)
        self.assertEqual(3, two.bar)
        self.assertEqual("5eb63bbbe01eeed093cb22bb8f5acdc3", two.md5)

        for attr in ["_id", "name", "content_type", "length", "chunk_size",
                     "upload_date", "aliases", "metadata", "md5"]:
            self.assertRaises(AttributeError, setattr, two, attr, 5)


class ChangeStreamsWCustomTypesTestMixin(object):
    def change_stream(self, *args, **kwargs):
        return self.watched_target.watch(*args, **kwargs)

    def insert_and_check(self, change_stream, insert_doc,
                         expected_doc):
        self.input_target.insert_one(insert_doc)
        change = next(change_stream)
        self.assertEqual(change['fullDocument'], expected_doc)

    def kill_change_stream_cursor(self, change_stream):
        # Cause a cursor not found error on the next getMore.
        cursor = change_stream._cursor
        address = _CursorAddress(cursor.address, cursor._CommandCursor__ns)
        client = self.input_target.database.client
        client._close_cursor_now(cursor.cursor_id, address)

    def test_simple(self):
        codecopts = CodecOptions(type_registry=TypeRegistry([
            UndecipherableIntEncoder(), UppercaseTextDecoder()]))
        self.create_targets(codec_options=codecopts)

        input_docs = [
            {'_id': UndecipherableInt64Type(1), 'data': 'hello'},
            {'_id': 2, 'data': 'world'},
            {'_id': UndecipherableInt64Type(3), 'data': '!'},]
        expected_docs = [
            {'_id': 1, 'data': 'HELLO'},
            {'_id': 2, 'data': 'WORLD'},
            {'_id': 3, 'data': '!'},]

        change_stream = self.change_stream()

        self.insert_and_check(change_stream, input_docs[0], expected_docs[0])
        self.kill_change_stream_cursor(change_stream)
        self.insert_and_check(change_stream, input_docs[1], expected_docs[1])
        self.kill_change_stream_cursor(change_stream)
        self.insert_and_check(change_stream, input_docs[2], expected_docs[2])

    def test_custom_type_in_pipeline(self):
        codecopts = CodecOptions(type_registry=TypeRegistry([
            UndecipherableIntEncoder(), UppercaseTextDecoder()]))
        self.create_targets(codec_options=codecopts)

        input_docs = [
            {'_id': UndecipherableInt64Type(1), 'data': 'hello'},
            {'_id': 2, 'data': 'world'},
            {'_id': UndecipherableInt64Type(3), 'data': '!'}]
        expected_docs = [
            {'_id': 2, 'data': 'WORLD'},
            {'_id': 3, 'data': '!'}]

        # UndecipherableInt64Type should be encoded with the TypeRegistry.
        change_stream = self.change_stream(
            [{'$match': {'documentKey._id': {
                '$gte': UndecipherableInt64Type(2)}}}])

        self.input_target.insert_one(input_docs[0])
        self.insert_and_check(change_stream, input_docs[1], expected_docs[0])
        self.kill_change_stream_cursor(change_stream)
        self.insert_and_check(change_stream, input_docs[2], expected_docs[1])

    def test_break_resume_token(self):
        # Get one document from a change stream to determine resumeToken type.
        self.create_targets()
        change_stream = self.change_stream()
        self.input_target.insert_one({"data": "test"})
        change = next(change_stream)
        resume_token_decoder = type_obfuscating_decoder_factory(
            type(change['_id']['_data']))

        # Custom-decoding the resumeToken type breaks resume tokens.
        codecopts = CodecOptions(type_registry=TypeRegistry([
            resume_token_decoder(), UndecipherableIntEncoder()]))

        # Re-create targets, change stream and proceed.
        self.create_targets(codec_options=codecopts)

        docs = [{'_id': 1}, {'_id': 2}, {'_id': 3}]

        change_stream = self.change_stream()
        self.insert_and_check(change_stream, docs[0], docs[0])
        self.kill_change_stream_cursor(change_stream)
        self.insert_and_check(change_stream, docs[1], docs[1])
        self.kill_change_stream_cursor(change_stream)
        self.insert_and_check(change_stream, docs[2], docs[2])

    def test_document_class(self):
        def run_test(doc_cls):
            codecopts = CodecOptions(type_registry=TypeRegistry([
                UppercaseTextDecoder(), UndecipherableIntEncoder()]),
                document_class=doc_cls)

            self.create_targets(codec_options=codecopts)
            change_stream = self.change_stream()

            doc = {'a': UndecipherableInt64Type(101), 'b': 'xyz'}
            self.input_target.insert_one(doc)
            change = next(change_stream)

            self.assertIsInstance(change, doc_cls)
            self.assertEqual(change['fullDocument']['a'], 101)
            self.assertEqual(change['fullDocument']['b'], 'XYZ')

        for doc_cls in [OrderedDict, RawBSONDocument]:
            run_test(doc_cls)


class TestCollectionChangeStreamsWCustomTypes(
    IntegrationTest, ChangeStreamsWCustomTypesTestMixin):
    @classmethod
    @client_context.require_version_min(3, 6, 0)
    @client_context.require_no_mmap
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestCollectionChangeStreamsWCustomTypes, cls).setUpClass()

    def tearDown(self):
        self.input_target.drop()

    def create_targets(self, *args, **kwargs):
        self.watched_target = self.db.get_collection(
            'test', *args, **kwargs)
        self.input_target = self.watched_target
        # Ensure the collection exists and is empty.
        self.input_target.insert_one({})
        self.input_target.delete_many({})


class TestDatabaseChangeStreamsWCustomTypes(
    IntegrationTest, ChangeStreamsWCustomTypesTestMixin):
    @classmethod
    @client_context.require_version_min(4, 0, 0)
    @client_context.require_no_mmap
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestDatabaseChangeStreamsWCustomTypes, cls).setUpClass()

    def tearDown(self):
        self.input_target.drop()
        self.client.drop_database(self.watched_target)

    def create_targets(self, *args, **kwargs):
        self.watched_target = self.client.get_database(
            self.db.name, *args, **kwargs)
        self.input_target = self.watched_target.test
        # Insert a record to ensure db, coll are created.
        self.input_target.insert_one({'data': 'dummy'})


class TestClusterChangeStreamsWCustomTypes(
    IntegrationTest, ChangeStreamsWCustomTypesTestMixin):
    @classmethod
    @client_context.require_version_min(4, 0, 0)
    @client_context.require_no_mmap
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestClusterChangeStreamsWCustomTypes, cls).setUpClass()

    def tearDown(self):
        self.input_target.drop()
        self.client.drop_database(self.db)

    def create_targets(self, *args, **kwargs):
        codec_options = kwargs.pop('codec_options', None)
        if codec_options:
            kwargs['type_registry'] = codec_options.type_registry
            kwargs['document_class'] = codec_options.document_class
        self.watched_target = rs_client(*args, **kwargs)
        self.addCleanup(self.watched_target.close)
        self.input_target = self.watched_target[self.db.name].test
        # Insert a record to ensure db, coll are created.
        self.input_target.insert_one({'data': 'dummy'})


if __name__ == "__main__":
    unittest.main()
