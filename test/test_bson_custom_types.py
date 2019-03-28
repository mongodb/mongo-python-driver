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
                  RE_TYPE,
                  _BUILT_IN_TYPES,
                  _dict_to_bson,
                  _bson_to_dict)
from bson.codec_options import (CodecOptions, TypeCodec, TypeDecoder,
                                TypeEncoder, TypeRegistry)
from bson.errors import InvalidDocument

from test import unittest


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


class CustomTypeTests(object):
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

class TestCustomPythonTypeToBSONMonolithicCodec(CustomTypeTests,
                                                unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        type_registry = TypeRegistry((DecimalCodec(),))
        codec_options = CodecOptions(type_registry=type_registry)
        cls.codecopts = codec_options


class TestCustomPythonTypeToBSONMultiplexedCodec(CustomTypeTests,
                                                 unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        type_registry = TypeRegistry((DecimalEncoder(), DecimalDecoder()))
        codec_options = CodecOptions(type_registry=type_registry)
        cls.codecopts = codec_options


class TestFallbackEncoder(unittest.TestCase):
    def _get_codec_options(self, fallback_encoder):
        type_registry = TypeRegistry(fallback_encoder=fallback_encoder)
        return CodecOptions(type_registry=type_registry)

    def test_simple(self):
        codecopts = self._get_codec_options(lambda x: Decimal128(x))
        document = {'average': Decimal('56.47')}
        bsonbytes = BSON().encode(document, codec_options=codecopts)

        exp_document = {'average': Decimal128('56.47')}
        exp_bsonbytes = BSON().encode(exp_document)
        self.assertEqual(bsonbytes, exp_bsonbytes)

    def test_erroring_fallback_encoder(self):
        codecopts = self._get_codec_options(lambda _: 1/0)

        # fallback converter should not be invoked when encoding known types.
        BSON().encode(
            {'a': 1, 'b': Decimal128('1.01'), 'c': {'arr': ['abc', 3.678]}},
            codec_options=codecopts)

        # expect an error when encoding a custom type.
        document = {'average': Decimal('56.47')}
        with self.assertRaises(ZeroDivisionError):
            BSON().encode(document, codec_options=codecopts)

    def test_noop_fallback_encoder(self):
        codecopts = self._get_codec_options(lambda x: x)
        document = {'average': Decimal('56.47')}
        with self.assertRaises(InvalidDocument):
            BSON().encode(document, codec_options=codecopts)

    def test_type_unencodable_by_fallback_encoder(self):
        def fallback_encoder(value):
            try:
                return Decimal128(value)
            except:
                raise TypeError("cannot encode type %s" % (type(value)))
        codecopts = self._get_codec_options(fallback_encoder)
        document = {'average': Decimal}
        with self.assertRaises(TypeError):
            BSON().encode(document, codec_options=codecopts)


class TestTypeEnDeCodecs(unittest.TestCase):
    def test_instantiation(self):
        msg = "Can't instantiate abstract class .* with abstract methods .*"
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


class TestCustomTypeEncoderAndFallbackEncoderTandem(unittest.TestCase):
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
        expected_bytes = BSON.encode({'x': 123})

        self.assertEqual(BSON.encode(testdoc, codec_options=codecopts),
                         expected_bytes)

    def test_encode_custom_then_fallback(self):
        codecopts = CodecOptions(type_registry=TypeRegistry(
            [self.B2A()], fallback_encoder=self.fallback_encoder_A2BSON))
        testdoc = {'x': self.TypeB(123)}
        expected_bytes = BSON.encode({'x': 123})

        self.assertEqual(BSON.encode(testdoc, codec_options=codecopts),
                         expected_bytes)

    def test_chaining_encoders_fails(self):
        codecopts = CodecOptions(type_registry=TypeRegistry(
            [self.A2B(), self.B2BSON()]))

        with self.assertRaises(InvalidDocument):
            BSON.encode({'x': self.TypeA(123)}, codec_options=codecopts)

    def test_infinite_loop_exceeds_max_recursion_depth(self):
        codecopts = CodecOptions(type_registry=TypeRegistry(
            [self.B2A()], fallback_encoder=self.fallback_encoder_A2B))

        # Raises max recursion depth exceeded error
        with self.assertRaises(RuntimeError):
            BSON.encode({'x': self.TypeA(100)}, codec_options=codecopts)


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
            msg = ("TypeEncoders cannot change how built-in types "
                   "are encoded \(encoder .* transforms type .*\)")
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


if __name__ == "__main__":
    unittest.main()
