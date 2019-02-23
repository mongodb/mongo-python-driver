from decimal import Decimal

from bson import BSON, Decimal128
from bson.codec_options import (
    CodecOptions, DEFAULT_CODEC_OPTIONS, TypeCodecBase, TypeRegistry)
from bson.errors import InvalidDocument


class DecimalCodec(TypeCodecBase):
    @property
    def bson_type(self):
        return Decimal128

    @property
    def python_type(self):
        return Decimal

    def transform_bson(self, value):
        """Decodes BSON type Decimal128 to Python native decimal."""
        return value.to_decimal()

    def transform_python(self, value):
        """Encodes Python native decimal to BSON type Decimal128."""
        return Decimal128(value)


TYPE_REGISTRY = TypeRegistry(DecimalCodec())
CODEC_OPTIONS = CodecOptions(type_registry=TYPE_REGISTRY)


def encode_vanilla(document):
    try:
        return BSON.encode(document, codec_options=DEFAULT_CODEC_OPTIONS)
    except InvalidDocument:
        # expected error
        pass


def encode_special(document):
    return BSON.encode(document, codec_options=CODEC_OPTIONS)


def decode_vanilla(bson_bytes, expected_document):
    document = BSON.decode(bson_bytes, codec_options=DEFAULT_CODEC_OPTIONS)
    assert document==expected_document, 'Vanilla decoding error!'


def decode_special(bson_bytes, expected_document):
    document = BSON.decode(bson_bytes, codec_options=CODEC_OPTIONS)
    assert document == expected_document, 'Special decoding error!'


if __name__ == '__main__':
    document = {'average': Decimal('56.47')}
    encode_vanilla(document)
    bsonbytes = encode_special(document)

    decode_vanilla(bsonbytes, {'average': Decimal128('56.47')})
    decode_special(bsonbytes, document)      # roundtrip

    print("DONE!")
