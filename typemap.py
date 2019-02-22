from decimal import Decimal

from bson import BSON, Decimal128
from bson.codec_options import CodecOptions, TypeRegistry
from bson.errors import InvalidDocument


def _encode_decimal(value):
    """Encodes Python native decimal to BSON type Decimal128."""
    return Decimal128(value)


def _decode_decimal128(value):
    """Decodes BSON type Decimal128 to Python native decimal."""
    return value.to_decimal()


def encode_vanilla(document):
    codec_options = CodecOptions()
    try:
        return BSON.encode(document, codec_options=codec_options)
    except InvalidDocument:
        # expected error
        pass


def encode_special(document):
    type_registry = TypeRegistry(encoder_map={Decimal: _encode_decimal})
    codec_options = CodecOptions(type_registry=type_registry)
    return BSON.encode(document, codec_options=codec_options)


def decode_vanilla(bson_bytes, expected_document):
    codec_options = CodecOptions()
    document = BSON.decode(bson_bytes, codec_options=codec_options)
    assert document==expected_document, 'Vanilla decoding error!'


def decode_special(bson_bytes, expected_document):
    type_registry = TypeRegistry(decoder_map={Decimal128: _decode_decimal128})
    codec_options = CodecOptions(type_registry=type_registry)
    document = BSON.decode(bson_bytes, codec_options=codec_options)
    assert document == expected_document, 'Special decoding error!'


if __name__ == '__main__':
    document = {'average': Decimal('56.47')}
    encode_vanilla(document)
    bsonbytes = encode_special(document)

    decode_vanilla(bsonbytes, {'average': Decimal128('56.47')})
    decode_special(bsonbytes, document)      # roundtrip

    print("DONE!")
