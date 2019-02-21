from decimal import Decimal

from bson import BSON, Decimal128
from bson.codec_options import CodecOptions
from bson.errors import InvalidDocument


def _encode_decimal(value):
    """Encodes Python native decimal to BSON type Decimal128."""
    return Decimal128(value)


def encode_vanilla(document):
    try:
        BSON.encode(document)
    except InvalidDocument as exc:
        print("Success! Received expected exception: %r\n" % (exc,))
    else:
        raise AssertionError("Expected encoding error")


def encode_special(document):
    codec_options = CodecOptions(encoder_map={Decimal: _encode_decimal})
    try:
        import ipdb; ipdb.set_trace()
        BSON.encode(document, codec_options=codec_options)
    except:
        raise AssertionError("Unexpected encoding error")
    else:
        print("Success! Did not receive exception")


if __name__ == '__main__':
    document = {'average': Decimal('56.47')}
    encode_vanilla(document)
    encode_special(document)