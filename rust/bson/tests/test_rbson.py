"""Differential tests for the Rust-backed BSON codec (_rbson).

The C extension (``bson._cbson``) is the behavioral reference: wherever the
Rust implementation handles a value, its output must match the C extension's
byte-for-byte on encode and value-for-value (including exact types) on decode.

These tests need no MongoDB server. Run from the repo root::

    .venv/bin/python -m pytest rust/bson/tests -v

after building the crate (from rust/bson: ``maturin develop --release``).
"""

from __future__ import annotations

import datetime
import os
import re
import sys
from pathlib import Path

import pytest

sys.path[0:0] = [str(Path(__file__).parent.parent.parent.parent)]

import _rbson  # noqa: E402

import bson as bson_module  # noqa: E402
from bson import (  # noqa: E402
    CodecOptions,
    _cbson,
)
from bson.errors import InvalidBSON, InvalidDocument  # noqa: E402

pytestmark = pytest.mark.default

OPTIONS = CodecOptions()


def encode_c(doc, check_keys=False, opts=OPTIONS, top_level=True):
    return _cbson._dict_to_bson(doc, check_keys, opts, top_level)


def encode_rust(doc, check_keys=False, opts=OPTIONS, top_level=True):
    return _rbson._dict_to_bson(doc, check_keys, opts, top_level)


def decode_c(data, opts=OPTIONS):
    return _cbson._bson_to_dict(data, opts)


def decode_rust(data, opts=OPTIONS):
    return _rbson._bson_to_dict(data, opts)


def assert_same_encode(doc, check_keys=False, opts=OPTIONS):
    expected = encode_c(doc, check_keys=check_keys, opts=opts)
    actual = encode_rust(doc, check_keys=check_keys, opts=opts)
    assert actual == expected, f"\nexpected: {expected!r}\nactual:   {actual!r}"


def assert_same_decode(data, opts=OPTIONS):
    expected = decode_c(data, opts)
    actual = decode_rust(data, opts)
    assert actual == expected, f"\nexpected: {expected!r}\nactual:   {actual!r}"
    for key in expected:
        assert type(actual[key]) is type(expected[key]), (
            f"value under {key!r}: {type(actual[key])!r} != {type(expected[key])!r}"
        )
    return actual


def assert_same_round_trip(doc, check_keys=False, opts=OPTIONS):
    assert_same_encode(doc, check_keys=check_keys, opts=opts)
    result = assert_same_decode(encode_rust(doc, check_keys=check_keys, opts=opts), opts)
    assert result == doc
    return result


# --- Scaffold: empty document ------------------------------------------------


def test_empty_document_round_trip():
    encoded = encode_rust({})
    assert encoded == b"\x05\x00\x00\x00\x00"
    assert decode_rust(encoded) == {}
    assert_same_round_trip({})


# --- Builtin scalars ---------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "hello",
        "",
        "áéíóú ☃",
        True,
        False,
        None,
        0.0,
        -0.5,
        1.5,
        1e300,
        -2.2250738585072014e-308,
        float("inf"),
        float("-inf"),
        b"\x00\x01\xff",
        b"",
    ],
)
def test_scalar_encode_matches_c(value):
    assert_same_round_trip({"a": value})


def test_nan_encode_matches_c():
    # NaN compares unequal to itself, so compare bytes.
    assert_same_encode({"a": float("nan")})


@pytest.mark.parametrize(
    "value",
    [0, 1, -1, 42, -(2**31), 2**31 - 1, -(2**31) - 1, 2**31, 2**63 - 1, -(2**63)],
)
def test_int_encode_matches_c(value):
    assert_same_round_trip({"a": value})


def test_int64_boundary_overflows():
    for value in (2**63, -(2**63) - 1):
        for encoder in (encode_c, encode_rust):
            with pytest.raises(OverflowError):
                encoder({"a": value})


# --- Containers --------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        [],
        [1, 2, 3],
        [1, True, 3.8, "world"],
        [[1, 2], [3, [4, 5]]],
        [[]],
        [{"nested": [1, {"deep": "doc"}]}],
    ],
)
def test_array_encode_matches_c(value):
    assert_same_round_trip({"a": value})


@pytest.mark.parametrize(
    "value",
    [
        ({},),
        (1, "a", None),
    ],
)
def test_tuple_encodes_like_c_and_decodes_as_list(value):
    # Tuples encode as arrays; they decode back as lists (like the C ext).
    assert_same_encode({"a": value})
    decoded = assert_same_decode(encode_rust({"a": value}))
    assert decoded == {"a": list(value)}


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"b": 1, "c": "two"},
        {"nested": {"deep": {"deeper": [1, 2, None]}}},
        {"unicode_key_☃": "áéíóú"},
        {"empty": {}},
    ],
)
def test_document_encode_matches_c(value):
    assert_same_round_trip({"a": value})


def test_subdocuments_and_arrays_are_containers():
    # Arrays are encoded as documents with numeric string keys.
    encoded = encode_rust({"a": [1]})
    expected = b"\x14\x00\x00\x00\x04a\x00\x0c\x00\x00\x00\x100\x00\x01\x00\x00\x00\x00\x00"
    assert encoded == expected
    assert encoded == encode_c({"a": [1]})
    assert_same_round_trip({"a": [1]})


# --- Top-level _id ordering --------------------------------------------------


def test_top_level_id_is_encoded_first():
    assert_same_round_trip({"z": 1, "_id": 2})
    encoded = encode_rust({"z": 1, "_id": 2})
    assert encoded.index(b"_id") < encoded.index(b"z")
    assert encoded == encode_c({"z": 1, "_id": 2})


def test_non_top_level_id_keeps_document_order():
    encoded = encode_rust({"a": {"z": 1, "_id": 2}})
    assert encoded.index(b"z") < encoded.index(b"_id")
    assert encoded == encode_c({"a": {"z": 1, "_id": 2}})


def test_top_level_false_preserves_order():
    encoded = encode_rust({"z": 1, "_id": 2}, top_level=False)
    assert encoded.index(b"z") < encoded.index(b"_id")
    assert encoded == encode_c({"z": 1, "_id": 2}, top_level=False)


# --- Key validation ----------------------------------------------------------


@pytest.mark.parametrize("key", ["$a", "a.b"])
def test_check_keys_rejects_dollar_and_dot(key):
    for encoder in (encode_c, encode_rust):
        with pytest.raises(InvalidDocument):
            encoder({key: 1}, check_keys=True)
    # Without check_keys the same keys encode.
    assert_same_encode({key: 1})


def test_dollar_and_dot_allowed_without_check_keys():
    assert_same_round_trip({"$a": 1, "a.b": 2})


def test_non_string_key_rejected():
    for encoder in (encode_c, encode_rust):
        with pytest.raises(InvalidDocument):
            encoder({1: "a"})


def test_nul_byte_key_rejected():
    for encoder in (encode_c, encode_rust):
        with pytest.raises(InvalidDocument, match="NULL"):
            encoder({"a\x00b": 1})


def test_non_mapping_top_level_rejected():
    for encoder in (encode_c, encode_rust):
        with pytest.raises(TypeError):
            encoder(42)  # type: ignore[arg-type]


# --- Builtin subclasses ------------------------------------------------------


class StrSubclass(str):
    pass


class IntSubclass(int):
    pass


class FloatSubclass(float):
    pass


class BytesSubclass(bytes):
    pass


class ListSubclass(list):
    pass


class TupleSubclass(tuple):
    pass


class DictSubclass(dict):
    pass


@pytest.mark.parametrize(
    "value",
    [
        StrSubclass("sub"),
        IntSubclass(42),
        FloatSubclass(1.5),
        BytesSubclass(b"bin"),
        ListSubclass([1, 2]),
        TupleSubclass((1, 2)),
        DictSubclass({"a": 1}),
        True,  # exact bool stays bool
    ],
)
def test_builtin_subclasses_encode_like_c(value):
    assert_same_encode({"a": value})


def test_str_subclass_with_lone_surrogate_raises():
    class BadStr(str):
        pass

    for encoder in (encode_c, encode_rust):
        with pytest.raises(UnicodeEncodeError):
            encoder({"a": BadStr("\ud800")})


# --- datetime ----------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        datetime.datetime(2026, 10, 20, 14, 30, 0),
        datetime.datetime(1970, 1, 1),
        datetime.datetime(1969, 12, 31, 23, 59, 59, 999000),
        datetime.datetime(1, 1, 2),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000),
        datetime.datetime(2026, 10, 20, 14, 30, 0, 123000),
        datetime.datetime(2026, 10, 20, 14, 30, 0, 999999),
    ],
)
def test_naive_datetime_encode_matches_c(value):
    assert_same_encode({"a": value})


def test_utc_aware_datetime_encode_matches_c():
    from datetime import timezone

    value = datetime.datetime(2026, 10, 20, 14, 30, 0, tzinfo=timezone.utc)
    assert_same_encode({"a": value})


# --- _type_marker classes (encode) --------------------------------------------

import uuid as uuid_module  # noqa: E402

from bson import (  # noqa: E402
    Binary,
    Code,
    DatetimeMS,
    DBRef,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    Timestamp,
)
from bson.binary import BinaryVectorDtype  # noqa: E402
from bson.codec_options import TypeRegistry  # noqa: E402
from bson.raw_bson import RawBSONDocument  # noqa: E402


def test_binary_subtypes():
    assert_same_encode({"a": Binary(b"ab", 0)})
    assert_same_encode({"a": Binary(b"ab", 2)})
    assert_same_encode({"a": Binary(b"ab", 128)})
    assert_same_encode({"a": Binary(b"ab", 254)})
    # Vectors (subtype 9) write their header bytes unchanged.
    vector = Binary.from_vector([1, 2, 3], BinaryVectorDtype.INT8)
    assert_same_encode({"a": vector})


def test_objectid():
    assert_same_encode({"a": ObjectId(b"123456789012")})
    assert_same_encode({"a": ObjectId("67e0e10a3f2b1c4d5e6f7a8b")})


def test_int64_marker():
    assert_same_encode({"a": Int64(7)})
    assert_same_encode({"a": Int64(-(2**31) - 1)})

    # An Int64 subclass uses the marker too.
    class MyInt64(Int64):
        pass

    assert_same_encode({"a": MyInt64(5)})


def test_code():
    assert_same_encode({"a": Code("x")})
    assert_same_encode({"a": Code("x", None)})
    assert_same_encode({"a": Code("x", {})})
    assert_same_encode({"a": Code("x", {"s": 1})})
    assert_same_encode({"a": Code("áé", {"nested": {"a": [1, "b"]}})})


def test_dbref():
    assert_same_encode({"a": DBRef("coll", 5)})
    assert_same_encode({"a": DBRef("coll", 5, "db")})
    assert_same_encode({"a": DBRef("coll", 5, "db", extra=1)})


def test_regex():
    assert_same_encode({"a": Regex("a*", "i")})
    assert_same_encode({"a": Regex(b"a*", 0)})
    assert_same_encode({"a": re.compile("a*", re.I)})
    assert_same_encode({"a": re.compile("")})


def test_timestamp():
    assert_same_encode({"a": Timestamp(4, 13)})
    assert_same_encode({"a": Timestamp(0, 0)})


def test_min_max_key_and_marker_only_class():
    assert_same_encode({"a": MinKey()})
    assert_same_encode({"a": MaxKey()})
    # A bare class with only the marker attribute encodes like MaxKey.

    class MyMaxKey:
        _type_marker = 127

    assert_same_encode({"a": MyMaxKey()})


def test_datetime_ms():
    assert_same_encode({"a": DatetimeMS(12345)})
    assert_same_encode({"a": DatetimeMS(-1)})


def test_decimal128():
    assert_same_encode({"a": Decimal128("1.5")})
    assert_same_encode({"a": Decimal128("-0.000")})


def test_raw_bson_document_nested():
    raw = encode_c({"inner": "doc"})
    raw_doc = RawBSONDocument(raw)
    assert_same_encode({"a": raw_doc})
    assert_same_encode({"outer": {"nested": raw_doc}})


def test_raw_bson_document_top_level():
    raw = encode_c({"inner": "doc"})
    raw_doc = RawBSONDocument(raw)
    # Top level: the raw bytes are returned as-is.
    assert encode_rust(raw_doc) == raw
    assert encode_c(raw_doc) == raw


def test_uuid_encodings():
    value = uuid_module.UUID("3fbee3e7-9c2e-4f47-a1aa-fc3b7b3d5c1e")
    standard = CodecOptions(uuid_representation=4)
    legacy = CodecOptions(uuid_representation=3)
    java = CodecOptions(uuid_representation=5)
    csharp = CodecOptions(uuid_representation=6)
    assert_same_encode({"a": value}, opts=standard)
    assert_same_encode({"a": value}, opts=legacy)
    assert_same_encode({"a": value}, opts=java)
    assert_same_encode({"a": value}, opts=csharp)
    # UNSPECIFIED raises for bare UUIDs.
    for encoder in (encode_c, encode_rust):
        with pytest.raises(ValueError, match="cannot encode native"):
            encoder({"a": value})


def test_non_utc_aware_datetime():
    from datetime import timedelta, timezone

    value = datetime.datetime(
        2026, 10, 20, 14, 30, 0, tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    assert_same_encode({"a": value})


def test_custom_type_registry():
    from bson.codec_options import TypeEncoder

    class Wrapped:
        def __init__(self, n):
            self.n = n

    # The encoder's type_codecs match by exact type.
    class WrappedEncoder(TypeEncoder):
        type_name = "wrapped"

        python_type = Wrapped

        def transform_python(self, value):
            return {"n": value.n}

    opts = CodecOptions(type_registry=TypeRegistry([WrappedEncoder()]))
    assert_same_encode({"a": Wrapped(3)}, opts=opts)


def test_fallback_encoder():
    def fallback(value):
        return str(value)

    opts = CodecOptions(type_registry=TypeRegistry(fallback_encoder=fallback))

    class Unknown:
        def __str__(self):
            return "unknown!"

    assert_same_encode({"a": Unknown()}, opts=opts)
    # The fallback also rescues oversized ints.
    assert_same_encode({"a": 2**64}, opts=opts)


def test_cannot_encode_unknown_type():
    class Unknown:
        pass

    for encoder in (encode_c, encode_rust):
        with pytest.raises(InvalidDocument, match="cannot encode object"):
            encoder({"a": Unknown()})


# --- decode of BSON classes and codec options (Task 4) ------------------------


def test_decode_objectid():
    result = assert_same_round_trip({"a": ObjectId(b"123456789012")})
    assert isinstance(result["a"], ObjectId)


def test_decode_binary():
    for subtype in (1, 2, 128, 254):
        result = assert_same_round_trip({"a": Binary(b"ab", subtype)})
        assert type(result["a"]) is Binary
        assert result["a"].subtype == subtype
    # Subtype 0 decodes to plain bytes.
    result = assert_same_round_trip({"a": b"ab"})
    assert type(result["a"]) is bytes
    # Vectors round trip as Binary subtype 9.
    vector = Binary.from_vector([1, 2, 3], BinaryVectorDtype.INT8)
    result = assert_same_round_trip({"a": vector})
    assert type(result["a"]) is Binary
    assert result["a"].subtype == 9


def test_decode_binary_subtype2_strips_prefix():
    data = encode_c({"a": Binary(b"ab", 2)})
    result = decode_rust(data)
    assert type(result["a"]) is Binary
    assert result["a"] == Binary(b"ab", 2)


def test_decode_uuid():
    value = uuid_module.UUID("3fbee3e7-9c2e-4f47-a1aa-fc3b7b3d5c1e")
    # STANDARD representation decodes to UUID.
    result = assert_same_round_trip({"a": value}, opts=CodecOptions(uuid_representation=4))
    assert isinstance(result["a"], uuid_module.UUID)
    # PYTHON_LEGACY round trips to UUID too (wire subtype 3).
    result = assert_same_round_trip({"a": value}, opts=CodecOptions(uuid_representation=3))
    assert isinstance(result["a"], uuid_module.UUID)
    # UNSPECIFIED keeps Binary.
    result = assert_same_round_trip({"a": Binary.from_uuid(value)}, opts=CodecOptions())
    assert type(result["a"]) is Binary
    # Subtype/representation mismatches keep Binary:
    # - wire subtype 4 with a non-standard representation configured;
    doc_sub4 = b"\x19\x00\x00\x00\x05a\x00\x0c\x00\x00\x00\x04" + value.bytes[:12] + b"\x00"
    result = assert_same_decode(doc_sub4, CodecOptions(uuid_representation=3))
    assert type(result["a"]) is Binary
    assert result["a"].subtype == 4
    # - wire subtype 3 with the standard representation configured.
    doc_sub3 = b"\x19\x00\x00\x00\x05a\x00\x0c\x00\x00\x00\x03" + value.bytes[:12] + b"\x00"
    result = assert_same_decode(doc_sub3, CodecOptions(uuid_representation=4))
    assert type(result["a"]) is Binary
    assert result["a"].subtype == 3


def test_decode_int64_is_int64():
    result = assert_same_round_trip({"a": Int64(7)})
    assert type(result["a"]) is Int64


def test_decode_code_and_scope():
    result = assert_same_round_trip({"a": Code("x")})
    assert type(result["a"]) is Code
    result = assert_same_round_trip({"a": Code("x", {"s": 1})})
    assert type(result["a"]) is Code
    assert result["a"].scope == {"s": 1}


def test_decode_dbref_hook():
    result = assert_same_decode(encode_c({"a": {"$ref": "c", "$id": 5}}))
    assert isinstance(result["a"], DBRef)
    assert result["a"].collection == "c"
    assert result["a"].id == 5
    result = assert_same_decode(encode_c({"a": {"$ref": "c", "$id": 5, "$db": "d"}}))
    assert isinstance(result["a"], DBRef)
    assert result["a"].database == "d"
    # Extra keys become kwargs.
    result = assert_same_decode(encode_c({"a": {"$ref": "c", "$id": 5, "x": 1}}))
    assert isinstance(result["a"], DBRef)
    assert result["a"].x == 1
    # Non-DBRef-shaped docs stay dicts.
    result = assert_same_round_trip({"a": {"$ref": 1, "$id": 5}})
    assert isinstance(result["a"], dict)
    result = assert_same_round_trip({"a": {"$ref": "c"}})
    assert isinstance(result["a"], dict)


def test_decode_regex():
    result = assert_same_round_trip({"a": Regex("a*", "i")})
    assert type(result["a"]) is Regex
    assert result["a"].pattern == "a*"
    assert result["a"].flags == 2
    # Bytes patterns decode to str patterns (round-trip lossy, like the C).
    assert_same_encode({"a": Regex(b"a*", 0)})
    result = assert_same_decode(encode_rust({"a": Regex(b"a*", 0)}))
    assert type(result["a"]) is Regex
    assert result["a"].pattern == "a*"


def test_decode_timestamp():
    result = assert_same_round_trip({"a": Timestamp(4, 13)})
    assert type(result["a"]) is Timestamp
    assert result["a"].time == 4
    assert result["a"].inc == 13


def test_decode_decimal128():
    result = assert_same_round_trip({"a": Decimal128("1.5")})
    assert type(result["a"]) is Decimal128
    assert result["a"] == Decimal128("1.5")


def test_decode_min_max_key():
    result = assert_same_round_trip({"a": MinKey()})
    assert type(result["a"]) is MinKey
    result = assert_same_round_trip({"a": MaxKey()})
    assert type(result["a"]) is MaxKey


def test_decode_datetime_ms_mode():
    opts = CodecOptions(datetime_conversion=2)  # DATETIME_CLAMP
    result = assert_same_round_trip({"a": datetime.datetime(2026, 10, 20, 14, 30, 0)}, opts=opts)
    assert isinstance(result["a"], datetime.datetime)
    # Out-of-range values clamp to datetime.min/max.
    result = decode_c(encode_c({"a": DatetimeMS(-(2**62))}, opts=opts), opts)
    assert result["a"] == datetime.datetime.min


def test_decode_datetime_ms_type():
    opts = CodecOptions(datetime_conversion=3)  # DATETIME_MS
    result = assert_same_round_trip({"a": DatetimeMS(12345)}, opts=opts)
    assert type(result["a"]) is DatetimeMS


def test_decode_datetime_auto_mode():
    opts = CodecOptions(datetime_conversion=4)  # DATETIME_AUTO
    result = assert_same_round_trip({"a": datetime.datetime(2026, 10, 20, 14, 30, 0)}, opts=opts)
    assert isinstance(result["a"], datetime.datetime)
    result = assert_same_round_trip({"a": DatetimeMS(-(2**62))}, opts=opts)
    assert type(result["a"]) is DatetimeMS


def test_decode_tz_aware():
    from datetime import timedelta, timezone

    opts = CodecOptions(tz_aware=True)
    data = encode_c({"a": datetime.datetime(2026, 10, 20, 14, 30, 0)})
    result = assert_same_decode(data, opts)
    assert result["a"].utcoffset() == timedelta(0)
    # With a tzinfo, times are converted.
    tz = timezone(timedelta(hours=-5))
    opts = CodecOptions(tz_aware=True, tzinfo=tz)
    result = assert_same_decode(data, opts)
    assert result["a"].utcoffset() == timedelta(hours=-5)


def test_decode_unicode_error_handler():
    # Encode an invalid surrogate-free 4-byte sequence manually: decode
    # invalid UTF-8 bytes with the strict handler raises InvalidBSON,
    # "replace" substitutes U+FFFD.
    import struct

    def make_string_doc(raw):
        payload = struct.pack("<i", len(raw) + 1) + raw + b"\x00"
        length = 4 + 1 + 2 + len(payload) + 1  # +1: eoo
        return struct.pack("<i", length) + b"\x02a\x00" + payload + b"\x00"

    bad_utf8 = make_string_doc(b"\xff\xfe")
    with pytest.raises(InvalidBSON):
        decode_rust(bad_utf8)
    opts = CodecOptions(unicode_decode_error_handler="replace")
    result = decode_rust(bad_utf8, opts)
    assert result["a"] == "\ufffd" * 2
    result = decode_c(bad_utf8, opts)
    assert result["a"] == "\ufffd" * 2


def test_decode_decoder_map():
    from bson.codec_options import TypeDecoder

    class MyIntDecoder(TypeDecoder):
        bson_type = Int64

        def transform_bson(self, value):
            return "decoded!"

    opts = CodecOptions(type_registry=TypeRegistry([MyIntDecoder()]))
    data = encode_c({"a": Int64(7)})
    result = assert_same_decode(data, opts)
    assert result["a"] == "decoded!"


def test_decode_all():
    one = encode_c({"a": 1})
    two = encode_c({"b": "two"})
    result = _rbson._decode_all(one + two, OPTIONS)
    expected = _cbson._decode_all(one + two, OPTIONS)
    assert result == expected
    assert result == [{"a": 1}, {"b": "two"}]


def test_element_to_dict():
    data = encode_c({"a": 1, "b": "two"})
    # Element starts at position 4 (after the length prefix).
    name, value, position = _rbson._element_to_dict(data, 4, len(data), OPTIONS, False)
    exp_name, exp_value, exp_position = _cbson._element_to_dict(data, 4, len(data), OPTIONS, False)
    assert (name, value, position) == (exp_name, exp_value, exp_position)
    assert name == "a"
    assert value == 1
    assert position == 4 + 1 + 2 + 4


def test_raw_to_dict():
    from bson.son import SON

    data = encode_c({"a": 1, "b": "two"})
    result_son = SON()
    returned = _rbson._raw_to_dict(data, 4, len(data) - 1, OPTIONS, result_son, False)
    expected = _cbson._raw_to_dict(data, 4, len(data) - 1, OPTIONS, SON(), False)
    assert returned is result_son
    assert result_son == expected
    assert result_son == {"a": 1, "b": "two"}


def test_array_of_documents_to_buffer():
    doc = {"a": 1}
    # A BSON array of documents, as produced by encoding
    # {"0": doc, "1": doc} with a dict of numeric keys.
    array = encode_c({"0": doc, "1": doc})
    result = _rbson._array_of_documents_to_buffer(array)
    expected = _cbson._array_of_documents_to_buffer(array)
    assert result == expected
    assert result == encode_c(doc) + encode_c(doc)


def test_raw_bson_document_class():
    raw = encode_c({"inner": "doc", "n": 5})
    opts = CodecOptions(document_class=RawBSONDocument)
    result = assert_same_decode(raw, opts)
    assert isinstance(result, RawBSONDocument)
    assert bytes(result.raw) == raw
    assert result["inner"] == "doc"


def test_raw_bson_document_memoryview_threshold():
    # Nested raw documents >= 4096 bytes decode from memoryview slices.
    inner = encode_c({"pad": "x" * 5000})
    outer = encode_c({"nested": RawBSONDocument(inner)})
    opts = CodecOptions(document_class=RawBSONDocument)
    result = assert_same_decode(outer, opts)
    assert isinstance(result, RawBSONDocument)
    nested = result["nested"]
    assert isinstance(nested, RawBSONDocument)
    assert isinstance(nested.raw, memoryview)
    assert bytes(nested.raw) == inner
    # Top-level documents pass their bytes buffer through unchanged.
    result = assert_same_decode(inner, opts)
    assert isinstance(result.raw, bytes)


def test_document_class_son():
    from bson.son import SON

    opts = CodecOptions(document_class=SON)
    data = encode_c({"b": 1, "a": 2})
    result = assert_same_decode(data, opts)
    assert type(result) is SON
    assert list(result) == ["b", "a"]


def test_decode_invalid_fails():
    for bad in (b"", b"\x03", b"\x04\x00\x00\x00\x00", b"\x0e\x00\x00\x00\x02a\x00"):
        for decoder in (decode_c, decode_rust):
            with pytest.raises(InvalidBSON):
                decoder(bad)


# --- recursion safety ---------------------------------------------------------


def test_cyclic_documents_raise_recursion_error():
    # Mirrors test_bson.py::test_bad_encode: the C survives cycles via
    # CPython's recursion guard; the Rust codec must too.
    evil_list: dict = {"a": []}
    evil_list["a"].append(evil_list)
    evil_dict: dict = {}
    evil_dict["a"] = evil_dict
    for evil_data in (evil_dict, evil_list):
        for encoder in (encode_c, encode_rust):
            with pytest.raises(RecursionError):
                encoder(evil_data)


# --- review findings: raw-document framing and boundary parity -----------------


def test_raw_bson_document_top_level_false():
    # I-1: a RawBSONDocument with top_level=False must pass the raw bytes
    # through unchanged (the C checks the marker unconditionally).
    raw = encode_c({"inner": "doc"})
    raw_doc = RawBSONDocument(raw)
    assert encode_rust(raw_doc, top_level=False) == raw
    assert encode_rust(raw_doc, top_level=False) == encode_c(raw_doc, top_level=False)


def test_code_with_raw_bson_document_scope():
    # I-2: a RawBSONDocument scope encodes as the raw bytes with no extra
    # framing; the result must decode back to the same document.
    raw = encode_c({"inner": "doc"})
    doc = {"a": Code("x", RawBSONDocument(raw))}
    assert_same_encode(doc)
    decoded = decode_c(encode_rust(doc))
    assert decoded["a"].scope == {"inner": "doc"}


def test_element_to_dict_position_boundary():
    # M-1: position at the end of the buffer raises InvalidBSON.
    data = encode_c({})
    for position, obj_end in ((5, 5), (6, 5)):
        for decoder in (_cbson._element_to_dict, _rbson._element_to_dict):
            with pytest.raises(InvalidBSON):
                decoder(data, position, obj_end, OPTIONS, False)


@pytest.mark.parametrize("bad", [b"\xff\xff\xff\xff\x00", b"\x00\x00\x00\x00\x00"])
def test_decode_invalid_size_prefix_message(bad):
    # M-2: negative/zero size prefixes raise "invalid message size".
    for decoder in (decode_c, decode_rust):
        with pytest.raises(InvalidBSON, match="invalid message size"):
            decoder(bad)


def test_select_impl_rebinds():
    # M-8: the harness's own contract — select_impl() rebinds the entry
    # points (a no-op rebind would be invisible to the C-parity suites).
    from test.performance.bson_impl_select import select_impl

    if os.environ.get("BSON_IMPL", "c") == "rust":
        assert select_impl() == "rust"
        assert bson_module._dict_to_bson is _rbson._dict_to_bson

        assert bson_module.raw_bson._raw_to_dict is _rbson._raw_to_dict


def test_invalid_id_not_wrapped():
    # MIN-1: a failure while writing the _id element is not wrapped with
    # the "Invalid document: " prefix (the C's handle_invalid_doc_error
    # covers only the iteration loops).
    for encoder in (encode_c, encode_rust):
        with pytest.raises(InvalidDocument) as exc_info:
            encoder({"_id": object()})
        assert "Invalid document:" not in str(exc_info.value)
    # Iteration-loop failures ARE wrapped at the top level.
    for encoder in (encode_c, encode_rust):
        with pytest.raises(InvalidDocument) as exc_info:
            encoder({"a": object()})
        assert "Invalid document:" in str(exc_info.value)


def test_non_contiguous_buffer_rejected():
    # MIN-4: a non-contiguous buffer exporter raises ValueError instead of
    # silently decoding wrong bytes.
    values = bytearray(b"\x0d\x00\x00\x00\x02a\x00\x01\x00\x00\x00x\x00\x00")
    view = memoryview(values)[::-1]  # reversed: not C-contiguous
    with pytest.raises(ValueError, match="contiguous"):
        _rbson._bson_to_dict(view, OPTIONS)


def test_invalid_utf8_field_name_wrapped():
    # MIN-2: an invalid-UTF-8 field name raises InvalidBSON (the C wraps
    # the UnicodeDecodeError), for every entry point.
    import struct

    def make(raw_name):
        payload = raw_name + b"\x00" + b"\x10\x01\x00\x00\x00"
        length = 4 + len(payload) + 1
        return struct.pack("<i", length) + payload + b"\x00"

    bad = make(b"\xff\xfe")
    for decoder in (_cbson._bson_to_dict, _rbson._bson_to_dict):
        with pytest.raises(InvalidBSON):
            decoder(bad, OPTIONS)
    for decoder in (_cbson._decode_all, _rbson._decode_all):
        with pytest.raises(InvalidBSON):
            decoder(bad, OPTIONS)
    for decoder in (_cbson._element_to_dict, _rbson._element_to_dict):
        with pytest.raises(InvalidBSON):
            decoder(bad, 4, len(bad), OPTIONS, False)
    # MIN-3: when BOTH the name and the value are malformed, the name
    # error wins (the C decodes the name first).
    both_bad = make(b"\xff" + b"\x00\x02\xff\xff\xff\xff")
    for decoder in (_cbson._bson_to_dict, _rbson._bson_to_dict):
        with pytest.raises(InvalidBSON, match="codec can't decode"):
            decoder(both_bad, OPTIONS)
    from bson.son import SON

    result_son = SON()
    for decoder in (_cbson._raw_to_dict, _rbson._raw_to_dict):
        with pytest.raises(InvalidBSON):
            decoder(bad, 4, len(bad) - 1, OPTIONS, SON(), False)
    assert not result_son


def test_field_name_cache_repeated_decodes():
    # The intern cache must be transparent: the second and third decodes of
    # a document (where every name comes from the cache) match the C.
    doc = {f"field_{i}": i for i in range(50)}
    data = encode_c(doc)
    for _ in range(3):
        assert assert_same_decode(data) == doc


def test_field_name_cache_eviction():
    # Two distinct names mapping to the same cache slot (a direct-mapped
    # cache) must evict each other without ever decoding one for the other.

    def fnv1a(raw):
        h = 0xCBF29CE484222325
        for b in raw:
            h ^= b
            h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
        return h & 0x1FFF  # KEY_CACHE_SLOTS - 1

    keys = [f"k{i}".encode() for i in range(2048)]
    slots = {}
    for key in keys:
        slots.setdefault(fnv1a(key), []).append(key)
    first, second = next(v for v in slots.values() if len(v) >= 2)[:2]
    first, second = first.decode(), second.decode()

    data_one = encode_c({first: 1})
    data_two = encode_c({second: 2})
    data_both = encode_c({first: 1, second: 2})
    for _ in range(5):
        # Each decode evicts the other name (and, for data_both, the doc's
        # own first name is evicted by its second).
        assert assert_same_decode(data_one) == {first: 1}
        assert assert_same_decode(data_two) == {second: 2}
        assert assert_same_decode(data_both) == {first: 1, second: 2}


def test_field_name_cache_long_key():
    # Keys over KEY_CACHE_MAX_KEY_LEN bypass the cache and still decode.
    doc = {"l" * 65: 1, "short": 2}
    data = encode_c(doc)
    for _ in range(2):
        assert assert_same_decode(data) == doc


def test_field_name_cache_not_shared_across_handlers():
    # The cache is only enabled for the strict handler: a successful decode
    # under a non-strict handler must not poison later strict decodes of
    # the same bytes (which must still raise InvalidBSON).
    import struct

    def make(raw_name):
        element = b"\x10" + raw_name + b"\x00" + b"\x01\x00\x00\x00"  # int32
        length = 4 + len(element) + 1
        return struct.pack("<i", length) + element + b"\x00"

    bad = make(b"\xff\xfe")
    opts = CodecOptions(unicode_decode_error_handler="replace")
    for _ in range(3):
        assert decode_rust(bad, opts) == decode_c(bad, opts)
        with pytest.raises(InvalidBSON):
            decode_rust(bad)
        with pytest.raises(InvalidBSON):
            decode_c(bad)


# --- Copilot review findings --------------------------------------------------


def test_dbref_none_db_value_matches_c():
    # A present $db whose value is None must still produce a DBRef (the C's
    # _dbref_hook accepts `database == Py_None`), not a plain dict.
    result = assert_same_decode(encode_c({"a": {"$ref": "c", "$id": 5, "$db": None}}))
    assert isinstance(result["a"], DBRef)
    assert result["a"].database is None


def test_encode_registered_mapping_without_items():
    # A Mapping registered with Mapping.register (not subclassed) need not
    # inherit the ABC's items() mixin: the C iterates keys and looks each
    # value up, so the Rust must not require an items() method.
    import collections.abc

    class RegisteredMapping:
        def __init__(self, data):
            self._data = dict(data)

        def __iter__(self):
            return iter(self._data)

        def __getitem__(self, key):
            return self._data[key]

        def __len__(self):
            return len(self._data)

    collections.abc.Mapping.register(RegisteredMapping)
    doc = {"b": RegisteredMapping({"x": 1}), "a": RegisteredMapping({"y": 2})}
    assert_same_encode(doc)

    # Missing keys during iteration raise KeyError, like the C.
    class MissingValue(RegisteredMapping):
        def __getitem__(self, key):
            if key == "gone":
                raise KeyError(key)
            return super().__getitem__(key)

    collections.abc.Mapping.register(MissingValue)
    with pytest.raises(KeyError):
        encode_rust({"a": MissingValue({"gone": 1})})
    with pytest.raises(KeyError):
        encode_c({"a": MissingValue({"gone": 1})})


def test_encode_list_subclass_len_override():
    # A list/tuple subclass's __len__ must not change the encoded element
    # count: the C sizes arrays with PyList_Size/PyTuple_Size.
    class LyingList(list):
        def __len__(self):
            return 0

    class LyingTuple(tuple):
        def __len__(self):
            return 0

    assert_same_encode({"a": LyingList([1, 2])})
    assert_same_encode({"a": LyingTuple((1, 2))})
    assert decode_rust(encode_rust({"a": LyingList([1, 2])})) == {"a": [1, 2]}


def test_raw_to_dict_no_payload_last_value():
    # The last element of a document may have no payload (None/MinKey/MaxKey):
    # its name's NUL is the byte before the terminator, and inflating such a
    # raw document must not raise "field name too large".
    from bson import MaxKey, MinKey

    for doc in (
        {"a": None},
        {"x": 1, "a": None},
        {"a": MinKey()},
        {"a": MaxKey()},
        {"x": MinKey(), "y": MaxKey()},
    ):
        raw = encode_c(doc)
        expected = _cbson._raw_to_dict(raw, 4, len(raw) - 1, OPTIONS, {}, False)
        result = {}
        actual = _rbson._raw_to_dict(raw, 4, len(raw) - 1, OPTIONS, result, False)
        assert actual is result
        assert result == expected == doc
        # RawBSONDocument's lazy inflation runs the same entry point.
        assert_same_decode(raw, CodecOptions(document_class=RawBSONDocument))


def test_decode_bytearray_mutated_by_decoder():
    # Decoding borrows the exporter's memory only through an owned snapshot:
    # a decoder callback may mutate (even empty) the source buffer mid-decode
    # without corrupting the values decoded from the snapshot.
    from bson.codec_options import TypeDecoder, TypeRegistry

    doc = {"a": "one", "b": "two", "c": "three"}
    buf = bytearray(encode_c(doc))

    class StrDecoder(TypeDecoder):
        bson_type = str

        def transform_bson(self, value):
            buf.clear()
            return value

    opts = CodecOptions(type_registry=TypeRegistry([StrDecoder()]))
    result = decode_rust(bytes(buf), opts)
    assert result == doc
    # bytearray and memoryview inputs decode exactly like bytes.
    assert decode_rust(bytearray(encode_c(doc)), opts) == decode_c(encode_c(doc), opts)
    assert decode_rust(memoryview(encode_c(doc)), opts) == decode_c(encode_c(doc), opts)
