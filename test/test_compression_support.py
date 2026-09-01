# Copyright 2026-present MongoDB, Inc.
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

"""Unit tests for compression_support.py."""

from __future__ import annotations

import sys
from unittest.mock import patch

sys.path[0:0] = [""]

from pymongo.compression_support import (
    CompressionSettings,
    SnappyContext,
    ZlibContext,
    ZstdContext,
    _have_snappy,
    _have_zlib,
    _have_zstd,
    _snappy_uncompressed_length,
    decompress,
    validate_compressors,
    validate_zlib_compression_level,
)
from pymongo.errors import ProtocolError
from test import unittest


class TestValidateCompressors(unittest.TestCase):
    def test_string_input_single(self):
        with patch("pymongo.compression_support._have_zlib", return_value=True):
            result = validate_compressors(None, "zlib")
        self.assertEqual(result, ["zlib"])

    def test_string_input_comma_separated(self):
        with (
            patch("pymongo.compression_support._have_zlib", return_value=True),
            patch("pymongo.compression_support._have_snappy", return_value=True),
        ):
            result = validate_compressors(None, "zlib,snappy")
        self.assertEqual(result, ["zlib", "snappy"])

    def test_iterable_input(self):
        with patch("pymongo.compression_support._have_zlib", return_value=True):
            result = validate_compressors(None, ["zlib"])
        self.assertEqual(result, ["zlib"])

    def test_unsupported_compressor_warns_and_removes(self):
        with self.assertWarns(UserWarning) as ctx:
            result = validate_compressors(None, ["bogus"])
        self.assertEqual(result, [])
        self.assertIn("Unsupported compressor: bogus", str(ctx.warning))

    def test_snappy_unavailable_warns_and_removes(self):
        with patch("pymongo.compression_support._have_snappy", return_value=False):
            with self.assertWarns(UserWarning) as ctx:
                result = validate_compressors(None, ["snappy"])
        self.assertEqual(result, [])
        self.assertIn("python-snappy", str(ctx.warning))

    def test_zlib_unavailable_warns_and_removes(self):
        with patch("pymongo.compression_support._have_zlib", return_value=False):
            with self.assertWarns(UserWarning) as ctx:
                result = validate_compressors(None, ["zlib"])
        self.assertEqual(result, [])
        self.assertIn("zlib", str(ctx.warning))

    def test_zstd_unavailable_warns_and_removes_pre_314(self):
        if sys.version_info >= (3, 14):
            self.skipTest("Python 3.14+ uses different warning message")
        with patch("pymongo.compression_support._have_zstd", return_value=False):
            with self.assertWarns(UserWarning) as ctx:
                result = validate_compressors(None, ["zstd"])
        self.assertEqual(result, [])
        self.assertIn("backports.zstd", str(ctx.warning))

    def test_zstd_unavailable_warns_and_removes_314_plus(self):
        if sys.version_info < (3, 14):
            self.skipTest("Only applies to Python 3.14+")
        with patch("pymongo.compression_support._have_zstd", return_value=False):
            with self.assertWarns(UserWarning) as ctx:
                result = validate_compressors(None, ["zstd"])
        self.assertEqual(result, [])
        self.assertIn("compression.zstd", str(ctx.warning))

    def test_multiple_valid_compressors_preserves_order(self):
        with (
            patch("pymongo.compression_support._have_zlib", return_value=True),
            patch("pymongo.compression_support._have_snappy", return_value=True),
        ):
            result = validate_compressors(None, ["zlib", "snappy"])
        self.assertEqual(result, ["zlib", "snappy"])

    def test_empty_list_returns_empty(self):
        result = validate_compressors(None, [])
        self.assertEqual(result, [])


class TestValidateZlibCompressionLevel(unittest.TestCase):
    def test_valid_minimum(self):
        self.assertEqual(validate_zlib_compression_level("level", -1), -1)

    def test_valid_maximum(self):
        self.assertEqual(validate_zlib_compression_level("level", 9), 9)

    def test_non_integer_raises_type_error(self):
        with self.assertRaises(TypeError) as ctx:
            validate_zlib_compression_level("level", "abc")
        self.assertIn("must be an integer", str(ctx.exception))

    def test_out_of_range_raises_value_error(self):
        for value in (-2, 10):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as ctx:
                    validate_zlib_compression_level("level", value)
                self.assertIn("must be between -1 and 9", str(ctx.exception))

    def test_string_integer_is_coerced(self):
        self.assertEqual(validate_zlib_compression_level("level", "5"), 5)


class TestCompressionSettings(unittest.TestCase):
    def _make(self, compressors=None, level=-1):
        return CompressionSettings(compressors or [], level)

    def test_get_context_none_for_empty_or_none(self):
        settings = self._make()
        for arg in ([], None):
            with self.subTest(arg=arg):
                self.assertIsNone(settings.get_compression_context(arg))

    def test_get_context_returns_correct_type(self):
        settings = self._make()
        cases = [("snappy", SnappyContext), ("zlib", ZlibContext), ("zstd", ZstdContext)]
        for name, expected_type in cases:
            with self.subTest(compressor=name):
                self.assertIsInstance(settings.get_compression_context([name]), expected_type)

    def test_get_context_uses_first_compressor(self):
        settings = self._make()
        ctx = settings.get_compression_context(["zlib", "snappy"])
        self.assertIsInstance(ctx, ZlibContext)

    def test_get_context_unknown_returns_none(self):
        settings = self._make()
        ctx = settings.get_compression_context(["unknown"])
        self.assertIsNone(ctx)


class TestZlibContext(unittest.TestCase):
    def setUp(self):
        if not _have_zlib():
            self.skipTest("zlib not available")

    def test_compress_and_decompress_roundtrip(self):
        import zlib

        ctx = ZlibContext(level=-1)
        data = b"hello world" * 100
        compressed = ctx.compress(data)
        self.assertEqual(zlib.decompress(compressed), data)


class TestSnappyUncompressedLength(unittest.TestCase):
    def test_single_byte(self):
        self.assertEqual(_snappy_uncompressed_length(b"\x03"), 3)

    def test_multi_byte(self):
        self.assertEqual(_snappy_uncompressed_length(b"\xac\x02"), 300)

    def test_truncated(self):

        with self.assertRaises(ProtocolError):
            _snappy_uncompressed_length(b"\xff")

    def test_overlong_varint(self):

        with self.assertRaises(ProtocolError):
            _snappy_uncompressed_length(b"\xff" * 5)


class TestDecompress(unittest.TestCase):
    def test_unknown_compressor_id_raises(self):
        with self.assertRaises(ValueError) as ctx:
            decompress(b"data", 99, max_message_size=2**20)
        self.assertIn("Unknown compressorId 99", str(ctx.exception))

    def _assert_roundtrip(self, compressed, compressor_id, data):
        for payload in (compressed, memoryview(compressed)):
            with self.subTest(type=type(payload).__name__):
                self.assertEqual(decompress(payload, compressor_id, max_message_size=2**20), data)

    def test_zlib_roundtrip(self):
        if not _have_zlib():
            self.skipTest("zlib not available")
        import zlib

        data = b"hello world"
        self._assert_roundtrip(zlib.compress(data), ZlibContext.compressor_id, data)

    def test_snappy_roundtrip(self):
        if not _have_snappy():
            self.skipTest("python-snappy not installed")
        data = b"hello world" * 50
        self._assert_roundtrip(SnappyContext.compress(data), SnappyContext.compressor_id, data)

    def test_zstd_roundtrip(self):
        if not _have_zstd():
            self.skipTest("zstd not available")
        data = b"hello world" * 50
        compressed = ZstdContext.compress(data)
        result = decompress(compressed, ZstdContext.compressor_id, max_message_size=2**20)
        self.assertEqual(result, data)


class TestDecompressSizeLimit(unittest.TestCase):
    def test_decompression_peak_memory_bounded(self):
        import tracemalloc
        import zlib

        # High expansion ratio payload (repeated zeros, ~100000:1 ratio)
        payload = zlib.compress(b"\x00" * 100_000_000)
        max_size = 1_000_000

        tracemalloc.start()
        try:
            with self.assertRaises(ProtocolError):
                decompress(payload, ZlibContext.compressor_id, max_message_size=max_size)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        # Peak allocation should stay near the bound rather than the payload size.
        self.assertLess(peak, 10 * max_size)

    def test_zlib_exact_boundary(self):
        import zlib

        # Data that decompresses to exactly max_message_size must be accepted.
        data = b"\x00" * 1000
        payload = zlib.compress(data)
        result = decompress(payload, ZlibContext.compressor_id, max_message_size=1000)
        self.assertEqual(result, data)
        # One byte over the limit must be rejected.
        data_over = b"\x00" * 1001
        payload_over = zlib.compress(data_over)

        with self.assertRaises(ProtocolError):
            decompress(payload_over, ZlibContext.compressor_id, max_message_size=1000)

    def test_snappy_exceeds_max_rejected(self):
        if not _have_snappy():
            self.skipTest("python-snappy not installed")

        data = b"\x00" * 100_000
        payload = SnappyContext.compress(data)
        with self.assertRaises(ProtocolError):
            decompress(payload, SnappyContext.compressor_id, max_message_size=1000)

    def test_snappy_peak_memory_bounded(self):
        if not _have_snappy():
            self.skipTest("python-snappy not installed")
        import tracemalloc

        payload = SnappyContext.compress(b"\x00" * 100_000_000)
        max_size = 1_000_000
        tracemalloc.start()
        try:
            with self.assertRaises(ProtocolError):
                decompress(payload, SnappyContext.compressor_id, max_message_size=max_size)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        # Peak allocation should stay near the bound rather than the payload size.
        self.assertLess(peak, 10 * max_size)

    def test_zstd_peak_memory_bounded(self):
        if not _have_zstd():
            self.skipTest("zstd not available")
        import tracemalloc

        payload = ZstdContext.compress(b"\x00" * 100_000_000)
        max_size = 1_000_000
        tracemalloc.start()
        try:
            with self.assertRaises(ProtocolError):
                decompress(payload, ZstdContext.compressor_id, max_message_size=max_size)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        # Peak allocation should stay near the bound rather than the payload size.
        self.assertLess(peak, 10 * max_size)

    def test_snappy_declared_size_exceeds_max_rejected(self):

        # Varint declaring 2^31 uncompressed bytes; rejected by the declared
        # size pre-check before python-snappy is imported.
        payload = b"\x80\x80\x80\x80\x08"
        with self.assertRaises(ProtocolError):
            decompress(payload, SnappyContext.compressor_id, max_message_size=1000)

    def test_zlib_truncated_rejected(self):
        import zlib

        payload = zlib.compress(b"\x00" * 1000)[:-1]
        with self.assertRaises(ProtocolError):
            decompress(payload, ZlibContext.compressor_id, max_message_size=10_000)

    def test_zstd_truncated_rejected(self):
        if not _have_zstd():
            self.skipTest("zstd not available")

        payload = ZstdContext.compress(b"\x00" * 1000)[:-1]
        with self.assertRaises(ProtocolError):
            decompress(payload, ZstdContext.compressor_id, max_message_size=10_000)

    def test_zlib_trailing_data_rejected(self):
        import zlib

        payload = zlib.compress(b"\x00" * 1000) + b"GARBAGE"
        with self.assertRaises(ProtocolError):
            decompress(payload, ZlibContext.compressor_id, max_message_size=10_000)

    def test_zstd_trailing_data_rejected(self):
        if not _have_zstd():
            self.skipTest("zstd not available")

        payload = ZstdContext.compress(b"\x00" * 1000) + b"GARBAGE"
        with self.assertRaises(ProtocolError):
            decompress(payload, ZstdContext.compressor_id, max_message_size=10_000)


if __name__ == "__main__":
    unittest.main()
