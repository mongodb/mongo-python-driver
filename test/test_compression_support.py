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
import unittest
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
    decompress,
    validate_compressors,
    validate_zlib_compression_level,
)


class TestHaveSnappy(unittest.TestCase):
    def test_returns_true_when_available(self):
        try:
            import snappy
        except ImportError:
            self.skipTest("python-snappy not installed")
        self.assertTrue(_have_snappy())

    def test_returns_false_on_import_error(self):
        with patch.dict(sys.modules, {"snappy": None}):
            self.assertFalse(_have_snappy())


class TestHaveZlib(unittest.TestCase):
    def test_returns_true_when_available(self):
        self.assertTrue(_have_zlib())

    def test_returns_false_on_import_error(self):
        with patch.dict(sys.modules, {"zlib": None}):
            self.assertFalse(_have_zlib())


class TestHaveZstd(unittest.TestCase):
    def test_returns_bool(self):
        result = _have_zstd()
        self.assertIsInstance(result, bool)

    def test_returns_false_when_unavailable_pre_314(self):
        if sys.version_info >= (3, 14):
            self.skipTest("Python 3.14+ uses compression.zstd")
        with patch.dict(sys.modules, {"backports": None, "backports.zstd": None}):
            self.assertFalse(_have_zstd())

    def test_returns_false_when_unavailable_314_plus(self):
        if sys.version_info < (3, 14):
            self.skipTest("Only applies to Python 3.14+")
        with patch.dict(sys.modules, {"compression": None, "compression.zstd": None}):
            self.assertFalse(_have_zstd())


class TestValidateCompressors(unittest.TestCase):
    def test_string_input_single(self):
        result = validate_compressors(None, "zlib")
        self.assertEqual(result, ["zlib"])

    def test_string_input_comma_separated(self):
        result = validate_compressors(None, "zlib,snappy")
        self.assertIn("zlib", result)

    def test_iterable_input(self):
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

    def test_valid_zlib_always_included(self):
        result = validate_compressors(None, ["zlib"])
        self.assertEqual(result, ["zlib"])

    def test_multiple_valid_compressors(self):
        result = validate_compressors(None, ["zlib"])
        self.assertIn("zlib", result)

    def test_empty_list_returns_empty(self):
        result = validate_compressors(None, [])
        self.assertEqual(result, [])


class TestValidateZlibCompressionLevel(unittest.TestCase):
    def test_valid_minimum(self):
        self.assertEqual(validate_zlib_compression_level("level", -1), -1)

    def test_valid_zero(self):
        self.assertEqual(validate_zlib_compression_level("level", 0), 0)

    def test_valid_maximum(self):
        self.assertEqual(validate_zlib_compression_level("level", 9), 9)

    def test_valid_midrange(self):
        self.assertEqual(validate_zlib_compression_level("level", 5), 5)

    def test_non_integer_raises_type_error(self):
        with self.assertRaises(TypeError) as ctx:
            validate_zlib_compression_level("level", "abc")
        self.assertIn("must be an integer", str(ctx.exception))

    def test_too_low_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            validate_zlib_compression_level("level", -2)
        self.assertIn("must be between -1 and 9", str(ctx.exception))

    def test_too_high_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            validate_zlib_compression_level("level", 10)
        self.assertIn("must be between -1 and 9", str(ctx.exception))

    def test_string_integer_is_coerced(self):
        self.assertEqual(validate_zlib_compression_level("level", "5"), 5)


class TestCompressionSettings(unittest.TestCase):
    def _make(self, compressors=None, level=-1):
        return CompressionSettings(compressors or [], level)

    def test_get_context_none_when_empty(self):
        settings = self._make()
        self.assertIsNone(settings.get_compression_context([]))

    def test_get_context_none_when_none(self):
        settings = self._make()
        self.assertIsNone(settings.get_compression_context(None))

    def test_get_context_snappy(self):
        settings = self._make()
        ctx = settings.get_compression_context(["snappy"])
        self.assertIsInstance(ctx, SnappyContext)

    def test_get_context_zlib(self):
        settings = self._make(level=6)
        ctx = settings.get_compression_context(["zlib"])
        self.assertIsInstance(ctx, ZlibContext)
        self.assertEqual(ctx.level, 6)

    def test_get_context_zstd(self):
        settings = self._make()
        ctx = settings.get_compression_context(["zstd"])
        self.assertIsInstance(ctx, ZstdContext)

    def test_get_context_uses_first_compressor(self):
        settings = self._make(level=1)
        ctx = settings.get_compression_context(["zlib", "snappy"])
        self.assertIsInstance(ctx, ZlibContext)

    def test_get_context_unknown_returns_none(self):
        settings = self._make()
        ctx = settings.get_compression_context(["unknown"])
        self.assertIsNone(ctx)


class TestZlibContext(unittest.TestCase):
    def test_compress_and_decompress_roundtrip(self):
        import zlib

        ctx = ZlibContext(level=-1)
        data = b"hello world" * 100
        compressed = ctx.compress(data)
        self.assertEqual(zlib.decompress(compressed), data)

    def test_compress_level_stored(self):
        ctx = ZlibContext(level=6)
        self.assertEqual(ctx.level, 6)

    def test_compressor_id(self):
        self.assertEqual(ZlibContext.compressor_id, 2)


class TestDecompress(unittest.TestCase):
    def test_unknown_compressor_id_raises(self):
        with self.assertRaises(ValueError) as ctx:
            decompress(b"data", 99)
        self.assertIn("Unknown compressorId 99", str(ctx.exception))

    def test_zlib_roundtrip(self):
        import zlib

        data = b"hello world"
        compressed = zlib.compress(data)
        result = decompress(compressed, ZlibContext.compressor_id)
        self.assertEqual(result, data)

    def test_zlib_with_memoryview(self):
        import zlib

        data = b"test data"
        compressed = zlib.compress(data)
        result = decompress(memoryview(compressed), ZlibContext.compressor_id)
        self.assertEqual(result, data)


if __name__ == "__main__":
    unittest.main()
