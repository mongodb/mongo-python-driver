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

"""Unit tests for pyopenssl_context.py.

These tests require PyOpenSSL (install via: pip install pymongo[ocsp]).
Tests are automatically skipped when PyOpenSSL is not available.
"""

from __future__ import annotations

import ssl
import sys
from unittest.mock import patch

sys.path[0:0] = [""]

from test import unittest

try:
    from pymongo import pyopenssl_context as _ctx_module
    from pymongo.pyopenssl_context import (
        PROTOCOL_SSLv23,
        SSLContext,
        _is_ip_address,
        _ragged_eof,
    )

    _HAVE_PYOPENSSL = True
except ImportError:
    _HAVE_PYOPENSSL = False


# ---------------------------------------------------------------------------
# Pure functions (no SSL context required)
# ---------------------------------------------------------------------------


class TestIsIpAddress(unittest.TestCase):
    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_ipv4(self):
        self.assertTrue(_is_ip_address("192.168.1.1"))

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_ipv6(self):
        self.assertTrue(_is_ip_address("::1"))
        self.assertTrue(_is_ip_address("2001:db8::1"))

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_hostname_is_not_ip(self):
        self.assertFalse(_is_ip_address("example.com"))
        self.assertFalse(_is_ip_address("localhost"))

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_invalid_string_returns_false(self):
        self.assertFalse(_is_ip_address("not-an-ip"))

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_unicode_error_returns_false(self):
        # UnicodeError path: some inputs that can't be decoded.
        # ip_address raises UnicodeError for byte strings with non-ASCII.
        self.assertFalse(_is_ip_address(b"\xff\xfe"))


class TestRaggedEof(unittest.TestCase):
    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_matching_args_returns_true(self):
        from OpenSSL.SSL import SysCallError

        exc = SysCallError(-1, "Unexpected EOF")
        self.assertTrue(_ragged_eof(exc))

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_non_matching_args_returns_false(self):
        from OpenSSL.SSL import SysCallError

        exc = SysCallError(0, "something else")
        self.assertFalse(_ragged_eof(exc))

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_wrong_code_returns_false(self):
        from OpenSSL.SSL import SysCallError

        exc = SysCallError(5, "Unexpected EOF")
        self.assertFalse(_ragged_eof(exc))


# ---------------------------------------------------------------------------
# SSLContext — construction and properties
# ---------------------------------------------------------------------------


class TestSSLContextConstruction(unittest.TestCase):
    def _make(self):
        return SSLContext(PROTOCOL_SSLv23)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_protocol_property(self):
        ctx = self._make()
        self.assertEqual(ctx.protocol, PROTOCOL_SSLv23)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_default_check_hostname(self):
        ctx = self._make()
        self.assertTrue(ctx.check_hostname)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_set_check_hostname_false(self):
        ctx = self._make()
        ctx.check_hostname = False
        self.assertFalse(ctx.check_hostname)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_set_check_hostname_invalid_raises(self):
        ctx = self._make()
        with self.assertRaises(TypeError):
            ctx.check_hostname = "yes"

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_default_check_ocsp_endpoint(self):
        ctx = self._make()
        self.assertTrue(ctx.check_ocsp_endpoint)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_set_check_ocsp_endpoint_false(self):
        ctx = self._make()
        ctx.check_ocsp_endpoint = False
        self.assertFalse(ctx.check_ocsp_endpoint)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_verify_mode_roundtrip(self):
        ctx = self._make()
        ctx.verify_mode = ssl.CERT_REQUIRED
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_verify_mode_cert_none(self):
        ctx = self._make()
        ctx.verify_mode = ssl.CERT_NONE
        self.assertEqual(ctx.verify_mode, ssl.CERT_NONE)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_options_setter_and_getter(self):
        ctx = self._make()
        from pymongo.pyopenssl_context import OP_NO_SSLv3

        ctx.options = OP_NO_SSLv3
        self.assertTrue(ctx.options & OP_NO_SSLv3)


# ---------------------------------------------------------------------------
# SSLContext._load_certifi
# ---------------------------------------------------------------------------


class TestLoadCertifi(unittest.TestCase):
    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_raises_when_certifi_unavailable(self):
        from pymongo.errors import ConfigurationError

        ctx = SSLContext(PROTOCOL_SSLv23)
        with patch.object(_ctx_module, "_HAVE_CERTIFI", False):
            with self.assertRaises(ConfigurationError) as exc_ctx:
                ctx._load_certifi()
        self.assertIn("certifi", str(exc_ctx.exception))

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_loads_when_certifi_available(self):
        if not _ctx_module._HAVE_CERTIFI:
            self.skipTest("certifi not installed")
        ctx = SSLContext(PROTOCOL_SSLv23)
        ctx.verify_mode = ssl.CERT_NONE
        # Should not raise.
        ctx._load_certifi()


# ---------------------------------------------------------------------------
# SSLContext.load_default_certs — platform branching
# ---------------------------------------------------------------------------


class TestLoadDefaultCerts(unittest.TestCase):
    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_darwin_calls_load_certifi(self):
        with patch.object(_ctx_module._sys, "platform", "darwin"):
            with patch.object(SSLContext, "_load_certifi") as mock_certifi:
                with patch("OpenSSL.SSL.Context.set_default_verify_paths"):
                    ctx = SSLContext(PROTOCOL_SSLv23)
                    ctx.load_default_certs()
        mock_certifi.assert_called()

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_win32_calls_load_wincerts(self):
        with patch.object(_ctx_module._sys, "platform", "win32"):
            with patch.object(SSLContext, "_load_wincerts") as mock_wincerts:
                with patch("OpenSSL.SSL.Context.set_default_verify_paths"):
                    ctx = SSLContext(PROTOCOL_SSLv23)
                    ctx.load_default_certs()
        calls = [call.args[0] for call in mock_wincerts.call_args_list]
        self.assertIn("CA", calls)
        self.assertIn("ROOT", calls)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_win32_falls_back_to_certifi_on_exception(self):
        with patch.object(_ctx_module._sys, "platform", "win32"):
            with patch.object(SSLContext, "_load_wincerts", side_effect=Exception("no certs")):
                with patch.object(SSLContext, "_load_certifi") as mock_certifi:
                    with patch("OpenSSL.SSL.Context.set_default_verify_paths"):
                        ctx = SSLContext(PROTOCOL_SSLv23)
                        ctx.load_default_certs()
        mock_certifi.assert_called()

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_linux_no_certifi_call(self):
        with patch.object(_ctx_module._sys, "platform", "linux"):
            with patch.object(SSLContext, "_load_certifi") as mock_certifi:
                with patch("OpenSSL.SSL.Context.set_default_verify_paths"):
                    ctx = SSLContext(PROTOCOL_SSLv23)
                    ctx.load_default_certs()
        mock_certifi.assert_not_called()

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_calls_set_default_verify_paths(self):
        with patch.object(_ctx_module._sys, "platform", "linux"):
            ctx = SSLContext(PROTOCOL_SSLv23)
            with patch.object(ctx._ctx, "set_default_verify_paths") as mock_sdvp:
                ctx.load_default_certs()
        mock_sdvp.assert_called_once()


# ---------------------------------------------------------------------------
# SSLContext.set_default_verify_paths
# ---------------------------------------------------------------------------


class TestSetDefaultVerifyPaths(unittest.TestCase):
    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_delegates_to_ctx(self):
        ctx = SSLContext(PROTOCOL_SSLv23)
        with patch.object(ctx._ctx, "set_default_verify_paths") as mock_sdvp:
            ctx.set_default_verify_paths()
        mock_sdvp.assert_called_once()


# ---------------------------------------------------------------------------
# SSLContext.load_verify_locations
# ---------------------------------------------------------------------------


class TestLoadVerifyLocations(unittest.TestCase):
    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_delegates_to_ctx(self):
        ctx = SSLContext(PROTOCOL_SSLv23)
        with patch.object(ctx._ctx, "load_verify_locations") as mock_lvl:
            ctx.load_verify_locations(cafile="/tmp/ca.pem")
        mock_lvl.assert_called_once_with("/tmp/ca.pem", None)


if __name__ == "__main__":
    unittest.main()
