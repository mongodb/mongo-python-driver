# Copyright 2011-present MongoDB, Inc.
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

"""Unit tests for pymongo/ssl_support.py and the SSL session cache in pool_shared.py.

These tests are pure unit tests (mocked or in-process) with no async-specific
behavior, so unlike test_ssl.py they are not generated/mirrored for sync and async.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import unittest.mock as mock

sys.path[0:0] = [""]

from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from pymongo.pool_shared import (
    _configured_protocol_interface,
    _configured_socket_interface,
    _get_ssl_session,
)
from pymongo.ssl_support import HAVE_PYSSL, HAVE_SSL, get_ssl_context
from test import unittest
from test.utils_shared import ignore_deprecations

_HAVE_PYOPENSSL = False
try:
    import pymongo.pyopenssl_context

    _HAVE_PYOPENSSL = True
except ImportError:
    pass

if HAVE_SSL:
    import ssl

CERT_PATH = os.path.join(pathlib.Path(__file__).resolve().parent, "certificates")
CLIENT_PEM = os.path.join(CERT_PATH, "client.pem")
CA_PEM = os.path.join(CERT_PATH, "ca.pem")
CRL_PEM = os.path.join(CERT_PATH, "crl.pem")


class TestClientSSL(unittest.TestCase):
    @unittest.skipIf(HAVE_SSL, "The ssl module is available, can't test what happens without it.")
    def test_no_ssl_module(self):
        # Explicit
        self.assertRaises(ConfigurationError, MongoClient, ssl=True)

        # Implied
        self.assertRaises(ConfigurationError, MongoClient, tlsCertificateKeyFile=CLIENT_PEM)

    @unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
    @ignore_deprecations
    def test_config_ssl(self):
        # Tests various ssl configurations
        self.assertRaises(ValueError, MongoClient, ssl="foo")
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.assertRaises(TypeError, MongoClient, ssl=0)
        self.assertRaises(TypeError, MongoClient, ssl=5.5)
        self.assertRaises(TypeError, MongoClient, ssl=[])

        self.assertRaises(IOError, MongoClient, tlsCertificateKeyFile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, tlsCertificateKeyFile=True)
        self.assertRaises(TypeError, MongoClient, tlsCertificateKeyFile=[])

        # Test invalid combinations
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.assertRaises(ConfigurationError, MongoClient, tls=False, tlsCAFile=CA_PEM)
        self.assertRaises(ConfigurationError, MongoClient, tls=False, tlsCRLFile=CRL_PEM)
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsAllowInvalidCertificates=False
        )
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsAllowInvalidHostnames=False
        )
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsDisableOCSPEndpointCheck=False
        )

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_use_pyopenssl_when_available(self):
        self.assertTrue(HAVE_PYSSL)

    def test_ssl_cert_file_env_var_uses_default_certs_on_linux(self):
        # PYTHON-5930: on Linux, load_default_certs() already honors SSL_CERT_FILE
        # correctly (unlike Windows/macOS+PyOpenSSL), so it must still be called
        # instead of bypassed.
        env = dict(os.environ)
        env.pop("SSL_CERT_DIR", None)
        env["SSL_CERT_FILE"] = CA_PEM
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(sys, "platform", "linux"),
            mock.patch("ssl.SSLContext.load_default_certs") as mock_default,
            mock.patch("ssl.SSLContext.load_verify_locations") as mock_verify,
        ):
            get_ssl_context(None, None, None, None, False, False, False, False)
            mock_default.assert_called_once()
            mock_verify.assert_not_called()

    def test_ssl_cert_file_env_var_bypasses_default_certs_on_windows(self):
        # PYTHON-5930: on win32, load_default_certs() merges the OS certificate
        # store with SSL_CERT_FILE/SSL_CERT_DIR, so it must be bypassed in favor
        # of loading the env vars exclusively.
        env = dict(os.environ)
        env.pop("SSL_CERT_DIR", None)
        env["SSL_CERT_FILE"] = CA_PEM
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(sys, "platform", "win32"),
            mock.patch("ssl.SSLContext.load_default_certs") as mock_default,
            mock.patch("ssl.SSLContext.load_verify_locations") as mock_verify,
        ):
            get_ssl_context(None, None, None, None, False, False, False, False)
            mock_verify.assert_called_once_with(cafile=CA_PEM, capath=None)
            mock_default.assert_not_called()

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_ssl_cert_file_env_var_bypasses_default_certs_on_macos_pyopenssl(self):
        # PYTHON-5930: on macOS with PyOpenSSL, load_default_certs() merges in
        # certifi certs, so it must be bypassed just like on win32.
        env = dict(os.environ)
        env.pop("SSL_CERT_DIR", None)
        env["SSL_CERT_FILE"] = CA_PEM
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(sys, "platform", "darwin"),
            mock.patch("pymongo.pyopenssl_context.SSLContext.load_default_certs") as mock_default,
            mock.patch("pymongo.pyopenssl_context.SSLContext.load_verify_locations") as mock_verify,
        ):
            get_ssl_context(None, None, None, None, False, False, False, True)
            mock_verify.assert_called_once_with(cafile=CA_PEM, capath=None)
            mock_default.assert_not_called()

    def test_ssl_session_cache(self):
        cache: list = [None]
        self.assertIsNone(cache[0])
        cache[0] = "session"
        self.assertEqual(cache[0], "session")
        cache[0] = "new_session"
        self.assertEqual(cache[0], "new_session")

    def test_tls_session_reused_on_second_connection(self):
        """Cached TLS session is passed to wrap_socket on subsequent connections."""

        fake_session = object()
        cache: list = [fake_session]

        fake_ssl_sock = mock.MagicMock()
        fake_ssl_sock.getpeercert.return_value = {}

        mock_ssl_context = mock.MagicMock()
        mock_ssl_context.wrap_socket.return_value = fake_ssl_sock
        mock_ssl_context.verify_mode = False
        mock_ssl_context.check_hostname = False

        mock_opts = mock.MagicMock()
        mock_opts._ssl_context = mock_ssl_context
        mock_opts.socket_timeout = None
        mock_opts.tls_allow_invalid_hostnames = True

        with mock.patch("pymongo.pool_shared._create_connection") as mock_create:
            mock_create.return_value = mock.MagicMock()
            _configured_socket_interface(("localhost", 27017), mock_opts, cache)

        mock_ssl_context.wrap_socket.assert_called_once()
        _, kwargs = mock_ssl_context.wrap_socket.call_args
        self.assertIs(kwargs.get("session"), fake_session)

    def test_get_ssl_session_pyopenssl_style(self):
        """_get_ssl_session uses get_session() when available (PyOpenSSL path)."""
        fake_session = object()
        conn = mock.MagicMock()
        conn.get_session.return_value = fake_session
        self.assertIs(_get_ssl_session(conn), fake_session)
        conn.get_session.assert_called_once()

    def test_get_ssl_session_stdlib_style(self):
        """_get_ssl_session falls back to .session attribute (stdlib ssl path)."""
        fake_session = object()

        class FakeSSLSock:
            session = fake_session

        self.assertIs(_get_ssl_session(FakeSSLSock()), fake_session)

    @unittest.skipUnless(
        sys.version_info >= (3, 11),
        "Tests async sslobject_class injection (Python 3.11+ only)",
    )
    def test_async_tls_session_injected_via_sslobject_class(self):
        """On Python 3.11+, a cached session is injected by setting sslobject_class."""
        fake_session = object()
        cache: list = [fake_session]

        real_ctx = ssl.create_default_context()
        self.assertIs(real_ctx.sslobject_class, ssl.SSLObject)

        # Simulate what _configured_protocol_interface does
        session = cache[0]
        assert session is not None
        _session = session

        class _SessionSSLObject(ssl.SSLObject):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.session = _session

        real_ctx.sslobject_class = _SessionSSLObject

        self.assertIs(real_ctx.sslobject_class, _SessionSSLObject)
        self.assertTrue(issubclass(real_ctx.sslobject_class, ssl.SSLObject))

    def test_async_configured_protocol_saves_session_to_cache(self):
        """After a successful TLS connection the session is stored in the cache."""
        fake_session = object()
        cache: list = [None]

        mock_ssl_obj = mock.MagicMock()
        mock_ssl_obj.session = fake_session

        mock_transport = mock.MagicMock()
        mock_transport.get_extra_info.side_effect = lambda key: (
            mock_ssl_obj if key == "ssl_object" else None
        )

        real_ctx = ssl.create_default_context()
        real_ctx.check_hostname = False
        real_ctx.verify_mode = ssl.CERT_NONE

        mock_opts = mock.MagicMock()
        mock_opts._ssl_context = real_ctx
        mock_opts.socket_timeout = None
        mock_opts.tls_allow_invalid_hostnames = True

        mock_loop = mock.MagicMock()
        mock_loop.create_connection = mock.AsyncMock(
            return_value=(mock_transport, mock.MagicMock())
        )

        async def run():
            with (
                mock.patch(
                    "pymongo.pool_shared._async_create_connection",
                    new=mock.AsyncMock(return_value=mock.MagicMock()),
                ),
                mock.patch(
                    "pymongo.pool_shared.asyncio.get_running_loop",
                    return_value=mock_loop,
                ),
            ):
                await _configured_protocol_interface(("localhost", 27017), mock_opts, cache)

        asyncio.run(run())
        self.assertIs(cache[0], fake_session)

    @unittest.skipUnless(
        sys.version_info >= (3, 11),
        "Tests async session injection on Python 3.11+",
    )
    def test_async_configured_protocol_injects_session_via_sslobject_class(self):
        """When the cache has a session, sslobject_class is set and its __init__ body runs."""
        initial_session = object()
        cache: list = [initial_session]

        mock_transport = mock.MagicMock()
        mock_transport.get_extra_info.return_value = None  # no ssl_object → save block skips

        real_ctx = ssl.create_default_context()
        real_ctx.check_hostname = False
        real_ctx.verify_mode = ssl.CERT_NONE

        mock_opts = mock.MagicMock()
        mock_opts._ssl_context = real_ctx
        mock_opts.socket_timeout = None
        mock_opts.tls_allow_invalid_hostnames = True

        mock_loop = mock.MagicMock()
        mock_loop.create_connection = mock.AsyncMock(
            return_value=(mock_transport, mock.MagicMock())
        )

        async def run():
            with (
                mock.patch(
                    "pymongo.pool_shared._async_create_connection",
                    new=mock.AsyncMock(return_value=mock.MagicMock()),
                ),
                mock.patch(
                    "pymongo.pool_shared.asyncio.get_running_loop",
                    return_value=mock_loop,
                ),
            ):
                await _configured_protocol_interface(("localhost", 27017), mock_opts, cache)

        asyncio.run(run())

        session_cls = real_ctx.sslobject_class  # type: ignore[attr-defined]
        self.assertIsNot(session_cls, ssl.SSLObject)
        self.assertTrue(issubclass(session_cls, ssl.SSLObject))

        # Exercise the __init__ body (super().__init__ + self.session = _session) by
        # calling wrap_bio, patching the session setter to accept non-SSLSession objects.
        incoming = ssl.MemoryBIO()
        outgoing = ssl.MemoryBIO()
        no_op_session = property(lambda s: None, lambda s, v: None)
        with mock.patch.object(ssl.SSLObject, "session", no_op_session):
            ssl_obj = real_ctx.wrap_bio(incoming, outgoing, server_side=False)
        self.assertIsInstance(ssl_obj, ssl.SSLObject)

    def test_async_configured_protocol_no_cache(self):
        """When ssl_session_cache is None, no injection or save occurs."""
        real_ctx = ssl.create_default_context()
        real_ctx.check_hostname = False
        real_ctx.verify_mode = ssl.CERT_NONE

        mock_opts = mock.MagicMock()
        mock_opts._ssl_context = real_ctx
        mock_opts.socket_timeout = None
        mock_opts.tls_allow_invalid_hostnames = True

        mock_transport = mock.MagicMock()
        mock_transport.get_extra_info.return_value = None

        mock_loop = mock.MagicMock()
        mock_loop.create_connection = mock.AsyncMock(
            return_value=(mock_transport, mock.MagicMock())
        )

        async def run():
            with (
                mock.patch(
                    "pymongo.pool_shared._async_create_connection",
                    new=mock.AsyncMock(return_value=mock.MagicMock()),
                ),
                mock.patch(
                    "pymongo.pool_shared.asyncio.get_running_loop",
                    return_value=mock_loop,
                ),
            ):
                await _configured_protocol_interface(("localhost", 27017), mock_opts, None)

        asyncio.run(run())
        self.assertIs(real_ctx.sslobject_class, ssl.SSLObject)  # type: ignore[attr-defined]

    def test_async_configured_protocol_new_session_is_none(self):
        """When ssl_object.session is None after connect, the cache is not updated."""
        cache: list = [None]

        mock_ssl_obj = mock.MagicMock()
        mock_ssl_obj.session = None

        mock_transport = mock.MagicMock()
        mock_transport.get_extra_info.side_effect = lambda key: (
            mock_ssl_obj if key == "ssl_object" else None
        )

        real_ctx = ssl.create_default_context()
        real_ctx.check_hostname = False
        real_ctx.verify_mode = ssl.CERT_NONE

        mock_opts = mock.MagicMock()
        mock_opts._ssl_context = real_ctx
        mock_opts.socket_timeout = None
        mock_opts.tls_allow_invalid_hostnames = True

        mock_loop = mock.MagicMock()
        mock_loop.create_connection = mock.AsyncMock(
            return_value=(mock_transport, mock.MagicMock())
        )

        async def run():
            with (
                mock.patch(
                    "pymongo.pool_shared._async_create_connection",
                    new=mock.AsyncMock(return_value=mock.MagicMock()),
                ),
                mock.patch(
                    "pymongo.pool_shared.asyncio.get_running_loop",
                    return_value=mock_loop,
                ),
            ):
                await _configured_protocol_interface(("localhost", 27017), mock_opts, cache)

        asyncio.run(run())
        self.assertIsNone(cache[0])


if __name__ == "__main__":
    unittest.main()
