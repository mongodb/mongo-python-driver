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

"""Unit tests for ssl_support.py.

These are pure unit tests with no async/sync variants, so this file is not
mirrored by ``just synchro``.
"""

from __future__ import annotations

import os
import sys
import unittest.mock as mock

sys.path[0:0] = [""]

from pymongo.ssl_support import HAVE_SSL, get_ssl_context
from test import unittest
from test.helpers_shared import CA_PEM

_HAVE_PYOPENSSL = False
try:
    from pymongo import pyopenssl_context

    _HAVE_PYOPENSSL = True
except ImportError:
    pass


@unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
class TestSSLCertFileEnvVar(unittest.TestCase):
    def test_uses_default_certs_on_linux(self):
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

    def test_bypasses_default_certs_on_windows(self):
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
    def test_bypasses_default_certs_on_macos_pyopenssl(self):
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


if __name__ == "__main__":
    unittest.main()
