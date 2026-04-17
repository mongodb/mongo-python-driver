# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for pymongo.ocsp_support."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch

sys.path[0:0] = [""]

from test import unittest

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.dsa import DSAPublicKey
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePublicKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from cryptography.hazmat.primitives.asymmetric.x448 import X448PublicKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PublicKey
from cryptography.x509 import (
    AuthorityInformationAccess,
    ExtensionNotFound,
    TLSFeature,
    TLSFeatureType,
)
from cryptography.x509.ocsp import OCSPCertStatus, OCSPResponseStatus
from cryptography.x509.oid import AuthorityInformationAccessOID, ExtendedKeyUsageOID

from pymongo.ocsp_support import (
    _build_ocsp_request,
    _get_certs_by_key_hash,
    _get_certs_by_name,
    _get_extension,
    _get_issuer_cert,
    _get_ocsp_response,
    _ocsp_callback,
    _public_key_hash,
    _verify_response,
    _verify_response_signature,
    _verify_signature,
)


class TestGetIssuerCert(unittest.TestCase):
    def test_found_in_chain(self):
        issuer_name = Mock()
        cert = Mock()
        cert.issuer = issuer_name
        candidate = Mock()
        candidate.subject = issuer_name

        self.assertEqual(_get_issuer_cert(cert, [candidate], None), candidate)

    def test_found_in_trusted_ca(self):
        issuer_name = Mock()
        cert = Mock()
        cert.issuer = issuer_name
        wrong = Mock()
        wrong.subject = Mock()
        trusted = Mock()
        trusted.subject = issuer_name

        self.assertEqual(_get_issuer_cert(cert, [wrong], [trusted]), trusted)

    def test_not_found_no_trusted(self):
        cert = Mock()
        cert.issuer = Mock()
        other = Mock()
        other.subject = Mock()

        self.assertIsNone(_get_issuer_cert(cert, [other], None))

    def test_not_found_with_trusted(self):
        cert = Mock()
        cert.issuer = Mock()
        other = Mock()
        other.subject = Mock()

        self.assertIsNone(_get_issuer_cert(cert, [other], [other]))


class TestVerifySignature(unittest.TestCase):
    def test_rsa_valid(self):
        key = MagicMock(spec=RSAPublicKey)
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 1)
        key.verify.assert_called_once()

    def test_rsa_invalid(self):
        key = MagicMock(spec=RSAPublicKey)
        key.verify.side_effect = InvalidSignature()
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 0)

    def test_dsa_valid(self):
        key = MagicMock(spec=DSAPublicKey)
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 1)
        key.verify.assert_called_once()

    def test_dsa_invalid(self):
        key = MagicMock(spec=DSAPublicKey)
        key.verify.side_effect = InvalidSignature()
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 0)

    def test_ec_valid(self):
        key = MagicMock(spec=EllipticCurvePublicKey)
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 1)
        key.verify.assert_called_once()

    def test_ec_invalid(self):
        key = MagicMock(spec=EllipticCurvePublicKey)
        key.verify.side_effect = InvalidSignature()
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 0)

    def test_x25519_skips_verify(self):
        key = MagicMock(spec=X25519PublicKey)
        # X25519 is for key exchange only; verify is not called, returns 1
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 1)

    def test_x448_skips_verify(self):
        key = MagicMock(spec=X448PublicKey)
        # X448 is for key exchange only; verify is not called, returns 1
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 1)

    def test_other_key_valid(self):
        key = Mock()
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 1)
        key.verify.assert_called_once_with(b"sig", b"data")

    def test_other_key_invalid(self):
        key = Mock()
        key.verify.side_effect = InvalidSignature()
        self.assertEqual(_verify_signature(key, b"sig", Mock(), b"data"), 0)


class TestGetExtension(unittest.TestCase):
    def test_found(self):
        ext = Mock()
        cert = Mock()
        cert.extensions.get_extension_for_class.return_value = ext
        self.assertEqual(_get_extension(cert, TLSFeature), ext)

    def test_not_found(self):
        cert = Mock()
        cert.extensions.get_extension_for_class.side_effect = ExtensionNotFound("missing", Mock())
        self.assertIsNone(_get_extension(cert, TLSFeature))


class TestPublicKeyHash(unittest.TestCase):
    def test_rsa(self):
        key = MagicMock(spec=RSAPublicKey)
        key.public_bytes.return_value = b"rsa_key_bytes"
        cert = Mock()
        cert.public_key.return_value = key
        result = _public_key_hash(cert)
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 20)  # SHA-1 digest

    def test_ec(self):
        key = MagicMock(spec=EllipticCurvePublicKey)
        key.public_bytes.return_value = b"ec_key_bytes"
        cert = Mock()
        cert.public_key.return_value = key
        result = _public_key_hash(cert)
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 20)

    def test_other_key_type(self):
        # Covers the else branch (Ed25519, Ed448, etc.)
        key = Mock()
        key.public_bytes.return_value = b"other_key_bytes"
        cert = Mock()
        cert.public_key.return_value = key
        result = _public_key_hash(cert)
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 20)


class TestGetCertsByKeyHash(unittest.TestCase):
    @patch("pymongo.ocsp_support._public_key_hash")
    def test_match(self, mock_hash):
        issuer = Mock()
        issuer.subject = "issuer_subject"
        cert1 = Mock()
        cert1.issuer = "issuer_subject"
        cert2 = Mock()
        cert2.issuer = "other_subject"
        mock_hash.side_effect = lambda c: b"hash1" if c is cert1 else b"hash2"

        result = _get_certs_by_key_hash([cert1, cert2], issuer, b"hash1")
        self.assertEqual(result, [cert1])

    @patch("pymongo.ocsp_support._public_key_hash")
    def test_no_match(self, mock_hash):
        issuer = Mock()
        issuer.subject = "issuer_subject"
        cert = Mock()
        cert.issuer = "issuer_subject"
        mock_hash.return_value = b"other_hash"

        result = _get_certs_by_key_hash([cert], issuer, b"expected_hash")
        self.assertEqual(result, [])


class TestGetCertsByName(unittest.TestCase):
    def test_match(self):
        issuer = Mock()
        issuer.subject = "issuer"
        cert1 = Mock()
        cert1.subject = "responder"
        cert1.issuer = "issuer"
        cert2 = Mock()
        cert2.subject = "other"
        cert2.issuer = "issuer"

        result = _get_certs_by_name([cert1, cert2], issuer, "responder")
        self.assertEqual(result, [cert1])

    def test_no_match(self):
        issuer = Mock()
        issuer.subject = "issuer"
        cert = Mock()
        cert.subject = "other"
        cert.issuer = "issuer"

        result = _get_certs_by_name([cert], issuer, "responder")
        self.assertEqual(result, [])


class TestBuildOcspRequest(unittest.TestCase):
    @patch("pymongo.ocsp_support._OCSPRequestBuilder")
    def test_builds_request(self, mock_builder_class):
        mock_builder = Mock()
        mock_builder.add_certificate.return_value = mock_builder
        mock_request = Mock()
        mock_builder.build.return_value = mock_request
        mock_builder_class.return_value = mock_builder

        result = _build_ocsp_request(Mock(), Mock())

        self.assertEqual(result, mock_request)
        mock_builder.add_certificate.assert_called_once()
        mock_builder.build.assert_called_once()


class TestVerifyResponseSignature(unittest.TestCase):
    @patch("pymongo.ocsp_support._verify_signature")
    def test_responder_is_issuer_by_name(self, mock_verify_sig):
        mock_verify_sig.return_value = 1
        name = Mock()
        issuer = Mock()
        issuer.subject = name
        response = Mock()
        response.responder_name = name
        response.responder_key_hash = b"rkey"
        response.issuer_key_hash = b"ikey"

        self.assertEqual(_verify_response_signature(issuer, response), 1)

    @patch("pymongo.ocsp_support._verify_signature")
    def test_responder_is_issuer_by_key_hash(self, mock_verify_sig):
        mock_verify_sig.return_value = 1
        issuer = Mock()
        response = Mock()
        response.responder_name = None
        response.responder_key_hash = b"same"
        response.issuer_key_hash = b"same"

        self.assertEqual(_verify_response_signature(issuer, response), 1)

    @patch("pymongo.ocsp_support._verify_signature")
    @patch("pymongo.ocsp_support._get_extension")
    @patch("pymongo.ocsp_support._get_certs_by_name")
    def test_delegate_by_name_success(self, mock_by_name, mock_get_ext, mock_verify_sig):
        mock_verify_sig.return_value = 1
        mock_by_name.return_value = [Mock()]
        ext = Mock()
        ext.value = [ExtendedKeyUsageOID.OCSP_SIGNING]
        mock_get_ext.return_value = ext
        name = Mock()
        issuer = Mock()
        issuer.subject = Mock()
        response = Mock()
        response.responder_name = name
        response.responder_key_hash = b"rkey"
        response.issuer_key_hash = b"ikey"
        response.certificates = []

        self.assertEqual(_verify_response_signature(issuer, response), 1)

    @patch("pymongo.ocsp_support._get_certs_by_key_hash")
    def test_delegate_by_key_hash_no_certs(self, mock_by_hash):
        mock_by_hash.return_value = []
        issuer = Mock()
        response = Mock()
        response.responder_name = None
        response.responder_key_hash = b"rkey"
        response.issuer_key_hash = b"ikey"
        response.certificates = []

        self.assertEqual(_verify_response_signature(issuer, response), 0)

    @patch("pymongo.ocsp_support._get_extension")
    @patch("pymongo.ocsp_support._get_certs_by_name")
    def test_delegate_no_eku_extension(self, mock_by_name, mock_get_ext):
        mock_by_name.return_value = [Mock()]
        mock_get_ext.return_value = None
        name = Mock()
        issuer = Mock()
        issuer.subject = Mock()
        response = Mock()
        response.responder_name = name
        response.responder_key_hash = b"rkey"
        response.issuer_key_hash = b"ikey"
        response.certificates = []

        self.assertEqual(_verify_response_signature(issuer, response), 0)

    @patch("pymongo.ocsp_support._get_extension")
    @patch("pymongo.ocsp_support._get_certs_by_name")
    def test_delegate_ocsp_signing_missing(self, mock_by_name, mock_get_ext):
        mock_by_name.return_value = [Mock()]
        ext = Mock()
        ext.value = []  # OCSP_SIGNING not present
        mock_get_ext.return_value = ext
        name = Mock()
        issuer = Mock()
        issuer.subject = Mock()
        response = Mock()
        response.responder_name = name
        response.responder_key_hash = b"rkey"
        response.issuer_key_hash = b"ikey"
        response.certificates = []

        self.assertEqual(_verify_response_signature(issuer, response), 0)

    @patch("pymongo.ocsp_support._verify_signature")
    @patch("pymongo.ocsp_support._get_extension")
    @patch("pymongo.ocsp_support._get_certs_by_name")
    def test_delegate_cert_sig_fail(self, mock_by_name, mock_get_ext, mock_verify_sig):
        mock_verify_sig.return_value = 0
        mock_by_name.return_value = [Mock()]
        ext = Mock()
        ext.value = [ExtendedKeyUsageOID.OCSP_SIGNING]
        mock_get_ext.return_value = ext
        name = Mock()
        issuer = Mock()
        issuer.subject = Mock()
        response = Mock()
        response.responder_name = name
        response.responder_key_hash = b"rkey"
        response.issuer_key_hash = b"ikey"
        response.certificates = []

        self.assertEqual(_verify_response_signature(issuer, response), 0)

    @patch("pymongo.ocsp_support._verify_signature")
    @patch("pymongo.ocsp_support._get_extension")
    @patch("pymongo.ocsp_support._get_certs_by_key_hash")
    def test_delegate_by_key_hash_success(self, mock_by_hash, mock_get_ext, mock_verify_sig):
        mock_verify_sig.return_value = 1
        mock_by_hash.return_value = [Mock()]
        ext = Mock()
        ext.value = [ExtendedKeyUsageOID.OCSP_SIGNING]
        mock_get_ext.return_value = ext
        issuer = Mock()
        issuer.subject = Mock()
        response = Mock()
        response.responder_name = None
        response.responder_key_hash = b"rkey"
        response.issuer_key_hash = b"ikey"
        response.certificates = []

        self.assertEqual(_verify_response_signature(issuer, response), 1)


class TestVerifyResponse(unittest.TestCase):
    @patch("pymongo.ocsp_support._verify_response_signature", return_value=0)
    def test_sig_fail(self, _):
        self.assertEqual(_verify_response(Mock(), Mock()), 0)

    @patch("pymongo.ocsp_support._verify_response_signature", return_value=1)
    @patch("pymongo.ocsp_support._next_update")
    @patch("pymongo.ocsp_support._this_update")
    def test_valid(self, mock_this, mock_next, _):
        now = datetime.now(tz=timezone.utc)
        mock_this.return_value = now - timedelta(seconds=60)
        mock_next.return_value = now + timedelta(hours=1)
        self.assertEqual(_verify_response(Mock(), Mock()), 1)

    @patch("pymongo.ocsp_support._verify_response_signature", return_value=1)
    @patch("pymongo.ocsp_support._next_update")
    @patch("pymongo.ocsp_support._this_update")
    def test_this_update_in_future(self, mock_this, mock_next, _):
        now = datetime.now(tz=timezone.utc)
        mock_this.return_value = now + timedelta(seconds=60)
        mock_next.return_value = now + timedelta(hours=1)
        self.assertEqual(_verify_response(Mock(), Mock()), 0)

    @patch("pymongo.ocsp_support._verify_response_signature", return_value=1)
    @patch("pymongo.ocsp_support._next_update")
    @patch("pymongo.ocsp_support._this_update")
    def test_next_update_in_past(self, mock_this, mock_next, _):
        now = datetime.now(tz=timezone.utc)
        mock_this.return_value = now - timedelta(hours=2)
        mock_next.return_value = now - timedelta(seconds=60)
        self.assertEqual(_verify_response(Mock(), Mock()), 0)

    @patch("pymongo.ocsp_support._verify_response_signature", return_value=1)
    @patch("pymongo.ocsp_support._next_update")
    @patch("pymongo.ocsp_support._this_update")
    def test_naive_datetime(self, mock_this, mock_next, _):
        # Use UTC-stripped naive time so comparisons don't depend on local timezone
        now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
        mock_this.return_value = now - timedelta(seconds=60)
        mock_next.return_value = now + timedelta(hours=1)
        self.assertEqual(_verify_response(Mock(), Mock()), 1)

    @patch("pymongo.ocsp_support._verify_response_signature", return_value=1)
    @patch("pymongo.ocsp_support._next_update")
    @patch("pymongo.ocsp_support._this_update")
    def test_none_timestamps(self, mock_this, mock_next, _):
        mock_this.return_value = None
        mock_next.return_value = None
        self.assertEqual(_verify_response(Mock(), Mock()), 1)


class TestGetOcspResponse(unittest.TestCase):
    @patch("pymongo.ocsp_support._build_ocsp_request")
    def test_cached(self, mock_build):
        mock_request = Mock()
        mock_build.return_value = mock_request
        mock_response = Mock()
        cache = MagicMock()
        cache.__getitem__.return_value = mock_response

        result = _get_ocsp_response(Mock(), Mock(), "http://ocsp.example.com", cache)
        self.assertEqual(result, mock_response)
        cache.__setitem__.assert_not_called()

    @patch("pymongo._csot.clamp_remaining", return_value=5.0)
    @patch("pymongo.ocsp_support._post")
    @patch("pymongo.ocsp_support._build_ocsp_request")
    def test_http_exception(self, mock_build, mock_post, _):
        from requests.exceptions import RequestException

        mock_build.return_value = Mock()
        cache = MagicMock()
        cache.__getitem__.side_effect = KeyError()
        mock_post.side_effect = RequestException("connection failed")

        result = _get_ocsp_response(Mock(), Mock(), "http://ocsp.example.com", cache)
        self.assertIsNone(result)

    @patch("pymongo._csot.clamp_remaining", return_value=5.0)
    @patch("pymongo.ocsp_support._post")
    @patch("pymongo.ocsp_support._build_ocsp_request")
    def test_non_200_response(self, mock_build, mock_post, _):
        mock_build.return_value = Mock()
        cache = MagicMock()
        cache.__getitem__.side_effect = KeyError()
        http_resp = Mock()
        http_resp.status_code = 503
        mock_post.return_value = http_resp

        result = _get_ocsp_response(Mock(), Mock(), "http://ocsp.example.com", cache)
        self.assertIsNone(result)

    @patch("pymongo._csot.clamp_remaining", return_value=5.0)
    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._post")
    @patch("pymongo.ocsp_support._build_ocsp_request")
    def test_unsuccessful_ocsp_status(self, mock_build, mock_post, mock_load, _):
        mock_build.return_value = Mock()
        cache = MagicMock()
        cache.__getitem__.side_effect = KeyError()
        http_resp = Mock()
        http_resp.status_code = 200
        http_resp.content = b"ocsp_bytes"
        mock_post.return_value = http_resp
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.UNAUTHORIZED
        mock_load.return_value = ocsp_resp

        result = _get_ocsp_response(Mock(), Mock(), "http://ocsp.example.com", cache)
        self.assertIsNone(result)

    @patch("pymongo._csot.clamp_remaining", return_value=5.0)
    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._post")
    @patch("pymongo.ocsp_support._build_ocsp_request")
    def test_serial_number_mismatch(self, mock_build, mock_post, mock_load, _):
        mock_request = Mock()
        mock_request.serial_number = 12345
        mock_build.return_value = mock_request
        cache = MagicMock()
        cache.__getitem__.side_effect = KeyError()
        http_resp = Mock()
        http_resp.status_code = 200
        http_resp.content = b"ocsp_bytes"
        mock_post.return_value = http_resp
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.SUCCESSFUL
        ocsp_resp.serial_number = 99999  # Mismatch
        mock_load.return_value = ocsp_resp

        result = _get_ocsp_response(Mock(), Mock(), "http://ocsp.example.com", cache)
        self.assertIsNone(result)

    @patch("pymongo._csot.clamp_remaining", return_value=5.0)
    @patch("pymongo.ocsp_support._verify_response", return_value=0)
    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._post")
    @patch("pymongo.ocsp_support._build_ocsp_request")
    def test_verify_response_fail(self, mock_build, mock_post, mock_load, mock_verify, _):
        mock_request = Mock()
        mock_request.serial_number = 12345
        mock_build.return_value = mock_request
        cache = MagicMock()
        cache.__getitem__.side_effect = KeyError()
        http_resp = Mock()
        http_resp.status_code = 200
        http_resp.content = b"ocsp_bytes"
        mock_post.return_value = http_resp
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.SUCCESSFUL
        ocsp_resp.serial_number = 12345
        mock_load.return_value = ocsp_resp

        result = _get_ocsp_response(Mock(), Mock(), "http://ocsp.example.com", cache)
        self.assertIsNone(result)

    @patch("pymongo._csot.clamp_remaining", return_value=5.0)
    @patch("pymongo.ocsp_support._verify_response", return_value=1)
    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._post")
    @patch("pymongo.ocsp_support._build_ocsp_request")
    def test_success_caches_response(self, mock_build, mock_post, mock_load, mock_verify, _):
        mock_request = Mock()
        mock_request.serial_number = 12345
        mock_build.return_value = mock_request
        cache = MagicMock()
        cache.__getitem__.side_effect = KeyError()
        http_resp = Mock()
        http_resp.status_code = 200
        http_resp.content = b"ocsp_bytes"
        mock_post.return_value = http_resp
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.SUCCESSFUL
        ocsp_resp.serial_number = 12345
        mock_load.return_value = ocsp_resp

        result = _get_ocsp_response(Mock(), Mock(), "http://ocsp.example.com", cache)
        self.assertEqual(result, ocsp_resp)
        cache.__setitem__.assert_called_once_with(mock_request, ocsp_resp)


class TestOcspCallback(unittest.TestCase):
    def _setup_conn(self, chain_length=1, has_verified_chain=True):
        if has_verified_chain:
            conn = MagicMock()
            pychain = [Mock() for _ in range(chain_length)]
            for item in pychain:
                item.to_cryptography.return_value = Mock()
            conn.get_verified_chain.return_value = pychain
        else:
            conn = Mock(spec=["get_peer_certificate", "get_peer_cert_chain"])
            pychain = [Mock() for _ in range(chain_length)]
            for item in pychain:
                item.to_cryptography.return_value = Mock()
            conn.get_peer_cert_chain.return_value = pychain

        pycert = Mock()
        pycert.to_cryptography.return_value = Mock()
        conn.get_peer_certificate.return_value = pycert
        return conn

    def _setup_user_data(self, check_ocsp_endpoint=True, trusted_ca_certs=None, cache=None):
        user_data = Mock()
        user_data.check_ocsp_endpoint = check_ocsp_endpoint
        user_data.trusted_ca_certs = trusted_ca_certs
        user_data.ocsp_response_cache = cache if cache is not None else Mock()
        return user_data

    def _aia_side_effect(self, uri="http://ocsp.example.com"):
        aia_ext = Mock()
        desc = Mock()
        desc.access_method = AuthorityInformationAccessOID.OCSP
        desc.access_location.value = uri
        aia_ext.value = [desc]

        def side_effect(cert, klass):
            if klass is AuthorityInformationAccess:
                return aia_ext
            return None

        return side_effect

    def test_no_peer_certificate(self):
        conn = MagicMock()
        conn.get_peer_certificate.return_value = None
        self.assertFalse(_ocsp_callback(conn, b"", self._setup_user_data()))

    def test_no_peer_chain(self):
        conn = MagicMock()
        pycert = Mock()
        pycert.to_cryptography.return_value = Mock()
        conn.get_peer_certificate.return_value = pycert
        conn.get_verified_chain.return_value = []
        self.assertFalse(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension")
    def test_no_staple_must_staple_hard_fail(self, mock_get_ext, mock_issuer):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()

        def ext_side_effect(cert, klass):
            if klass is TLSFeature:
                ext = Mock()
                ext.value = [TLSFeatureType.status_request]
                return ext
            return None

        mock_get_ext.side_effect = ext_side_effect
        self.assertFalse(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_no_staple_endpoint_check_disabled(self, _, mock_issuer):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        result = _ocsp_callback(conn, b"", self._setup_user_data(check_ocsp_endpoint=False))
        self.assertTrue(result)

    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_no_staple_no_aia_soft_fail(self, _, mock_issuer):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        self.assertTrue(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension")
    def test_no_staple_aia_no_ocsp_uris_soft_fail(self, mock_get_ext, mock_issuer):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()

        def ext_side_effect(cert, klass):
            if klass is AuthorityInformationAccess:
                aia_ext = Mock()
                desc = Mock()
                desc.access_method = AuthorityInformationAccessOID.CA_ISSUERS  # Not OCSP
                aia_ext.value = [desc]
                return aia_ext
            return None

        mock_get_ext.side_effect = ext_side_effect
        self.assertTrue(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_issuer_cert", return_value=None)
    @patch("pymongo.ocsp_support._get_extension")
    def test_no_staple_no_issuer_hard_fail(self, mock_get_ext, _):
        conn = self._setup_conn()
        mock_get_ext.side_effect = self._aia_side_effect()
        self.assertFalse(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_ocsp_response", return_value=None)
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension")
    def test_no_staple_response_none_soft_fail(self, mock_get_ext, mock_issuer, _):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        mock_get_ext.side_effect = self._aia_side_effect()
        self.assertTrue(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_ocsp_response")
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension")
    def test_no_staple_cert_good(self, mock_get_ext, mock_issuer, mock_get_ocsp):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        mock_get_ext.side_effect = self._aia_side_effect()
        ocsp_resp = Mock()
        ocsp_resp.certificate_status = OCSPCertStatus.GOOD
        mock_get_ocsp.return_value = ocsp_resp
        self.assertTrue(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_ocsp_response")
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension")
    def test_no_staple_cert_revoked(self, mock_get_ext, mock_issuer, mock_get_ocsp):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        mock_get_ext.side_effect = self._aia_side_effect()
        ocsp_resp = Mock()
        ocsp_resp.certificate_status = OCSPCertStatus.REVOKED
        mock_get_ocsp.return_value = ocsp_resp
        self.assertFalse(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_ocsp_response")
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension")
    def test_no_staple_unknown_status_soft_fail(self, mock_get_ext, mock_issuer, mock_get_ocsp):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        mock_get_ext.side_effect = self._aia_side_effect()
        ocsp_resp = Mock()
        ocsp_resp.certificate_status = OCSPCertStatus.UNKNOWN
        mock_get_ocsp.return_value = ocsp_resp
        self.assertTrue(_ocsp_callback(conn, b"", self._setup_user_data()))

    @patch("pymongo.ocsp_support._get_issuer_cert", return_value=None)
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_stapled_no_issuer(self, _, __):
        conn = self._setup_conn()
        self.assertFalse(_ocsp_callback(conn, b"stapled", self._setup_user_data()))

    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_stapled_unsuccessful_status(self, _, mock_issuer, mock_load):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.UNAUTHORIZED
        mock_load.return_value = ocsp_resp
        self.assertFalse(_ocsp_callback(conn, b"stapled", self._setup_user_data()))

    @patch("pymongo.ocsp_support._verify_response", return_value=0)
    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_stapled_verify_fail(self, _, mock_issuer, mock_load, __):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.SUCCESSFUL
        mock_load.return_value = ocsp_resp
        self.assertFalse(_ocsp_callback(conn, b"stapled", self._setup_user_data()))

    @patch("pymongo.ocsp_support._build_ocsp_request")
    @patch("pymongo.ocsp_support._verify_response", return_value=1)
    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_stapled_revoked(self, _, mock_issuer, mock_load, __, mock_build):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        mock_build.return_value = Mock()
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.SUCCESSFUL
        ocsp_resp.certificate_status = OCSPCertStatus.REVOKED
        mock_load.return_value = ocsp_resp
        cache = MagicMock()
        self.assertFalse(_ocsp_callback(conn, b"stapled", self._setup_user_data(cache=cache)))

    @patch("pymongo.ocsp_support._build_ocsp_request")
    @patch("pymongo.ocsp_support._verify_response", return_value=1)
    @patch("pymongo.ocsp_support._load_der_ocsp_response")
    @patch("pymongo.ocsp_support._get_issuer_cert")
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_stapled_good(self, _, mock_issuer, mock_load, __, mock_build):
        conn = self._setup_conn()
        mock_issuer.return_value = Mock()
        mock_build.return_value = Mock()
        ocsp_resp = Mock()
        ocsp_resp.response_status = OCSPResponseStatus.SUCCESSFUL
        ocsp_resp.certificate_status = OCSPCertStatus.GOOD
        mock_load.return_value = ocsp_resp
        cache = MagicMock()
        self.assertTrue(_ocsp_callback(conn, b"stapled", self._setup_user_data(cache=cache)))

    @patch("pymongo.ocsp_support._get_issuer_cert", return_value=None)
    @patch("pymongo.ocsp_support._get_extension", return_value=None)
    def test_uses_peer_cert_chain_fallback(self, _, __):
        # conn without get_verified_chain triggers the fallback path
        conn = self._setup_conn(has_verified_chain=False)
        user_data = self._setup_user_data()
        user_data.trusted_ca_certs = []
        # No AIA (_get_extension returns None) → soft fail → True
        self.assertTrue(_ocsp_callback(conn, b"", user_data))


if __name__ == "__main__":
    unittest.main()
