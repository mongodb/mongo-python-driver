# Copyright 2020-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Support for requesting and verifying OCSP responses."""

import logging as _logging

from datetime import datetime as _datetime

from cryptography.exceptions import InvalidSignature as _InvalidSignature
from cryptography.hazmat.backends import default_backend as _default_backend
from cryptography.hazmat.primitives.asymmetric.dsa import (
    DSAPublicKey as _DSAPublicKey)
from cryptography.hazmat.primitives.asymmetric.ec import (
    ECDSA as _ECDSA,
    EllipticCurvePublicKey as _EllipticCurvePublicKey)
from cryptography.hazmat.primitives.asymmetric.padding import (
    PKCS1v15 as _PKCS1v15)
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPublicKey as _RSAPublicKey)
from cryptography.hazmat.primitives.hashes import (
        Hash as _Hash,
        SHA1 as _SHA1)
from cryptography.hazmat.primitives.serialization import (
    Encoding as _Encoding,
    PublicFormat as _PublicFormat)
from cryptography.x509 import (
    AuthorityInformationAccess as _AuthorityInformationAccess,
    ExtendedKeyUsage as _ExtendedKeyUsage,
    ExtensionNotFound as _ExtensionNotFound,
    TLSFeature as _TLSFeature,
    TLSFeatureType as _TLSFeatureType)
from cryptography.x509.oid import (
    AuthorityInformationAccessOID as _AuthorityInformationAccessOID,
    ExtendedKeyUsageOID as _ExtendedKeyUsageOID)
from cryptography.x509.ocsp import (
    load_der_ocsp_response as _load_der_ocsp_response,
    OCSPCertStatus as _OCSPCertStatus,
    OCSPRequestBuilder as _OCSPRequestBuilder,
    OCSPResponseStatus as _OCSPResponseStatus)
from requests import post as _post
from requests.exceptions import RequestException as _RequestException

# Note: the functions in this module generally return 1 or 0. The reason
# is simple. The entry point, ocsp_callback, is registered as a callback
# with OpenSSL through PyOpenSSL. The callback must return 1 (success) or
# 0 (failure).

_LOGGER = _logging.getLogger(__name__)


def _get_issuer_cert(cert, chain):
    issuer_name = cert.issuer
    for candidate in chain:
        if candidate.subject == issuer_name:
            return candidate
    return None


def _verify_signature(key, signature, algorithm, data):
    # See cryptography.x509.Certificate.public_key
    # for the public key types.
    try:
        if isinstance(key, _RSAPublicKey):
            key.verify(signature, data, _PKCS1v15(), algorithm)
        elif isinstance(key, _DSAPublicKey):
            key.verify(signature, data, algorithm)
        elif isinstance(key, _EllipticCurvePublicKey):
            key.verify(signature, data, _ECDSA(algorithm))
        else:
            key.verify(signature, data)
    except _InvalidSignature:
        return 0
    return 1


def _get_extension(cert, klass):
    try:
        return cert.extensions.get_extension_for_class(klass)
    except _ExtensionNotFound:
        return None


def _public_key_hash(cert):
    public_key = cert.public_key()
    # https://tools.ietf.org/html/rfc2560#section-4.2.1
    # "KeyHash ::= OCTET STRING -- SHA-1 hash of responder's public key
    # (excluding the tag and length fields)"
    # https://stackoverflow.com/a/46309453/600498
    if isinstance(public_key, _RSAPublicKey):
        pbytes = public_key.public_bytes(
            _Encoding.DER, _PublicFormat.PKCS1)
    elif isinstance(public_key, _EllipticCurvePublicKey):
        pbytes = public_key.public_bytes(
            _Encoding.X962, _PublicFormat.UncompressedPoint)
    else:
        pbytes = public_key.public_bytes(
            _Encoding.DER, _PublicFormat.SubjectPublicKeyInfo)
    digest = _Hash(_SHA1(), backend=_default_backend())
    digest.update(pbytes)
    return digest.finalize()


def _get_certs_by_key_hash(certificates, issuer, responder_key_hash):
    return [
        cert for cert in certificates
        if _public_key_hash(cert) == responder_key_hash and
        cert.issuer == issuer.subject]


def _get_certs_by_name(certificates, issuer, responder_name):
    return [
        cert for cert in certificates
        if cert.subject == responder_name and
        cert.issuer == issuer.subject]


def _verify_response_signature(issuer, response):
    # Response object will have a responder_name or responder_key_hash
    # not both.
    name = response.responder_name
    rkey_hash = response.responder_key_hash
    ikey_hash = response.issuer_key_hash
    if name is not None and name == issuer.subject or rkey_hash == ikey_hash:
        _LOGGER.debug("Responder is issuer")
        # Responder is the issuer
        responder_cert = issuer
    else:
        _LOGGER.debug("Responder is a delegate")
        # Responder is a delegate
        # https://tools.ietf.org/html/rfc6960#section-2.6
        # RFC6960, Section 3.2, Number 3
        certs = response.certificates
        if response.responder_name is not None:
            responder_certs = _get_certs_by_name(certs, issuer, name)
            _LOGGER.debug("Using responder name")
        else:
            responder_certs = _get_certs_by_key_hash(certs, issuer, rkey_hash)
            _LOGGER.debug("Using key hash")
        if not responder_certs:
            _LOGGER.debug("No matching or valid responder certs.")
            return 0
        # XXX: Can there be more than one? If so, should we try each one
        # until we find one that passes signature verification?
        responder_cert = responder_certs[0]

        # RFC6960, Section 3.2, Number 4
        ext = _get_extension(responder_cert, _ExtendedKeyUsage)
        if not ext or _ExtendedKeyUsageOID.OCSP_SIGNING not in ext.value:
            _LOGGER.debug("Delegate not authorized for OCSP signing")
            return 0
        if not _verify_signature(
                issuer.public_key(),
                responder_cert.signature,
                responder_cert.signature_hash_algorithm,
                responder_cert.tbs_certificate_bytes):
            _LOGGER.debug("Delegate signature verification failed")
            return 0
    # RFC6960, Section 3.2, Number 2
    ret = _verify_signature(
        responder_cert.public_key(),
        response.signature,
        response.signature_hash_algorithm,
        response.tbs_response_bytes)
    if not ret:
        _LOGGER.debug("Response signature verification failed")
    return ret


def _request_ocsp(cert, issuer, uri):
    # https://cryptography.io/en/latest/x509/ocsp/#creating-requests
    builder = _OCSPRequestBuilder()
    # add_certificate returns a new instance
    builder = builder.add_certificate(cert, issuer, _SHA1())
    ocsp_request = builder.build()
    try:
        response = _post(
            uri,
            data=ocsp_request.public_bytes(_Encoding.DER),
            headers={'Content-Type': 'application/ocsp-request'},
            timeout=5)
    except _RequestException:
        _LOGGER.debug("HTTP request failed")
        return None
    if response.status_code != 200:
        _LOGGER.debug("HTTP request returned %d", response.status_code)
        return None
    ocsp_response = _load_der_ocsp_response(response.content)
    _LOGGER.debug(
        "OCSP response status: %r", ocsp_response.response_status)
    if ocsp_response.response_status != _OCSPResponseStatus.SUCCESSFUL:
        return None
    # RFC6960, Section 3.2, Number 1. Only relevant if we need to
    # talk to the responder directly.
    # Accessing response.serial_number raises if response status is not
    # SUCCESSFUL.
    if ocsp_response.serial_number != ocsp_request.serial_number:
        _LOGGER.debug("Response serial number does not match request")
        return None
    return ocsp_response


def _verify_response(issuer, response):
    _LOGGER.debug("Verifying response")
    # RFC6960, Section 3.2, Number 2, 3 and 4 happen here.
    res = _verify_response_signature(issuer, response)
    if not res:
        return 0

    # Note that we are not using a "tolerence period" as discussed in
    # https://tools.ietf.org/rfc/rfc5019.txt?
    now = _datetime.utcnow()
    # RFC6960, Section 3.2, Number 5
    if response.this_update > now:
        _LOGGER.debug("thisUpdate is in the future")
        return 0
    # RFC6960, Section 3.2, Number 6
    if response.next_update and response.next_update < now:
        _LOGGER.debug("nextUpdate is in the past")
        return 0
    return 1


def ocsp_callback(conn, ocsp_bytes, user_data):
    """Callback for use with OpenSSL.SSL.Context.set_ocsp_client_callback."""
    cert = conn.get_peer_certificate().to_cryptography()
    chain = [cer.to_cryptography() for cer in conn.get_peer_cert_chain()]
    issuer = _get_issuer_cert(cert, chain)
    if issuer is None:
        _LOGGER.debug("No issuer cert?")
        return 0
    must_staple = False
    # https://tools.ietf.org/html/rfc7633#section-4.2.3.1
    ext = _get_extension(cert, _TLSFeature)
    if ext is not None:
        for feature in ext.value:
            if feature == _TLSFeatureType.status_request:
                _LOGGER.debug("Peer presented a must-staple cert")
                must_staple = True
                break
    # No stapled OCSP response
    if ocsp_bytes == b'':
        _LOGGER.debug("Peer did not staple an OCSP response")
        if must_staple:
            _LOGGER.debug("Must-staple cert with no stapled response, hard fail.")
            return 0
        # https://tools.ietf.org/html/rfc6960#section-3.1
        ext = _get_extension(cert, _AuthorityInformationAccess)
        if ext is None:
            _LOGGER.debug("No authority access information, soft fail")
            # No stapled OCSP response, no responder URI, soft fail.
            return 1
        uris = [desc.access_location.value
                for desc in ext.value
                if desc.access_method == _AuthorityInformationAccessOID.OCSP]
        if not uris:
            _LOGGER.debug("No OCSP URI, soft fail")
            # No responder URI, soft fail.
            return 1
        _LOGGER.debug("Requesting OCSP data")
        # When requesting data from an OCSP endpoint we only fail on
        # successful, valid responses with a certificate status of REVOKED.
        for uri in uris:
            _LOGGER.debug("Trying %s", uri)
            response = _request_ocsp(cert, issuer, uri)
            if response is None:
                # The endpoint didn't respond in time, or the response was
                # unsuccessful or didn't match the request.
                continue
            if not _verify_response(issuer, response):
                # The response failed verification.
                continue
            _LOGGER.debug("OCSP cert status: %r", response.certificate_status)
            if response.certificate_status == _OCSPCertStatus.GOOD:
                return 1
            if response.certificate_status == _OCSPCertStatus.REVOKED:
                return 0
        # Soft fail if we couldn't get a definitive status.
        _LOGGER.debug("No definitive OCSP cert status, soft fail")
        return 1

    _LOGGER.debug("Peer stapled an OCSP response")
    response = _load_der_ocsp_response(ocsp_bytes)
    _LOGGER.debug(
        "OCSP response status: %r", response.response_status)
    # This happens in _request_ocsp when there is no stapled response so
    # we know if we can compare serial numbers for the request and response.
    if response.response_status != _OCSPResponseStatus.SUCCESSFUL:
        return 0
    if not _verify_response(issuer, response):
        return 0
    _LOGGER.debug("OCSP cert status: %r", response.certificate_status)
    if response.certificate_status == _OCSPCertStatus.REVOKED:
        return 0
    return 1
