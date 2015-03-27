# Copyright 2013-2015 MongoDB, Inc.
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

"""Authentication helpers."""

import hmac

HAVE_KERBEROS = True
try:
    import kerberos
except ImportError:
    HAVE_KERBEROS = False

from base64 import standard_b64decode, standard_b64encode
from collections import namedtuple
from hashlib import md5, sha1
from random import SystemRandom

from bson.binary import Binary
from bson.py3compat import b, string_type, _unicode, PY3
from bson.son import SON
from pymongo.errors import ConfigurationError, OperationFailure


MECHANISMS = frozenset(
    ['GSSAPI', 'MONGODB-CR', 'MONGODB-X509', 'PLAIN', 'SCRAM-SHA-1', 'DEFAULT'])
"""The authentication mechanisms supported by PyMongo."""


MongoCredential = namedtuple(
    'MongoCredential',
    ['mechanism', 'source', 'username', 'password', 'mechanism_properties'])
"""A hashable namedtuple of values used for authentication."""


GSSAPIProperties = namedtuple('GSSAPIProperties', ['service_name'])
"""Mechanism properties for GSSAPI authentication."""


def _build_credentials_tuple(mech, source, user, passwd, extra):
    """Build and return a mechanism specific credentials tuple.
    """
    user = _unicode(user)
    if mech == 'GSSAPI':
        properties = extra.get('authmechanismproperties', {})
        service_name = properties.get('SERVICE_NAME', 'mongodb')
        props = GSSAPIProperties(service_name=service_name)
        # No password, source is always $external.
        return MongoCredential(mech, '$external', user, None, props)
    elif mech == 'MONGODB-X509':
        return MongoCredential(mech, '$external', user, None, None)
    else:
        if passwd is None:
            raise ConfigurationError("A password is required.")
        return MongoCredential(mech, source, user, _unicode(passwd), None)


if PY3:
    def _xor(fir, sec):
        """XOR two byte strings together (python 3.x)."""
        return b"".join([bytes([x ^ y]) for x, y in zip(fir, sec)])


    _from_bytes = int.from_bytes
    _to_bytes = int.to_bytes
else:
    from binascii import (hexlify as _hexlify,
                          unhexlify as _unhexlify)


    def _xor(fir, sec):
        """XOR two byte strings together (python 2.x)."""
        return b"".join([chr(ord(x) ^ ord(y)) for x, y in zip(fir, sec)])


    def _from_bytes(value, dummy, int=int, _hexlify=_hexlify):
        """An implementation of int.from_bytes for python 2.x."""
        return int(_hexlify(value), 16)


    def _to_bytes(value, dummy0, dummy1, _unhexlify=_unhexlify):
        """An implementation of int.to_bytes for python 2.x."""
        return _unhexlify('%040x' % value)


try:
    # The fastest option, if it's been compiled to use OpenSSL's HMAC.
    from backports.pbkdf2 import pbkdf2_hmac

    def _hi(data, salt, iterations):
        return pbkdf2_hmac('sha1', data, salt, iterations)

except ImportError:
    try:
        # Python 2.7.8+, or Python 3.4+.
        from hashlib import pbkdf2_hmac

        def _hi(data, salt, iterations):
            return pbkdf2_hmac('sha1', data, salt, iterations)

    except ImportError:

        def _hi(data, salt, iterations):
            """A simple implementation of PBKDF2."""
            mac = hmac.HMAC(data, None, sha1)

            def _digest(msg, mac=mac):
                """Get a digest for msg."""
                _mac = mac.copy()
                _mac.update(msg)
                return _mac.digest()

            from_bytes = _from_bytes
            to_bytes = _to_bytes

            _u1 = _digest(salt + b'\x00\x00\x00\x01')
            _ui = from_bytes(_u1, 'big')
            for _ in range(iterations - 1):
                _u1 = _digest(_u1)
                _ui ^= from_bytes(_u1, 'big')
            return to_bytes(_ui, 20, 'big')


def _parse_scram_response(response):
    """Split a scram response into key, value pairs."""
    return dict(item.split(b"=", 1) for item in response.split(b","))


def _authenticate_scram_sha1(credentials, sock_info):
    """Authenticate using SCRAM-SHA-1."""
    username = credentials.username
    password = credentials.password
    source = credentials.source

    # Make local
    _hmac = hmac.HMAC
    _sha1 = sha1

    user = username.encode("utf-8").replace(b"=", b"=3D").replace(b",", b"=2C")
    nonce = standard_b64encode(
        (("%s" % (SystemRandom().random(),))[2:]).encode("utf-8"))
    first_bare = b"n=" + user + b",r=" + nonce

    cmd = SON([('saslStart', 1),
               ('mechanism', 'SCRAM-SHA-1'),
               ('payload', Binary(b"n,," + first_bare)),
               ('autoAuthorize', 1)])
    res = sock_info.command(source, cmd)

    server_first = res['payload']
    parsed = _parse_scram_response(server_first)
    iterations = int(parsed[b'i'])
    salt = parsed[b's']
    rnonce = parsed[b'r']
    if not rnonce.startswith(nonce):
        raise OperationFailure("Server returned an invalid nonce.")

    without_proof = b"c=biws,r=" + rnonce
    salted_pass = _hi(_password_digest(username, password).encode("utf-8"),
                      standard_b64decode(salt),
                      iterations)
    client_key = _hmac(salted_pass, b"Client Key", _sha1).digest()
    stored_key = _sha1(client_key).digest()
    auth_msg = b",".join((first_bare, server_first, without_proof))
    client_sig = _hmac(stored_key, auth_msg, _sha1).digest()
    client_proof = b"p=" + standard_b64encode(_xor(client_key, client_sig))
    client_final = b",".join((without_proof, client_proof))

    server_key = _hmac(salted_pass, b"Server Key", _sha1).digest()
    server_sig = standard_b64encode(
        _hmac(server_key, auth_msg, _sha1).digest())

    cmd = SON([('saslContinue', 1),
               ('conversationId', res['conversationId']),
               ('payload', Binary(client_final))])
    res = sock_info.command(source, cmd)

    parsed = _parse_scram_response(res['payload'])
    if parsed[b'v'] != server_sig:
        raise OperationFailure("Server returned an invalid signature.")

    # Depending on how it's configured, Cyrus SASL (which the server uses)
    # requires a third empty challenge.
    if not res['done']:
        cmd = SON([('saslContinue', 1),
                   ('conversationId', res['conversationId']),
                   ('payload', Binary(b''))])
        res = sock_info.command(source, cmd)
        if not res['done']:
            raise OperationFailure('SASL conversation failed to complete.')


def _password_digest(username, password):
    """Get a password digest to use for authentication.
    """
    if not isinstance(password, string_type):
        raise TypeError("password must be an "
                        "instance of %s" % (string_type.__name__,))
    if len(password) == 0:
        raise ValueError("password can't be empty")
    if not isinstance(username, string_type):
        raise TypeError("password must be an "
                        "instance of  %s" % (string_type.__name__,))

    md5hash = md5()
    data = "%s:mongo:%s" % (username, password)
    md5hash.update(data.encode('utf-8'))
    return _unicode(md5hash.hexdigest())


def _auth_key(nonce, username, password):
    """Get an auth key to use for authentication.
    """
    digest = _password_digest(username, password)
    md5hash = md5()
    data = "%s%s%s" % (nonce, username, digest)
    md5hash.update(data.encode('utf-8'))
    return _unicode(md5hash.hexdigest())


def _authenticate_gssapi(credentials, sock_info):
    """Authenticate using GSSAPI.
    """
    if not HAVE_KERBEROS:
        raise ConfigurationError('The "kerberos" module must be '
                                 'installed to use GSSAPI authentication.')

    try:
        username = credentials.username
        gsn = credentials.mechanism_properties.service_name
        # Starting here and continuing through the while loop below - establish
        # the security context. See RFC 4752, Section 3.1, first paragraph.
        result, ctx = kerberos.authGSSClientInit(
            gsn + '@' + sock_info.host, gssflags=kerberos.GSS_C_MUTUAL_FLAG)

        if result != kerberos.AUTH_GSS_COMPLETE:
            raise OperationFailure('Kerberos context failed to initialize.')

        try:
            # pykerberos uses a weird mix of exceptions and return values
            # to indicate errors.
            # 0 == continue, 1 == complete, -1 == error
            # Only authGSSClientStep can return 0.
            if kerberos.authGSSClientStep(ctx, '') != 0:
                raise OperationFailure('Unknown kerberos '
                                       'failure in step function.')

            # Start a SASL conversation with mongod/s
            # Note: pykerberos deals with base64 encoded byte strings.
            # Since mongo accepts base64 strings as the payload we don't
            # have to use bson.binary.Binary.
            payload = kerberos.authGSSClientResponse(ctx)
            cmd = SON([('saslStart', 1),
                       ('mechanism', 'GSSAPI'),
                       ('payload', payload),
                       ('autoAuthorize', 1)])
            response = sock_info.command('$external', cmd)

            # Limit how many times we loop to catch protocol / library issues
            for _ in range(10):
                result = kerberos.authGSSClientStep(ctx,
                                                    str(response['payload']))
                if result == -1:
                    raise OperationFailure('Unknown kerberos '
                                           'failure in step function.')

                payload = kerberos.authGSSClientResponse(ctx) or ''

                cmd = SON([('saslContinue', 1),
                           ('conversationId', response['conversationId']),
                           ('payload', payload)])
                response = sock_info.command('$external', cmd)

                if result == kerberos.AUTH_GSS_COMPLETE:
                    break
            else:
                raise OperationFailure('Kerberos '
                                       'authentication failed to complete.')

            # Once the security context is established actually authenticate.
            # See RFC 4752, Section 3.1, last two paragraphs.
            if kerberos.authGSSClientUnwrap(ctx,
                                            str(response['payload'])) != 1:
                raise OperationFailure('Unknown kerberos '
                                       'failure during GSS_Unwrap step.')

            if kerberos.authGSSClientWrap(ctx,
                                          kerberos.authGSSClientResponse(ctx),
                                          username) != 1:
                raise OperationFailure('Unknown kerberos '
                                       'failure during GSS_Wrap step.')

            payload = kerberos.authGSSClientResponse(ctx)
            cmd = SON([('saslContinue', 1),
                       ('conversationId', response['conversationId']),
                       ('payload', payload)])
            sock_info.command('$external', cmd)

        finally:
            kerberos.authGSSClientClean(ctx)

    except kerberos.KrbError as exc:
        raise OperationFailure(str(exc))


def _authenticate_plain(credentials, sock_info):
    """Authenticate using SASL PLAIN (RFC 4616)
    """
    source = credentials.source
    username = credentials.username
    password = credentials.password
    payload = ('\x00%s\x00%s' % (username, password)).encode('utf-8')
    cmd = SON([('saslStart', 1),
               ('mechanism', 'PLAIN'),
               ('payload', Binary(payload)),
               ('autoAuthorize', 1)])
    sock_info.command(source, cmd)


def _authenticate_cram_md5(credentials, sock_info):
    """Authenticate using CRAM-MD5 (RFC 2195)
    """
    source = credentials.source
    username = credentials.username
    password = credentials.password
    # The password used as the mac key is the
    # same as what we use for MONGODB-CR
    passwd = _password_digest(username, password)
    cmd = SON([('saslStart', 1),
               ('mechanism', 'CRAM-MD5'),
               ('payload', Binary(b'')),
               ('autoAuthorize', 1)])
    response = sock_info.command(source, cmd)
    # MD5 as implicit default digest for digestmod is deprecated
    # in python 3.4
    mac = hmac.HMAC(key=passwd.encode('utf-8'), digestmod=md5)
    mac.update(response['payload'])
    challenge = username.encode('utf-8') + b' ' + b(mac.hexdigest())
    cmd = SON([('saslContinue', 1),
               ('conversationId', response['conversationId']),
               ('payload', Binary(challenge))])
    sock_info.command(source, cmd)


def _authenticate_x509(credentials, sock_info):
    """Authenticate using MONGODB-X509.
    """
    query = SON([('authenticate', 1),
                 ('mechanism', 'MONGODB-X509'),
                 ('user', credentials.username)])
    sock_info.command('$external', query)


def _authenticate_mongo_cr(credentials, sock_info):
    """Authenticate using MONGODB-CR.
    """
    source = credentials.source
    username = credentials.username
    password = credentials.password
    # Get a nonce
    response = sock_info.command(source, {'getnonce': 1})
    nonce = response['nonce']
    key = _auth_key(nonce, username, password)

    # Actually authenticate
    query = SON([('authenticate', 1),
                 ('user', username),
                 ('nonce', nonce),
                 ('key', key)])
    sock_info.command(source, query)


def _authenticate_default(credentials, sock_info):
    if sock_info.max_wire_version >= 3:
        return _authenticate_scram_sha1(credentials, sock_info)
    else:
        return _authenticate_mongo_cr(credentials, sock_info)


_AUTH_MAP = {
    'CRAM-MD5': _authenticate_cram_md5,
    'GSSAPI': _authenticate_gssapi,
    'MONGODB-CR': _authenticate_mongo_cr,
    'MONGODB-X509': _authenticate_x509,
    'PLAIN': _authenticate_plain,
    'SCRAM-SHA-1': _authenticate_scram_sha1,
    'DEFAULT': _authenticate_default,
}


def authenticate(credentials, sock_info):
    """Authenticate sock_info."""
    mechanism = credentials.mechanism
    auth_func = _AUTH_MAP.get(mechanism)
    auth_func(credentials, sock_info)


def logout(source, sock_info):
    """Log out from a database."""
    sock_info.command(source, {'logout': 1})
