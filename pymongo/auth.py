# Copyright 2013-present MongoDB, Inc.
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

import functools
import hashlib
import hmac
import os
import socket

from base64 import standard_b64decode, standard_b64encode
from collections import namedtuple
from urllib.parse import quote

from bson.binary import Binary
from bson.son import SON
from pymongo.auth_aws import _authenticate_aws
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.saslprep import saslprep

HAVE_KERBEROS = True
_USE_PRINCIPAL = False
try:
    import winkerberos as kerberos
    if tuple(map(int, kerberos.__version__.split('.')[:2])) >= (0, 5):
        _USE_PRINCIPAL = True
except ImportError:
    try:
        import kerberos
    except ImportError:
        HAVE_KERBEROS = False


MECHANISMS = frozenset(
    ['GSSAPI',
     'MONGODB-CR',
     'MONGODB-X509',
     'MONGODB-AWS',
     'PLAIN',
     'SCRAM-SHA-1',
     'SCRAM-SHA-256',
     'DEFAULT'])
"""The authentication mechanisms supported by PyMongo."""


class _Cache(object):
    __slots__ = ("data",)

    _hash_val = hash('_Cache')

    def __init__(self):
        self.data = None

    def __eq__(self, other):
        # Two instances must always compare equal.
        if isinstance(other, _Cache):
            return True
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, _Cache):
            return False
        return NotImplemented

    def __hash__(self):
        return self._hash_val



MongoCredential = namedtuple(
    'MongoCredential',
    ['mechanism',
     'source',
     'username',
     'password',
     'mechanism_properties',
     'cache'])
"""A hashable namedtuple of values used for authentication."""


GSSAPIProperties = namedtuple('GSSAPIProperties',
                              ['service_name',
                               'canonicalize_host_name',
                               'service_realm'])
"""Mechanism properties for GSSAPI authentication."""


_AWSProperties = namedtuple('AWSProperties', ['aws_session_token'])
"""Mechanism properties for MONGODB-AWS authentication."""


def _build_credentials_tuple(mech, source, user, passwd, extra, database):
    """Build and return a mechanism specific credentials tuple.
    """
    if mech not in ('MONGODB-X509', 'MONGODB-AWS') and user is None:
        raise ConfigurationError("%s requires a username." % (mech,))
    if mech == 'GSSAPI':
        if source is not None and source != '$external':
            raise ValueError(
                "authentication source must be $external or None for GSSAPI")
        properties = extra.get('authmechanismproperties', {})
        service_name = properties.get('SERVICE_NAME', 'mongodb')
        canonicalize = properties.get('CANONICALIZE_HOST_NAME', False)
        service_realm = properties.get('SERVICE_REALM')
        props = GSSAPIProperties(service_name=service_name,
                                 canonicalize_host_name=canonicalize,
                                 service_realm=service_realm)
        # Source is always $external.
        return MongoCredential(mech, '$external', user, passwd, props, None)
    elif mech == 'MONGODB-X509':
        if passwd is not None:
            raise ConfigurationError(
                "Passwords are not supported by MONGODB-X509")
        if source is not None and source != '$external':
            raise ValueError(
                "authentication source must be "
                "$external or None for MONGODB-X509")
        # Source is always $external, user can be None.
        return MongoCredential(mech, '$external', user, None, None, None)
    elif mech == 'MONGODB-AWS':
        if user is not None and passwd is None:
            raise ConfigurationError(
                "username without a password is not supported by MONGODB-AWS")
        if source is not None and source != '$external':
            raise ConfigurationError(
                "authentication source must be "
                "$external or None for MONGODB-AWS")

        properties = extra.get('authmechanismproperties', {})
        aws_session_token = properties.get('AWS_SESSION_TOKEN')
        props = _AWSProperties(aws_session_token=aws_session_token)
        # user can be None for temporary link-local EC2 credentials.
        return MongoCredential(mech, '$external', user, passwd, props, None)
    elif mech == 'PLAIN':
        source_database = source or database or '$external'
        return MongoCredential(mech, source_database, user, passwd, None, None)
    else:
        source_database = source or database or 'admin'
        if passwd is None:
            raise ConfigurationError("A password is required.")
        return MongoCredential(
            mech, source_database, user, passwd, None, _Cache())


def _xor(fir, sec):
    """XOR two byte strings together (python 3.x)."""
    return b"".join([bytes([x ^ y]) for x, y in zip(fir, sec)])


def _parse_scram_response(response):
    """Split a scram response into key, value pairs."""
    return dict(item.split(b"=", 1) for item in response.split(b","))


def _authenticate_scram_start(credentials, mechanism):
    username = credentials.username
    user = username.encode("utf-8").replace(b"=", b"=3D").replace(b",", b"=2C")
    nonce = standard_b64encode(os.urandom(32))
    first_bare = b"n=" + user + b",r=" + nonce

    cmd = SON([('saslStart', 1),
               ('mechanism', mechanism),
               ('payload', Binary(b"n,," + first_bare)),
               ('autoAuthorize', 1),
               ('options', {'skipEmptyExchange': True})])
    return nonce, first_bare, cmd


def _authenticate_scram(credentials, sock_info, mechanism):
    """Authenticate using SCRAM."""
    username = credentials.username
    if mechanism == 'SCRAM-SHA-256':
        digest = "sha256"
        digestmod = hashlib.sha256
        data = saslprep(credentials.password).encode("utf-8")
    else:
        digest = "sha1"
        digestmod = hashlib.sha1
        data = _password_digest(username, credentials.password).encode("utf-8")
    source = credentials.source
    cache = credentials.cache

    # Make local
    _hmac = hmac.HMAC

    ctx = sock_info.auth_ctx.get(credentials)
    if ctx and ctx.speculate_succeeded():
        nonce, first_bare = ctx.scram_data
        res = ctx.speculative_authenticate
    else:
        nonce, first_bare, cmd = _authenticate_scram_start(
            credentials, mechanism)
        res = sock_info.command(source, cmd)

    server_first = res['payload']
    parsed = _parse_scram_response(server_first)
    iterations = int(parsed[b'i'])
    if iterations < 4096:
        raise OperationFailure("Server returned an invalid iteration count.")
    salt = parsed[b's']
    rnonce = parsed[b'r']
    if not rnonce.startswith(nonce):
        raise OperationFailure("Server returned an invalid nonce.")

    without_proof = b"c=biws,r=" + rnonce
    if cache.data:
        client_key, server_key, csalt, citerations = cache.data
    else:
        client_key, server_key, csalt, citerations = None, None, None, None

    # Salt and / or iterations could change for a number of different
    # reasons. Either changing invalidates the cache.
    if not client_key or salt != csalt or iterations != citerations:
        salted_pass = hashlib.pbkdf2_hmac(
            digest, data, standard_b64decode(salt), iterations)
        client_key = _hmac(salted_pass, b"Client Key", digestmod).digest()
        server_key = _hmac(salted_pass, b"Server Key", digestmod).digest()
        cache.data = (client_key, server_key, salt, iterations)
    stored_key = digestmod(client_key).digest()
    auth_msg = b",".join((first_bare, server_first, without_proof))
    client_sig = _hmac(stored_key, auth_msg, digestmod).digest()
    client_proof = b"p=" + standard_b64encode(_xor(client_key, client_sig))
    client_final = b",".join((without_proof, client_proof))

    server_sig = standard_b64encode(
        _hmac(server_key, auth_msg, digestmod).digest())

    cmd = SON([('saslContinue', 1),
               ('conversationId', res['conversationId']),
               ('payload', Binary(client_final))])
    res = sock_info.command(source, cmd)

    parsed = _parse_scram_response(res['payload'])
    if not hmac.compare_digest(parsed[b'v'], server_sig):
        raise OperationFailure("Server returned an invalid signature.")

    # A third empty challenge may be required if the server does not support
    # skipEmptyExchange: SERVER-44857.
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
    if not isinstance(password, str):
        raise TypeError("password must be an instance of str")
    if len(password) == 0:
        raise ValueError("password can't be empty")
    if not isinstance(username, str):
        raise TypeError("username must be an instance of str")

    md5hash = hashlib.md5()
    data = "%s:mongo:%s" % (username, password)
    md5hash.update(data.encode('utf-8'))
    return md5hash.hexdigest()


def _auth_key(nonce, username, password):
    """Get an auth key to use for authentication.
    """
    digest = _password_digest(username, password)
    md5hash = hashlib.md5()
    data = "%s%s%s" % (nonce, username, digest)
    md5hash.update(data.encode('utf-8'))
    return md5hash.hexdigest()


def _canonicalize_hostname(hostname):
    """Canonicalize hostname following MIT-krb5 behavior."""
    # https://github.com/krb5/krb5/blob/d406afa363554097ac48646a29249c04f498c88e/src/util/k5test.py#L505-L520
    af, socktype, proto, canonname, sockaddr = socket.getaddrinfo(
        hostname, None, 0, 0, socket.IPPROTO_TCP, socket.AI_CANONNAME)[0]

    try:
        name = socket.getnameinfo(sockaddr, socket.NI_NAMEREQD)
    except socket.gaierror:
        return canonname.lower()

    return name[0].lower()


def _authenticate_gssapi(credentials, sock_info):
    """Authenticate using GSSAPI.
    """
    if not HAVE_KERBEROS:
        raise ConfigurationError('The "kerberos" module must be '
                                 'installed to use GSSAPI authentication.')

    try:
        username = credentials.username
        password = credentials.password
        props = credentials.mechanism_properties
        # Starting here and continuing through the while loop below - establish
        # the security context. See RFC 4752, Section 3.1, first paragraph.
        host = sock_info.address[0]
        if props.canonicalize_host_name:
            host = _canonicalize_hostname(host)
        service = props.service_name + '@' + host
        if props.service_realm is not None:
            service = service + '@' + props.service_realm

        if password is not None:
            if _USE_PRINCIPAL:
                # Note that, though we use unquote_plus for unquoting URI
                # options, we use quote here. Microsoft's UrlUnescape (used
                # by WinKerberos) doesn't support +.
                principal = ":".join((quote(username), quote(password)))
                result, ctx = kerberos.authGSSClientInit(
                    service, principal, gssflags=kerberos.GSS_C_MUTUAL_FLAG)
            else:
                if '@' in username:
                    user, domain = username.split('@', 1)
                else:
                    user, domain = username, None
                result, ctx = kerberos.authGSSClientInit(
                    service, gssflags=kerberos.GSS_C_MUTUAL_FLAG,
                    user=user, domain=domain, password=password)
        else:
            result, ctx = kerberos.authGSSClientInit(
                service, gssflags=kerberos.GSS_C_MUTUAL_FLAG)

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
    mac = hmac.HMAC(key=passwd.encode('utf-8'), digestmod=hashlib.md5)
    mac.update(response['payload'])
    challenge = username.encode('utf-8') + b' ' + mac.hexdigest().encode('utf-8')
    cmd = SON([('saslContinue', 1),
               ('conversationId', response['conversationId']),
               ('payload', Binary(challenge))])
    sock_info.command(source, cmd)


def _authenticate_x509(credentials, sock_info):
    """Authenticate using MONGODB-X509.
    """
    ctx = sock_info.auth_ctx.get(credentials)
    if ctx and ctx.speculate_succeeded():
        # MONGODB-X509 is done after the speculative auth step.
        return

    cmd = _X509Context(credentials).speculate_command()
    sock_info.command('$external', cmd)


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
    if sock_info.max_wire_version >= 7:
        if credentials in sock_info.negotiated_mechanisms:
            mechs = sock_info.negotiated_mechanisms[credentials]
        else:
            source = credentials.source
            cmd = sock_info.hello_cmd()
            cmd['saslSupportedMechs'] = source + '.' + credentials.username
            mechs = sock_info.command(
                source, cmd, publish_events=False).get(
                'saslSupportedMechs', [])
        if 'SCRAM-SHA-256' in mechs:
            return _authenticate_scram(credentials, sock_info, 'SCRAM-SHA-256')
        else:
            return _authenticate_scram(credentials, sock_info, 'SCRAM-SHA-1')
    else:
        return _authenticate_scram(credentials, sock_info, 'SCRAM-SHA-1')


_AUTH_MAP = {
    'CRAM-MD5': _authenticate_cram_md5,
    'GSSAPI': _authenticate_gssapi,
    'MONGODB-CR': _authenticate_mongo_cr,
    'MONGODB-X509': _authenticate_x509,
    'MONGODB-AWS': _authenticate_aws,
    'PLAIN': _authenticate_plain,
    'SCRAM-SHA-1': functools.partial(
        _authenticate_scram, mechanism='SCRAM-SHA-1'),
    'SCRAM-SHA-256': functools.partial(
        _authenticate_scram, mechanism='SCRAM-SHA-256'),
    'DEFAULT': _authenticate_default,
}


class _AuthContext(object):
    def __init__(self, credentials):
        self.credentials = credentials
        self.speculative_authenticate = None

    @staticmethod
    def from_credentials(creds):
        spec_cls = _SPECULATIVE_AUTH_MAP.get(creds.mechanism)
        if spec_cls:
            return spec_cls(creds)
        return None

    def speculate_command(self):
        raise NotImplementedError

    def parse_response(self, hello):
        self.speculative_authenticate = hello.speculative_authenticate

    def speculate_succeeded(self):
        return bool(self.speculative_authenticate)


class _ScramContext(_AuthContext):
    def __init__(self, credentials, mechanism):
        super(_ScramContext, self).__init__(credentials)
        self.scram_data = None
        self.mechanism = mechanism

    def speculate_command(self):
        nonce, first_bare, cmd = _authenticate_scram_start(
            self.credentials, self.mechanism)
        # The 'db' field is included only on the speculative command.
        cmd['db'] = self.credentials.source
        # Save for later use.
        self.scram_data = (nonce, first_bare)
        return cmd


class _X509Context(_AuthContext):
    def speculate_command(self):
        cmd = SON([('authenticate', 1),
                   ('mechanism', 'MONGODB-X509')])
        if self.credentials.username is not None:
            cmd['user'] = self.credentials.username
        return cmd


_SPECULATIVE_AUTH_MAP = {
    'MONGODB-X509': _X509Context,
    'SCRAM-SHA-1': functools.partial(_ScramContext, mechanism='SCRAM-SHA-1'),
    'SCRAM-SHA-256': functools.partial(_ScramContext,
                                       mechanism='SCRAM-SHA-256'),
    'DEFAULT': functools.partial(_ScramContext, mechanism='SCRAM-SHA-256'),
}


def authenticate(credentials, sock_info):
    """Authenticate sock_info."""
    mechanism = credentials.mechanism
    auth_func = _AUTH_MAP.get(mechanism)
    auth_func(credentials, sock_info)

