# Copyright 2013 10gen, Inc.
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

try:
    import hashlib
    _MD5 = hashlib.md5
except ImportError:  # for Python < 2.5
    import md5
    _MD5 = md5.new

HAVE_KERBEROS = True
try:
    import kerberos
except ImportError:
    HAVE_KERBEROS = False

from bson.son import SON
from pymongo.errors import ConfigurationError, OperationFailure


MECHANISMS = ('MONGODB-CR', 'GSSAPI')
"""The authentication mechanisms supported by PyMongo."""


def _password_digest(username, password):
    """Get a password digest to use for authentication.
    """
    if not isinstance(password, basestring):
        raise TypeError("password must be an instance "
                        "of %s" % (basestring.__name__,))
    if len(password) == 0:
        raise TypeError("password can't be empty")
    if not isinstance(username, basestring):
        raise TypeError("username must be an instance "
                        "of %s" % (basestring.__name__,))

    md5hash = _MD5()
    data = "%s:mongo:%s" % (username, password)
    md5hash.update(data.encode('utf-8'))
    return unicode(md5hash.hexdigest())


def _auth_key(nonce, username, password):
    """Get an auth key to use for authentication.
    """
    digest = _password_digest(username, password)
    md5hash = _MD5()
    data = "%s%s%s" % (nonce, unicode(username), digest)
    md5hash.update(data.encode('utf-8'))
    return unicode(md5hash.hexdigest())


def _authenticate_gssapi(username, sock_info, cmd_func):
    """Authenticate using GSSAPI.
    """
    try:
        # Starting here and continuing through the while loop below - establish
        # the security context. See RFC 4752, Section 3.1, first paragraph.
        result, ctx = kerberos.authGSSClientInit('mongodb@' + sock_info.host,
                                                 kerberos.GSS_C_MUTUAL_FLAG)
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
            response, _ = cmd_func(sock_info, '$external', cmd)

            # Limit how many times we loop to catch protocol / library issues
            for _ in xrange(10):
                result = kerberos.authGSSClientStep(ctx,
                                                    str(response['payload']))
                if result == -1:
                    raise OperationFailure('Unknown kerberos '
                                           'failure in step function.')

                payload = kerberos.authGSSClientResponse(ctx) or ''

                cmd = SON([('saslContinue', 1),
                           ('conversationId', response['conversationId']),
                           ('payload', payload)])
                response, _ = cmd_func(sock_info, '$external', cmd)

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
            response, _ = cmd_func(sock_info, '$external', cmd)

        finally:
            kerberos.authGSSClientClean(ctx)

    except kerberos.KrbError, exc:
        raise OperationFailure(str(exc))


def _authenticate_mongo_cr(username, password, source, sock_info, cmd_func):
    """Authenticate using MONGODB-CR.
    """
    # Get a nonce
    response, _ = cmd_func(sock_info, source, {'getnonce': 1})
    nonce = response['nonce']
    key = _auth_key(nonce, username, password)

    # Actually authenticate
    query = SON([('authenticate', 1),
                 ('user', username),
                 ('nonce', nonce),
                 ('key', key)])
    cmd_func(sock_info, source, query)


def authenticate(credentials, sock_info, cmd_func):
    """Authenticate sock_info.
    """
    source, username, password, mechanism = credentials
    # Use a dict for this when we support more mechanisms.
    if mechanism == 'GSSAPI':
        if not HAVE_KERBEROS:
            raise ConfigurationError('The "kerberos" module must be '
                                     'installed to use GSSAPI authentication.')
        _authenticate_gssapi(username, sock_info, cmd_func)
    else:
        _authenticate_mongo_cr(username, password, source, sock_info, cmd_func)

