# Copyright 2014-present MongoDB, Inc.
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

"""Support for SSL in PyMongo."""

import sys

from pymongo.errors import ConfigurationError

HAVE_SSL = True

try:
    import pymongo.pyopenssl_context as _ssl
except ImportError:
    try:
        import pymongo.ssl_context as _ssl
    except ImportError:
        HAVE_SSL = False


if HAVE_SSL:
    # Note: The validate* functions below deal with users passing
    # CPython ssl module constants to configure certificate verification
    # at a high level. This is legacy behavior, but requires us to
    # import the ssl module even if we're only using it for this purpose.
    import ssl as _stdlibssl
    from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED
    HAS_SNI = _ssl.HAS_SNI
    IPADDR_SAFE = _ssl.IS_PYOPENSSL or sys.version_info[:2] >= (3, 7)
    SSLError = _ssl.SSLError
    def validate_allow_invalid_certs(option, value):
        """Validate the option to allow invalid certificates is valid."""
        # Avoid circular import.
        from pymongo.common import validate_boolean_or_string
        boolean_cert_reqs = validate_boolean_or_string(option, value)
        if boolean_cert_reqs:
            return CERT_NONE
        return CERT_REQUIRED

    def get_ssl_context(*args):
        """Create and return an SSLContext object."""
        (certfile,
         keyfile,
         passphrase,
         ca_certs,
         cert_reqs,
         crlfile,
         allow_invalid_hostnames,
         check_ocsp_endpoint) = args
        verify_mode = CERT_REQUIRED if cert_reqs is None else cert_reqs
        ctx = _ssl.SSLContext(_ssl.PROTOCOL_SSLv23)
        # SSLContext.check_hostname was added in CPython 3.4.
        if hasattr(ctx, "check_hostname"):
            if _ssl.CHECK_HOSTNAME_SAFE and verify_mode != CERT_NONE:
                ctx.check_hostname = not allow_invalid_hostnames
            else:
                ctx.check_hostname = False
        if hasattr(ctx, "check_ocsp_endpoint"):
            ctx.check_ocsp_endpoint = check_ocsp_endpoint
        if hasattr(ctx, "options"):
            # Explicitly disable SSLv2, SSLv3 and TLS compression. Note that
            # up to date versions of MongoDB 2.4 and above already disable
            # SSLv2 and SSLv3, python disables SSLv2 by default in >= 2.7.7
            # and >= 3.3.4 and SSLv3 in >= 3.4.3.
            ctx.options |= _ssl.OP_NO_SSLv2
            ctx.options |= _ssl.OP_NO_SSLv3
            ctx.options |= _ssl.OP_NO_COMPRESSION
            ctx.options |= _ssl.OP_NO_RENEGOTIATION
        if certfile is not None:
            try:
                ctx.load_cert_chain(certfile, keyfile, passphrase)
            except _ssl.SSLError as exc:
                raise ConfigurationError(
                    "Private key doesn't match certificate: %s" % (exc,))
        if crlfile is not None:
            if _ssl.IS_PYOPENSSL:
                raise ConfigurationError(
                    "tlsCRLFile cannot be used with PyOpenSSL")
            # Match the server's behavior.
            ctx.verify_flags = getattr(_ssl, "VERIFY_CRL_CHECK_LEAF", 0)
            ctx.load_verify_locations(crlfile)
        if ca_certs is not None:
            ctx.load_verify_locations(ca_certs)
        elif cert_reqs != CERT_NONE:
            ctx.load_default_certs()
        ctx.verify_mode = verify_mode
        return ctx
else:
    class SSLError(Exception):
        pass
    HAS_SNI = False
    IPADDR_SAFE = False
    def validate_allow_invalid_certs(option, dummy):
        """No ssl module, raise ConfigurationError."""
        raise ConfigurationError("The value of %s is set but can't be "
                                 "validated. The ssl module is not available"
                                 % (option,))

    def get_ssl_context(*dummy):
        """No ssl module, raise ConfigurationError."""
        raise ConfigurationError("The ssl module is not available.")
