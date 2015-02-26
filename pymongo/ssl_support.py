# Copyright 2014 MongoDB, Inc.
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

HAVE_SSL = True
try:
    from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED, PROTOCOL_SSLv23
except ImportError:
    HAVE_SSL = False

from pymongo.errors import ConfigurationError

if HAVE_SSL:
    try:
        # Python 3.2 and above.
        from ssl import SSLContext
    except ImportError:
        from pymongo.ssl_context import SSLContext

    def validate_cert_reqs(option, value):
        """Validate the cert reqs are valid. It must be None or one of the
        three values ``ssl.CERT_NONE``, ``ssl.CERT_OPTIONAL`` or
        ``ssl.CERT_REQUIRED``.
        """
        if value is None:
            return value
        elif value in (CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED):
            return value
        raise ValueError("The value of %s must be one of: "
                         "`ssl.CERT_NONE`, `ssl.CERT_OPTIONAL` or "
                         "`ssl.CERT_REQUIRED" % (option,))

    def get_ssl_context(*args):
        """Create and return an SSLContext object."""
        certfile, keyfile, ca_certs, cert_reqs = args
        ctx = SSLContext(PROTOCOL_SSLv23)
        if certfile is not None:
            ctx.load_cert_chain(certfile, keyfile)
        if ca_certs is not None:
            ctx.load_verify_locations(ca_certs)
        if cert_reqs is not None:
            ctx.verify_mode = cert_reqs
        return ctx
else:
    def validate_cert_reqs(option, dummy):
        """No ssl module, raise ConfigurationError."""
        raise ConfigurationError("The value of %s is set but can't be "
                                 "validated. The ssl module is not available"
                                 % (option,))

    def get_ssl_context(*dummy):
        """No ssl module, raise ConfigurationError."""
        raise ConfigurationError("The ssl module is not available.")
