import sys as _sys
from errno import EINTR as _EINTR
from ipaddress import ip_address as _ip_address

from cryptography.x509 import load_der_x509_certificate as _load_der_x509_certificate
from OpenSSL import SSL as _SSL
from OpenSSL import crypto as _crypto
from service_identity import CertificateError as _SICertificateError
from service_identity import VerificationError as _SIVerificationError
from service_identity.pyopenssl import verify_hostname as _verify_hostname
from service_identity.pyopenssl import verify_ip_address as _verify_ip_address

import pymongo.pyopenssl_context
from pymongo.errors import ConfigurationError as _ConfigurationError
from pymongo.errors import _CertificateError
from pymongo.io import socket as _socket
from pymongo.io import ssl as _stdlibssl
from pymongo.io import time as _time
from pymongo.ocsp_cache import _OCSPCache
from pymongo.ocsp_support import _load_trusted_ca_certs, _ocsp_callback
from pymongo.socket_checker import SocketChecker as _SocketChecker
from pymongo.socket_checker import _errno_from_exception
from pymongo.write_concern import validate_boolean


async def wrap_socket_async(
    self,
    sock,
    server_side=False,
    do_handshake_on_connect=True,
    suppress_ragged_eofs=True,
    server_hostname=None,
    session=None,
):
    """Wrap an existing Python socket sock and return a TLS socket
    object.
    """
    ssl_conn = pymongo._sslConn(self._ctx, sock, suppress_ragged_eofs)
    loop = _asyncio.get_running_loop()
    if session:
        ssl_conn.set_session(session)
    if server_side is True:
        ssl_conn.set_accept_state()
    else:
        # SNI
        if server_hostname and not pymongo._is_ip_address(server_hostname):
            # XXX: Do this in a callback registered with
            # SSLContext.set_info_callback? See Twisted for an example.
            ssl_conn.set_tlsext_host_name(server_hostname.encode("idna"))
        if self.verify_mode != _stdlibssl.CERT_NONE:
            # Request a stapled OCSP response.
            await loop.run_in_executor(None, ssl_conn.request_ocsp)

        ssl_conn.set_connect_state()
    # If this wasn't true the caller of wrap_socket would call
    # do_handshake()
    if do_handshake_on_connect:
        # XXX: If we do hostname checking in a callback we can get rid
        # of this call to do_handshake() since the handshake
        # will happen automatically later.
        await loop.run_in_executor(None, ssl_conn.do_handshake)
        # XXX: Do this in a callback registered with
        # SSLContext.set_info_callback? See Twisted for an example.
        if self.check_hostname and server_hostname is not None:
            try:
                if pymongo._is_ip_address(server_hostname):
                    _verify_ip_address(ssl_conn, server_hostname)
                else:
                    _verify_hostname(ssl_conn, server_hostname)
            except (_SICertificateError, _SIVerificationError) as exc:
                raise _CertificateError(str(exc))
    return ssl_conn
