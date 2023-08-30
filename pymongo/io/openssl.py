# try:
#     from greenletio import async_, await_
#     from service_identity import CertificateError as _SICertificateError
#     from service_identity import VerificationError as _SIVerificationError
#     from service_identity.pyopenssl import verify_hostname as _verify_hostname
#     from service_identity.pyopenssl import verify_ip_address as _verify_ip_address
#
#     from pymongo.pyopenssl_context import _sslConn, _is_ip_address, _CallbackData, SSLContext, HAS_SNI, SSLError, BLOCKING_IO_ERRORS, PROTOCOL_SSLv23, OP_NO_SSLv2, OP_NO_SSLv3, OP_NO_COMPRESSION, OP_NO_RENEGOTIATION, IS_PYOPENSSL
#     from pymongo.errors import _CertificateError
#     from pymongo.io import ssl as _stdlibssl
#
#
#     async def _wrap_socket_async(
#             self,
#             sock,
#             server_side=False,
#             do_handshake_on_connect=True,
#             suppress_ragged_eofs=True,
#             server_hostname=None,
#             session=None,
#     ):
#         """Wrap an existing Python socket sock and return a TLS socket
#         object.
#         """
#         ssl_conn = _sslConn(self._ctx, sock, suppress_ragged_eofs)
#         if session:
#             ssl_conn.set_session(session)
#         if server_side is True:
#             ssl_conn.set_accept_state()
#         else:
#             # SNI
#             if server_hostname and not _is_ip_address(server_hostname):
#                 # XXX: Do this in a callback registered with
#                 # SSLContext.set_info_callback? See Twisted for an example.
#                 ssl_conn.set_tlsext_host_name(server_hostname.encode("idna"))
#             if self.verify_mode != _stdlibssl.CERT_NONE:
#                 # Request a stapled OCSP response.
#                 await async_(ssl_conn.request_ocsp)
#
#             ssl_conn.set_connect_state()
#         # If this wasn't true the caller of wrap_socket would call
#         # do_handshake()
#         if do_handshake_on_connect:
#             # XXX: If we do hostname checking in a callback we can get rid
#             # of this call to do_handshake() since the handshake
#             # will happen automatically later.
#             await async_(ssl_conn.do_handshake)
#             # XXX: Do this in a callback registered with
#             # SSLContext.set_info_callback? See Twisted for an example.
#             if self.check_hostname and server_hostname is not None:
#                 try:
#                     if _is_ip_address(server_hostname):
#                         _verify_ip_address(ssl_conn, server_hostname)
#                     else:
#                         _verify_hostname(ssl_conn, server_hostname)
#                 except (_SICertificateError, _SIVerificationError) as exc:
#                     raise _CertificateError(str(exc))
#         return ssl_conn
#
#
#     def wrap_socket(
#             sock,
#             server_side=False,
#             do_handshake_on_connect=True,
#             suppress_ragged_eofs=True,
#             server_hostname=None,
#             session=None,
#     ):
#         await_(
#             _wrap_socket_async(sock, server_side, do_handshake_on_connect, suppress_ragged_eofs, server_hostname, session))
#
#
#     SSLContext.wrap_socket = wrap_socket
# except ImportError:
#     import pymongo.pyopenssl_context
