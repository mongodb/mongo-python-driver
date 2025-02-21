# Copyright 2025-present MongoDB, Inc.
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

"""Utility and helper methods for creating connections."""
from __future__ import annotations

import asyncio
import functools
import socket
import ssl
from typing import (
    TYPE_CHECKING,
    Union,
)

from pymongo import _csot
from pymongo.asynchronous.helpers import _getaddrinfo
from pymongo.errors import (  # type:ignore[attr-defined]
    ConnectionFailure,
    _CertificateError,
)
from pymongo.network_layer import AsyncNetworkingInterface, NetworkingInterface, PyMongoProtocol
from pymongo.pool_options import PoolOptions
from pymongo.pool_shared import (
    _get_timeout_details,
    _raise_connection_failure,
    _set_keepalive_times,
    _set_non_inheritable_non_atomic,
)
from pymongo.ssl_support import HAS_SNI, SSLError

if TYPE_CHECKING:
    from pymongo.pyopenssl_context import _sslConn
    from pymongo.typings import _Address

_IS_SYNC = False


async def _async_create_connection(address: _Address, options: PoolOptions) -> socket.socket:
    """Given (host, port) and PoolOptions, connect and return a raw socket object.

    Can raise socket.error.

    This is a modified version of create_connection from CPython >= 2.7.
    """
    host, port = address

    # Check if dealing with a unix domain socket
    if host.endswith(".sock"):
        if not hasattr(socket, "AF_UNIX"):
            raise ConnectionFailure("UNIX-sockets are not supported on this system")
        sock = socket.socket(socket.AF_UNIX)
        # SOCK_CLOEXEC not supported for Unix sockets.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.connect(host)
            return sock
        except OSError:
            sock.close()
            raise

    # Don't try IPv6 if we don't support it. Also skip it if host
    # is 'localhost' (::1 is fine). Avoids slow connect issues
    # like PYTHON-356.
    family = socket.AF_INET
    if socket.has_ipv6 and host != "localhost":
        family = socket.AF_UNSPEC

    err = None
    for res in await _getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM):  # type: ignore[attr-defined]
        af, socktype, proto, dummy, sa = res
        # SOCK_CLOEXEC was new in CPython 3.2, and only available on a limited
        # number of platforms (newer Linux and *BSD). Starting with CPython 3.4
        # all file descriptors are created non-inheritable. See PEP 446.
        try:
            sock = socket.socket(af, socktype | getattr(socket, "SOCK_CLOEXEC", 0), proto)
        except OSError:
            # Can SOCK_CLOEXEC be defined even if the kernel doesn't support
            # it?
            sock = socket.socket(af, socktype, proto)
        # Fallback when SOCK_CLOEXEC isn't available.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # CSOT: apply timeout to socket connect.
            timeout = _csot.remaining()
            if timeout is None:
                timeout = options.connect_timeout
            elif timeout <= 0:
                raise socket.timeout("timed out")
            sock.settimeout(timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            _set_keepalive_times(sock)
            sock.connect(sa)
            return sock
        except OSError as e:
            err = e
            sock.close()

    if err is not None:
        raise err
    else:
        # This likely means we tried to connect to an IPv6 only
        # host with an OS/kernel or Python interpreter that doesn't
        # support IPv6. The test case is Jython2.5.1 which doesn't
        # support IPv6 at all.
        raise OSError("getaddrinfo failed")


async def _async_configured_socket(
    address: _Address, options: PoolOptions
) -> Union[socket.socket, _sslConn]:
    """Given (host, port) and PoolOptions, return a raw configured socket.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets socket's SSL and timeout options.
    """
    sock = await _async_create_connection(address, options)
    ssl_context = options._ssl_context

    if ssl_context is None:
        sock.settimeout(options.socket_timeout)
        return sock

    host = address[0]
    try:
        # We have to pass hostname / ip address to wrap_socket
        # to use SSLContext.check_hostname.
        if HAS_SNI:
            if hasattr(ssl_context, "a_wrap_socket"):
                ssl_sock = await ssl_context.a_wrap_socket(sock, server_hostname=host)  # type: ignore[assignment, misc]
            else:
                loop = asyncio.get_running_loop()
                ssl_sock = await loop.run_in_executor(
                    None,
                    functools.partial(ssl_context.wrap_socket, sock, server_hostname=host),  # type: ignore[assignment, misc]
                )
        else:
            if hasattr(ssl_context, "a_wrap_socket"):
                ssl_sock = await ssl_context.a_wrap_socket(sock)  # type: ignore[assignment, misc]
            else:
                loop = asyncio.get_running_loop()
                ssl_sock = await loop.run_in_executor(None, ssl_context.wrap_socket, sock)  # type: ignore[assignment, misc]
    except _CertificateError:
        sock.close()
        # Raise _CertificateError directly like we do after match_hostname
        # below.
        raise
    except (OSError, SSLError) as exc:
        sock.close()
        # We raise AutoReconnect for transient and permanent SSL handshake
        # failures alike. Permanent handshake failures, like protocol
        # mismatch, will be turned into ServerSelectionTimeoutErrors later.
        details = _get_timeout_details(options)
        _raise_connection_failure(address, exc, "SSL handshake failed: ", timeout_details=details)
    if (
        ssl_context.verify_mode
        and not ssl_context.check_hostname
        and not options.tls_allow_invalid_hostnames
    ):
        try:
            ssl.match_hostname(ssl_sock.getpeercert(), hostname=host)  # type:ignore[attr-defined]
        except _CertificateError:
            ssl_sock.close()
            raise

    ssl_sock.settimeout(options.socket_timeout)
    return ssl_sock


async def _configured_protocol_interface(
    address: _Address, options: PoolOptions
) -> AsyncNetworkingInterface:
    """Given (host, port) and PoolOptions, return a configured AsyncNetworkingInterface.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets protocol's SSL and timeout options.
    """
    sock = await _async_create_connection(address, options)
    ssl_context = options._ssl_context
    timeout = options.socket_timeout

    if ssl_context is None:
        return AsyncNetworkingInterface(
            await asyncio.get_running_loop().create_connection(
                lambda: PyMongoProtocol(timeout=timeout, buffer_size=2**16), sock=sock
            )
        )

    host = address[0]
    try:
        # We have to pass hostname / ip address to wrap_socket
        # to use SSLContext.check_hostname.
        transport, protocol = await asyncio.get_running_loop().create_connection(  # type: ignore[call-overload]
            lambda: PyMongoProtocol(timeout=timeout, buffer_size=2**14),
            sock=sock,
            server_hostname=host,
            ssl=ssl_context,
        )
    except _CertificateError:
        transport.abort()
        # Raise _CertificateError directly like we do after match_hostname
        # below.
        raise
    except (OSError, SSLError) as exc:
        transport.abort()
        # We raise AutoReconnect for transient and permanent SSL handshake
        # failures alike. Permanent handshake failures, like protocol
        # mismatch, will be turned into ServerSelectionTimeoutErrors later.
        details = _get_timeout_details(options)
        _raise_connection_failure(address, exc, "SSL handshake failed: ", timeout_details=details)
    if (
        ssl_context.verify_mode
        and not ssl_context.check_hostname
        and not options.tls_allow_invalid_hostnames
    ):
        try:
            ssl.match_hostname(transport.get_extra_info("peercert"), hostname=host)  # type:ignore[attr-defined,unused-ignore]
        except _CertificateError:
            transport.abort()
            raise

    return AsyncNetworkingInterface((transport, protocol))


if _IS_SYNC:
    from pymongo.synchronous.connection_helpers import _create_connection

    def _configured_socket_interface(
        address: _Address, options: PoolOptions
    ) -> NetworkingInterface:
        """Given (host, port) and PoolOptions, return a NetworkingInterface wrapping a configured socket.

        Can raise socket.error, ConnectionFailure, or _CertificateError.

        Sets socket's SSL and timeout options.
        """
        sock = _create_connection(address, options)
        ssl_context = options._ssl_context

        if ssl_context is None:
            sock.settimeout(options.socket_timeout)
            return NetworkingInterface(sock)

        host = address[0]
        try:
            # We have to pass hostname / ip address to wrap_socket
            # to use SSLContext.check_hostname.
            if HAS_SNI:
                ssl_sock = ssl_context.wrap_socket(sock, server_hostname=host)
            else:
                ssl_sock = ssl_context.wrap_socket(sock)
        except _CertificateError:
            sock.close()
            # Raise _CertificateError directly like we do after match_hostname
            # below.
            raise
        except (OSError, SSLError) as exc:
            sock.close()
            # We raise AutoReconnect for transient and permanent SSL handshake
            # failures alike. Permanent handshake failures, like protocol
            # mismatch, will be turned into ServerSelectionTimeoutErrors later.
            details = _get_timeout_details(options)
            _raise_connection_failure(
                address, exc, "SSL handshake failed: ", timeout_details=details
            )
        if (
            ssl_context.verify_mode
            and not ssl_context.check_hostname
            and not options.tls_allow_invalid_hostnames
        ):
            try:
                ssl.match_hostname(ssl_sock.getpeercert(), hostname=host)  # type:ignore[attr-defined,unused-ignore]
            except _CertificateError:
                ssl_sock.close()
                raise

        ssl_sock.settimeout(options.socket_timeout)
        return NetworkingInterface(ssl_sock)
