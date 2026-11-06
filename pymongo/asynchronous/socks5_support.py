# Copyright 2026-present MongoDB, Inc.
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

"""Asynchronous SOCKS5 proxy connection establishment."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING, Optional

from pymongo.pool_shared import _configure_tcp_socket, _new_tcp_socket
from pymongo.socks5_shared import (
    _SOCKS5_ATYP_DOMAIN,
    _SOCKS5_AUTH_REPLY_LEN,
    _SOCKS5_AUTH_USER_PASS,
    _SOCKS5_CONNECT_REPLY_LEN,
    _SOCKS5_DOMAIN_LENGTH_LEN,
    _SOCKS5_GREETING_REPLY_LEN,
    _async_proxy_connect,
    _async_proxy_getaddrinfo,
    _async_socks5_recv,
    _async_socks5_sendall,
    _proxy_deadline,
    _proxy_remaining,
    _proxy_socket_timeout,
    _socks5_auth_request,
    _socks5_bound_address_size,
    _socks5_check_auth_reply,
    _socks5_check_connect_reply,
    _socks5_check_greeting_reply,
    _socks5_greeting,
    _socks5_request,
    _Socks5Error,
    _Socks5Timeout,
)

if TYPE_CHECKING:
    from pymongo.pool_options import PoolOptions, _ProxyOptions
    from pymongo.typings import _Address

_IS_SYNC = False


async def _async_socks5_read_exact(
    sock: socket.socket, size: int, deadline: Optional[float]
) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = await _async_socks5_recv(sock, size - len(data), deadline)
        if not chunk:
            raise _Socks5Error("SOCKS5 handshake failed: unexpected EOF")
        data.extend(chunk)
    return bytes(data)


async def _async_socks5_handshake(
    sock: socket.socket, address: _Address, proxy: _ProxyOptions, deadline: Optional[float]
) -> None:
    greeting, allowed = _socks5_greeting(proxy)
    await _async_socks5_sendall(sock, greeting, deadline)
    method = _socks5_check_greeting_reply(
        await _async_socks5_read_exact(sock, _SOCKS5_GREETING_REPLY_LEN, deadline), allowed
    )
    if method == _SOCKS5_AUTH_USER_PASS:
        await _async_socks5_sendall(sock, _socks5_auth_request(proxy), deadline)
        _socks5_check_auth_reply(
            await _async_socks5_read_exact(sock, _SOCKS5_AUTH_REPLY_LEN, deadline)
        )
    await _async_socks5_sendall(sock, _socks5_request(address), deadline)
    atyp = _socks5_check_connect_reply(
        await _async_socks5_read_exact(sock, _SOCKS5_CONNECT_REPLY_LEN, deadline)
    )
    domain_length = (
        (await _async_socks5_read_exact(sock, _SOCKS5_DOMAIN_LENGTH_LEN, deadline))[0]
        if atyp == _SOCKS5_ATYP_DOMAIN
        else None
    )
    bound_size = _socks5_bound_address_size(atyp, domain_length)
    await _async_socks5_read_exact(sock, bound_size, deadline)


async def _async_create_proxy_connection(
    address: _Address, options: PoolOptions, proxy: _ProxyOptions
) -> socket.socket:
    deadline = _proxy_deadline(options)
    family = socket.AF_INET
    if socket.has_ipv6 and proxy.host != "localhost":
        family = socket.AF_UNSPEC
    try:
        results = await _async_proxy_getaddrinfo(proxy, family, deadline)
    except TimeoutError as exc:
        raise _Socks5Timeout("SOCKS5 proxy timed out") from exc
    except OSError as exc:
        raise _Socks5Error(f"SOCKS5 proxy connection failed: {exc}") from exc

    err: Optional[OSError] = None
    for af, socktype, proto, _, sockaddr in results:
        sock = _new_tcp_socket(af, socktype, proto)
        connected = False
        try:
            _configure_tcp_socket(sock, non_blocking=not _IS_SYNC)
            await _async_proxy_connect(sock, sockaddr, deadline)
            connected = True
            await _async_socks5_handshake(sock, address, proxy, deadline)
            sock.settimeout(_proxy_socket_timeout(options))
            return sock
        except TimeoutError as exc:
            sock.close()
            if connected:
                raise _Socks5Timeout("SOCKS5 proxy timed out") from exc
            try:
                _proxy_remaining(deadline)
            except _Socks5Timeout as timeout:
                raise timeout from exc
            err = _Socks5Timeout("SOCKS5 proxy timed out")
            err.__cause__ = exc
        except _Socks5Error:
            sock.close()
            raise
        except OSError as exc:
            sock.close()
            err = exc
            # A connected proxy's handshake failure must not try a different
            # resolved proxy address; the failure is specific to the endpoint.
            if connected:
                raise _Socks5Error(f"SOCKS5 handshake failed: {exc}") from exc
        except BaseException:
            sock.close()
            raise

    if isinstance(err, socket.timeout):
        raise err
    if err is not None:
        raise _Socks5Error(f"SOCKS5 proxy connection failed: {err}") from err
    raise _Socks5Error("SOCKS5 proxy connection failed: getaddrinfo returned no addresses")
