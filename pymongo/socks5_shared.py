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

"""SOCKS5 protocol utilities and socket helpers shared by both APIs."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import time
from typing import TYPE_CHECKING, Optional

from pymongo import _csot
from pymongo.asynchronous.helpers import _getaddrinfo
from pymongo.common import _SOCKS5_MAX_STR_LEN

if TYPE_CHECKING:
    from pymongo.pool_options import PoolOptions, _ProxyOptions
    from pymongo.typings import _Address

_SOCKS5_VERSION = 0x05
_SOCKS5_AUTH_VERSION = 0x01
_SOCKS5_AUTH_NO_AUTH = 0x00
_SOCKS5_AUTH_USER_PASS = 0x02
_SOCKS5_AUTH_NO_ACCEPTABLE = 0xFF
_SOCKS5_AUTH_SUCCESS = 0x00
_SOCKS5_RESERVED = 0x00
_SOCKS5_AUTH_METHOD_COUNT_NO_CREDENTIALS = 1
_SOCKS5_AUTH_METHOD_COUNT_WITH_CREDENTIALS = 2
_SOCKS5_CMD_CONNECT = 0x01
_SOCKS5_REPLY_GENERAL_FAILURE = 0x01
_SOCKS5_REPLY_RULESET_DENIED = 0x02
_SOCKS5_REPLY_NETWORK_UNREACHABLE = 0x03
_SOCKS5_REPLY_HOST_UNREACHABLE = 0x04
_SOCKS5_REPLY_CONNECTION_REFUSED = 0x05
_SOCKS5_REPLY_TTL_EXPIRED = 0x06
_SOCKS5_REPLY_COMMAND_UNSUPPORTED = 0x07
_SOCKS5_REPLY_ADDRESS_UNSUPPORTED = 0x08
_SOCKS5_ATYP_IPV4 = 0x01
_SOCKS5_ATYP_DOMAIN = 0x03
_SOCKS5_ATYP_IPV6 = 0x04
_SOCKS5_IPV4_BOUND_LEN = 6
_SOCKS5_IPV6_BOUND_LEN = 18
_SOCKS5_PORT_LEN = 2
_SOCKS5_GREETING_REPLY_LEN = 2
_SOCKS5_AUTH_REPLY_LEN = 2
_SOCKS5_CONNECT_REPLY_LEN = 4
_SOCKS5_DOMAIN_LENGTH_LEN = 1

_SOCKS5_REPLY_REASONS = {
    _SOCKS5_REPLY_GENERAL_FAILURE: "general SOCKS server failure",
    _SOCKS5_REPLY_RULESET_DENIED: "connection not allowed by ruleset",
    _SOCKS5_REPLY_NETWORK_UNREACHABLE: "network unreachable",
    _SOCKS5_REPLY_HOST_UNREACHABLE: "host unreachable",
    _SOCKS5_REPLY_CONNECTION_REFUSED: "connection refused",
    _SOCKS5_REPLY_TTL_EXPIRED: "TTL expired",
    _SOCKS5_REPLY_COMMAND_UNSUPPORTED: "command not supported",
    _SOCKS5_REPLY_ADDRESS_UNSUPPORTED: "address type not supported",
}

_ProxyAddress = tuple[str, int] | tuple[str, int, int, int] | tuple[int, bytes]
_ProxyAddressInfo = tuple[socket.AddressFamily, socket.SocketKind, int, str, _ProxyAddress]


class _Socks5Error(OSError):
    """A SOCKS5 proxy connection or protocol failure."""


class _Socks5Timeout(socket.timeout):
    """A timeout while connecting through a SOCKS5 proxy."""


def _proxy_deadline(options: PoolOptions) -> Optional[float]:
    timeout = _csot.remaining()
    if timeout is None:
        timeout = options.connect_timeout
    elif timeout <= 0:
        raise _Socks5Timeout("timed out")
    if timeout is None:
        return None
    return time.monotonic() + timeout


def _proxy_socket_timeout(options: PoolOptions) -> Optional[float]:
    timeout = _csot.remaining() if _csot.get_timeout() else options.connect_timeout
    if timeout is not None and timeout <= 0:
        raise _Socks5Timeout("timed out")
    return timeout


def _proxy_remaining(deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _Socks5Timeout("timed out")
    return remaining


def _socks5_bound_address_size(atyp: int, domain_length: Optional[int] = None) -> int:
    if atyp == _SOCKS5_ATYP_IPV4:
        return _SOCKS5_IPV4_BOUND_LEN
    if atyp == _SOCKS5_ATYP_IPV6:
        return _SOCKS5_IPV6_BOUND_LEN
    if domain_length is None:
        raise _Socks5Error("SOCKS5 connect failed: missing bound domain length")
    return domain_length + _SOCKS5_PORT_LEN


def _socks5_greeting(proxy: _ProxyOptions) -> tuple[bytes, tuple[int, ...]]:
    if proxy.username is not None and proxy.password is not None:
        return bytes(
            (
                _SOCKS5_VERSION,
                _SOCKS5_AUTH_METHOD_COUNT_WITH_CREDENTIALS,
                _SOCKS5_AUTH_NO_AUTH,
                _SOCKS5_AUTH_USER_PASS,
            )
        ), (
            _SOCKS5_AUTH_NO_AUTH,
            _SOCKS5_AUTH_USER_PASS,
        )
    return bytes(
        (_SOCKS5_VERSION, _SOCKS5_AUTH_METHOD_COUNT_NO_CREDENTIALS, _SOCKS5_AUTH_NO_AUTH)
    ), (_SOCKS5_AUTH_NO_AUTH,)


def _socks5_check_greeting_reply(data: bytes, allowed: tuple[int, ...]) -> int:
    version, method = data
    if version != _SOCKS5_VERSION:
        raise _Socks5Error(f"SOCKS5 initial handshake failed: unexpected version {version:#04x}")
    if method not in allowed:
        if method == _SOCKS5_AUTH_NO_ACCEPTABLE:
            raise _Socks5Error(
                "SOCKS5 initial handshake failed: no acceptable authentication methods"
            )
        raise _Socks5Error(
            f"SOCKS5 initial handshake failed: unexpected authentication method {method:#04x}"
        )
    return method


def _socks5_auth_request(proxy: _ProxyOptions) -> bytes:
    assert proxy.username is not None
    assert proxy.password is not None
    username = proxy.username.encode("utf-8")
    password = proxy.password.encode("utf-8")
    return (
        bytes((_SOCKS5_AUTH_VERSION, len(username))) + username + bytes((len(password),)) + password
    )


def _socks5_check_auth_reply(data: bytes) -> None:
    auth_version, status = data
    if auth_version != _SOCKS5_AUTH_VERSION or status != _SOCKS5_AUTH_SUCCESS:
        raise _Socks5Error("SOCKS5 authentication failed")


def _socks5_request(address: _Address) -> bytes:
    host, port = address
    if port is None:
        raise _Socks5Error("SOCKS5 connect failed: destination port is required")
    try:
        ip = ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        try:
            domain = host.encode("ascii") if host.isascii() else host.encode("idna")
        except UnicodeError as exc:
            raise _Socks5Error(f"SOCKS5 connect failed: invalid domain name: {exc}") from exc
        if not 1 <= len(domain) <= _SOCKS5_MAX_STR_LEN:
            raise _Socks5Error("SOCKS5 connect failed: invalid domain name length") from None
        encoded_host = bytes((_SOCKS5_ATYP_DOMAIN, len(domain))) + domain
    else:
        encoded_host = (
            bytes((_SOCKS5_ATYP_IPV4 if ip.version == 4 else _SOCKS5_ATYP_IPV6,)) + ip.packed
        )
    return (
        bytes((_SOCKS5_VERSION, _SOCKS5_CMD_CONNECT, _SOCKS5_RESERVED))
        + encoded_host
        + port.to_bytes(_SOCKS5_PORT_LEN, "big")
    )


def _socks5_check_connect_reply(data: bytes) -> int:
    version, reply, reserved, atyp = data
    if version != _SOCKS5_VERSION:
        raise _Socks5Error(f"SOCKS5 connect failed: unexpected version {version:#04x}")
    if reserved != _SOCKS5_RESERVED:
        raise _Socks5Error(f"SOCKS5 connect failed: invalid reserved byte {reserved:#04x}")
    if reply:
        reason = _SOCKS5_REPLY_REASONS.get(reply, f"unknown error {reply:#04x}")
        raise _Socks5Error(f"SOCKS5 connect failed: {reason}")
    if atyp not in (_SOCKS5_ATYP_IPV4, _SOCKS5_ATYP_DOMAIN, _SOCKS5_ATYP_IPV6):
        raise _Socks5Error(f"SOCKS5 connect failed: invalid address type {atyp:#04x}")
    return atyp


def _socks5_sendall(sock: socket.socket, message: bytes, deadline: Optional[float]) -> None:
    sock.settimeout(_proxy_remaining(deadline))
    sock.sendall(message)


async def _async_socks5_sendall(
    sock: socket.socket, message: bytes, deadline: Optional[float]
) -> None:
    loop = asyncio.get_running_loop()
    async with asyncio.timeout(_proxy_remaining(deadline)):
        await loop.sock_sendall(sock, message)


def _socks5_recv(sock: socket.socket, size: int, deadline: Optional[float]) -> bytes:
    sock.settimeout(_proxy_remaining(deadline))
    return sock.recv(size)


async def _async_socks5_recv(sock: socket.socket, size: int, deadline: Optional[float]) -> bytes:
    loop = asyncio.get_running_loop()
    async with asyncio.timeout(_proxy_remaining(deadline)):
        return await loop.sock_recv(sock, size)


def _proxy_getaddrinfo(
    proxy: _ProxyOptions, family: int, deadline: Optional[float]
) -> list[_ProxyAddressInfo]:
    _proxy_remaining(deadline)
    return socket.getaddrinfo(proxy.host, proxy.port, family=family, type=socket.SOCK_STREAM)


async def _async_proxy_getaddrinfo(
    proxy: _ProxyOptions, family: int, deadline: Optional[float]
) -> list[_ProxyAddressInfo]:
    async with asyncio.timeout(_proxy_remaining(deadline)):
        return await _getaddrinfo(proxy.host, proxy.port, family=family, type=socket.SOCK_STREAM)


def _proxy_connect(sock: socket.socket, address: _ProxyAddress, deadline: Optional[float]) -> None:
    sock.settimeout(_proxy_remaining(deadline))
    sock.connect(address)


async def _async_proxy_connect(
    sock: socket.socket, address: _ProxyAddress, deadline: Optional[float]
) -> None:
    loop = asyncio.get_running_loop()
    async with asyncio.timeout(_proxy_remaining(deadline)):
        await loop.sock_connect(sock, address)
