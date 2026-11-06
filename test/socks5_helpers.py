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

"""Threaded SOCKS5 test servers shared by synchronous and asynchronous tests."""

from __future__ import annotations

import select
import socket
import threading
from typing import Optional

from pymongo.socks5_shared import (
    _SOCKS5_ATYP_DOMAIN,
    _SOCKS5_ATYP_IPV4,
    _SOCKS5_ATYP_IPV6,
    _SOCKS5_AUTH_NO_AUTH,
    _SOCKS5_AUTH_REPLY_LEN,
    _SOCKS5_AUTH_SUCCESS,
    _SOCKS5_AUTH_USER_PASS,
    _SOCKS5_AUTH_VERSION,
    _SOCKS5_CONNECT_REPLY_LEN,
    _SOCKS5_DOMAIN_LENGTH_LEN,
    _SOCKS5_GREETING_REPLY_LEN,
    _SOCKS5_IPV4_BOUND_LEN,
    _SOCKS5_IPV6_BOUND_LEN,
    _SOCKS5_PORT_LEN,
    _SOCKS5_REPLY_SUCCESS,
    _SOCKS5_RESERVED,
    _SOCKS5_VERSION,
)


class _Socks5Server:
    def __init__(
        self,
        method: int = _SOCKS5_AUTH_NO_AUTH,
        auth_status: int = _SOCKS5_AUTH_SUCCESS,
        reply: int = _SOCKS5_REPLY_SUCCESS,
        bound_atyp: int = _SOCKS5_ATYP_IPV4,
        payload: bytes = b"",
        method_version: int = _SOCKS5_VERSION,
        reply_version: int = _SOCKS5_VERSION,
        eof_stage: Optional[str] = None,
        stall: bool = False,
        target: Optional[tuple[str, int]] = None,
    ) -> None:
        self.method = method
        self.auth_status = auth_status
        self.reply = reply
        self.bound_atyp = bound_atyp
        self.payload = payload
        self.method_version = method_version
        self.reply_version = reply_version
        self.eof_stage = eof_stage
        self.stall = stall
        self.target = target
        self.started = threading.Event()
        self.greeting = b""
        self.auth = b""
        self.request = b""
        self.error: Optional[BaseException] = None
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.settimeout(3)
        self.address = self.listener.getsockname()
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    @staticmethod
    def _read(sock: socket.socket, size: int) -> bytes:
        data = bytearray()
        while len(data) < size:
            part = sock.recv(size - len(data))
            if not part:
                raise EOFError("client closed connection")
            data.extend(part)
        return bytes(data)

    def _serve(self) -> None:
        upstream: Optional[socket.socket] = None
        try:
            with self.listener.accept()[0] as conn:
                conn.settimeout(3)
                version, nmethods = self._read(conn, _SOCKS5_GREETING_REPLY_LEN)
                self.greeting = bytes((version, nmethods)) + self._read(conn, nmethods)
                self.started.set()
                if self.stall:
                    conn.recv(1)
                    return
                if self.eof_stage == "greeting":
                    conn.sendall(bytes((_SOCKS5_VERSION,)))
                    return
                conn.sendall(bytes((self.method_version, self.method)))
                if self.method == _SOCKS5_AUTH_USER_PASS:
                    auth_version, username_length = self._read(conn, _SOCKS5_AUTH_REPLY_LEN)
                    username = self._read(conn, username_length)
                    password_length = self._read(conn, _SOCKS5_DOMAIN_LENGTH_LEN)[0]
                    password = self._read(conn, password_length)
                    self.auth = (
                        bytes((auth_version, username_length))
                        + username
                        + bytes((password_length,))
                        + password
                    )
                    if self.eof_stage == "auth":
                        conn.sendall(bytes((_SOCKS5_AUTH_VERSION,)))
                        return
                    conn.sendall(bytes((_SOCKS5_AUTH_VERSION, self.auth_status)))
                    if self.auth_status:
                        return
                version, command, reserved, atyp = self._read(conn, _SOCKS5_CONNECT_REPLY_LEN)
                if atyp == _SOCKS5_ATYP_IPV4:
                    addr = self._read(conn, _SOCKS5_IPV4_BOUND_LEN - _SOCKS5_PORT_LEN)
                elif atyp == _SOCKS5_ATYP_IPV6:
                    addr = self._read(conn, _SOCKS5_IPV6_BOUND_LEN - _SOCKS5_PORT_LEN)
                else:
                    length = self._read(conn, _SOCKS5_DOMAIN_LENGTH_LEN)
                    addr = length + self._read(conn, length[0])
                self.request = (
                    bytes((version, command, reserved, atyp))
                    + addr
                    + self._read(conn, _SOCKS5_PORT_LEN)
                )
                if self.eof_stage == "connect":
                    conn.sendall(bytes((_SOCKS5_VERSION, _SOCKS5_REPLY_SUCCESS)))
                    return
                if self.target is not None:
                    upstream = socket.create_connection(self.target, timeout=3)
                if self.bound_atyp == _SOCKS5_ATYP_IPV4:
                    bound = (
                        bytes((_SOCKS5_ATYP_IPV4,))
                        + socket.inet_aton("127.0.0.1")
                        + bytes(_SOCKS5_PORT_LEN)
                    )
                elif self.bound_atyp == _SOCKS5_ATYP_IPV6:
                    bound = (
                        bytes((_SOCKS5_ATYP_IPV6,))
                        + socket.inet_pton(socket.AF_INET6, "::1")
                        + bytes(_SOCKS5_PORT_LEN)
                    )
                else:
                    host = b"x"
                    bound = bytes((_SOCKS5_ATYP_DOMAIN, len(host))) + host + bytes(_SOCKS5_PORT_LEN)
                conn.sendall(
                    bytes((self.reply_version, self.reply, _SOCKS5_RESERVED)) + bound + self.payload
                )
                if self.payload:
                    return
                if upstream is not None:
                    conn.setblocking(False)
                    upstream.setblocking(False)
                    while True:
                        readable, _, _ = select.select([conn, upstream], [], [], 5)
                        if not readable:
                            return
                        for source in readable:
                            data = source.recv(65536)
                            if not data:
                                return
                            destination = upstream if source is conn else conn
                            destination.sendall(data)
                else:
                    while conn.recv(1024):
                        pass
        except BaseException as exc:
            self.error = exc
        finally:
            if upstream is not None:
                upstream.close()
            self.listener.close()

    def close(self) -> None:
        self.listener.close()
        self.thread.join(timeout=3)
