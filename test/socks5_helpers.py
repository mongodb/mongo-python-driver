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


class _Socks5Server:
    def __init__(
        self,
        method: int = 0,
        auth_status: int = 0,
        reply: int = 0,
        bound_atyp: int = 1,
        payload: bytes = b"",
        method_version: int = 5,
        reply_version: int = 5,
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
                version, nmethods = self._read(conn, 2)
                self.greeting = bytes((version, nmethods)) + self._read(conn, nmethods)
                self.started.set()
                if self.stall:
                    conn.recv(1)
                    return
                if self.eof_stage == "greeting":
                    conn.sendall(b"\x05")
                    return
                conn.sendall(bytes((self.method_version, self.method)))
                if self.method == 2:
                    auth_version, username_length = self._read(conn, 2)
                    username = self._read(conn, username_length)
                    password_length = self._read(conn, 1)[0]
                    password = self._read(conn, password_length)
                    self.auth = (
                        bytes((auth_version, username_length))
                        + username
                        + bytes((password_length,))
                        + password
                    )
                    if self.eof_stage == "auth":
                        conn.sendall(b"\x01")
                        return
                    conn.sendall(bytes((1, self.auth_status)))
                    if self.auth_status:
                        return
                version, command, reserved, atyp = self._read(conn, 4)
                if atyp == 1:
                    addr = self._read(conn, 4)
                elif atyp == 4:
                    addr = self._read(conn, 16)
                else:
                    length = self._read(conn, 1)
                    addr = length + self._read(conn, length[0])
                self.request = (
                    bytes((version, command, reserved, atyp)) + addr + self._read(conn, 2)
                )
                if self.eof_stage == "connect":
                    conn.sendall(b"\x05\x00")
                    return
                if self.target is not None:
                    upstream = socket.create_connection(self.target, timeout=3)
                if self.bound_atyp == 1:
                    bound = b"\x01\x7f\x00\x00\x01\x00\x00"
                elif self.bound_atyp == 4:
                    bound = b"\x04" + (b"\x00" * 16) + b"\x00\x00"
                else:
                    bound = b"\x03\x01x\x00\x00"
                conn.sendall(bytes((self.reply_version, self.reply, 0)) + bound + self.payload)
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
