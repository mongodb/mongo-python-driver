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


class Socks5Server:
    """Small threaded SOCKS5 forwarder for the driver prose tests."""

    def __init__(
        self,
        port: int,
        mapping: dict[tuple[str, int], tuple[str, int]],
        credentials: Optional[tuple[str, str]] = None,
    ) -> None:
        self.mapping = mapping
        self.credentials = credentials
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        try:
            self.listener.bind(("127.0.0.1", port))
        except OSError:
            self.listener.bind(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(0.5)
        self._closed = threading.Event()
        self._threads: list[threading.Thread] = []
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    @staticmethod
    def _read(sock: socket.socket, length: int) -> bytes:
        data = bytearray()
        while len(data) < length:
            part = sock.recv(length - len(data))
            if not part:
                raise EOFError
            data.extend(part)
        return bytes(data)

    def _serve_client(self, client: socket.socket) -> None:
        upstream: Optional[socket.socket] = None
        try:
            client.settimeout(5)
            version, method_count = self._read(client, 2)
            methods = self._read(client, method_count)
            if version != 5:
                return
            if self.credentials is not None:
                method = 2 if 2 in methods else 0xFF
            else:
                method = 0 if 0 in methods else 0xFF
            client.sendall(bytes((5, method)))
            if method == 0xFF:
                return
            if method == 2:
                version, username_length = self._read(client, 2)
                username = self._read(client, username_length)
                password_length = self._read(client, 1)[0]
                password = self._read(client, password_length)
                user, secret = self.credentials or ("", "")
                status = (
                    0
                    if version == 1 and username == user.encode() and password == secret.encode()
                    else 1
                )
                client.sendall(bytes((1, status)))
                if status:
                    return
            version, command, reserved, atyp = self._read(client, 4)
            if atyp == 1:
                host = socket.inet_ntop(socket.AF_INET, self._read(client, 4))
            elif atyp == 4:
                host = socket.inet_ntop(socket.AF_INET6, self._read(client, 16))
            elif atyp == 3:
                length = self._read(client, 1)[0]
                host = self._read(client, length).decode("idna")
            else:
                client.sendall(b"\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00")
                return
            port = int.from_bytes(self._read(client, 2), "big")
            if version != 5 or command != 1 or reserved != 0:
                client.sendall(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                return
            target = self.mapping.get((host.lower(), port), (host, port))
            try:
                upstream = socket.create_connection(target, timeout=5)
            except OSError:
                client.sendall(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
                return
            bound_host, bound_port = upstream.getsockname()[:2]
            try:
                bound_ip = socket.inet_pton(socket.AF_INET, bound_host)
                atyp = 1
            except OSError:
                bound_ip = socket.inet_pton(socket.AF_INET6, bound_host)
                atyp = 4
            client.sendall(bytes((5, 0, 0, atyp)) + bound_ip + int(bound_port).to_bytes(2, "big"))
            client.setblocking(False)
            upstream.setblocking(False)
            while True:
                readable, _, _ = select.select([client, upstream], [], [], 10)
                if not readable:
                    continue
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    target_sock = upstream if source is client else client
                    target_sock.sendall(data)
        except (OSError, EOFError):
            pass
        finally:
            if upstream is not None:
                upstream.close()
            client.close()

    def _serve(self) -> None:
        while not self._closed.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            thread = threading.Thread(target=self._serve_client, args=(client,), daemon=True)
            self._threads.append(thread)
            thread.start()

    def close(self) -> None:
        self._closed.set()
        self.listener.close()
        self._thread.join(timeout=2)
        for thread in self._threads:
            thread.join(timeout=2)
