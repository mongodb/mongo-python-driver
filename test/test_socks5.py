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

"""Tests for SOCKS5 proxy connection establishment."""

from __future__ import annotations

import asyncio
import select
import socket
import ssl
import sys
import threading
from types import SimpleNamespace
from typing import Any, Optional, cast
from unittest import mock

import pytest

sys.path[0:0] = [""]

from pymongo.encryption_options import _HAVE_PYMONGOCRYPT
from pymongo.pool_options import PoolOptions, _ProxyOptions
from pymongo.pool_shared import _configured_socket, _create_connection, _Socks5Error
from test import PyMongoTestCase, unittest
from test.helpers_shared import CERT_PATH

_IS_SYNC = True
pytestmark = pytest.mark.default


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


class TestSocks5(PyMongoTestCase):
    def _connect(
        self,
        server: _Socks5Server,
        address: tuple[str, int],
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 2,
    ) -> socket.socket:
        return _create_connection(
            address,
            PoolOptions(
                connect_timeout=timeout,
                proxy_options=_ProxyOptions(
                    server.address[0], server.address[1], username, password
                ),
            ),
        )

    def test_greeting_and_authenticated_connect(self) -> None:
        server = _Socks5Server(method=2)
        sock = self._connect(server, ("mongodb.example", 27017), "üser", "påss")
        sock.close()
        server.close()
        self.assertEqual(server.greeting, b"\x05\x02\x00\x02")
        self.assertEqual(server.auth, b"\x01\x05" + "üser".encode() + b"\x05" + "påss".encode())
        self.assertEqual(server.request[:4], b"\x05\x01\x00\x03")
        self.assertEqual(
            server.request[4:],
            bytes((len(b"mongodb.example"),)) + b"mongodb.example" + (27017).to_bytes(2, "big"),
        )

    def test_no_auth_method_is_preferred_when_credentials_exist(self) -> None:
        server = _Socks5Server(method=0)
        sock = self._connect(server, ("127.0.0.1", 27017), "user", "password")
        sock.close()
        server.close()
        self.assertEqual(server.greeting, b"\x05\x02\x00\x02")
        self.assertEqual(server.auth, b"")
        self.assertEqual(server.request, b"\x05\x01\x00\x01\x7f\x00\x00\x01\x69\x89")

    def test_no_credentials_only_offer_no_authentication(self) -> None:
        server = _Socks5Server(method=2)
        with self.assertRaises(_Socks5Error):
            self._connect(server, ("localhost", 27017))
        server.close()
        self.assertEqual(server.greeting, b"\x05\x01\x00")

    def test_rejects_all_unacceptable_methods(self) -> None:
        for method in (0x01, 0xFF):
            server = _Socks5Server(method=method)
            with self.subTest(method=method), self.assertRaises(_Socks5Error):
                self._connect(server, ("localhost", 27017))
            server.close()

    def test_rejects_authentication_failure(self) -> None:
        server = _Socks5Server(method=2, auth_status=1)
        with self.assertRaises(_Socks5Error):
            self._connect(server, ("localhost", 27017), "user", "password")
        server.close()
        self.assertEqual(server.auth, b"\x01\x04user\x08password")

    def test_connect_request_address_types(self) -> None:
        cases = (
            ("192.0.2.1", b"\x01\xc0\x00\x02\x01"),
            ("2001:db8::1", b"\x04" + bytes.fromhex("20010db8000000000000000000000001")),
            (
                "münich.example",
                b"\x03"
                + bytes((len("xn--mnich-kva.example"),))
                + "xn--mnich-kva.example".encode("ascii"),
            ),
        )
        for host, expected in cases:
            server = _Socks5Server()
            sock = self._connect(server, (host, 27017))
            sock.close()
            server.close()
            self.assertEqual(server.request, b"\x05\x01\x00" + expected + b"\x69\x89")

    def test_drains_bound_address_and_preserves_early_payload(self) -> None:
        for atyp in (1, 3, 4):
            server = _Socks5Server(bound_atyp=atyp, payload=b"mongo")
            sock = self._connect(server, ("localhost", 27017))
            if _IS_SYNC:
                payload = sock.recv(5)
            else:
                sock.setblocking(False)
                payload = asyncio.get_running_loop().sock_recv(sock, 5)
            sock.close()
            server.close()
            self.assertEqual(payload, b"mongo")

    def test_rejects_socks_reply_codes(self) -> None:
        for reply in range(1, 9):
            server = _Socks5Server(reply=reply)
            with self.subTest(reply=reply), self.assertRaises(_Socks5Error):
                self._connect(server, ("localhost", 27017))
            server.close()

    def test_proxy_options_reach_server_and_monitor_pools(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient
        from pymongo.synchronous.topology import Topology

        client = MongoClient("mongodb://localhost/?proxyHost=127.0.0.1", connect=False)
        try:
            topology = Topology(client._topology_settings)
            server_pool = topology._create_pool_for_server(("localhost", 27017))
            monitor_pool = topology._create_pool_for_monitor(("localhost", 27017))
            expected = _ProxyOptions("127.0.0.1")
            self.assertEqual(server_pool.opts._proxy_options, expected)
            self.assertEqual(monitor_pool.opts._proxy_options, expected)
        finally:
            client.close()

    def test_kwargs_override_uri_proxy_options_and_empty_values_are_ignored(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient

        client = MongoClient(
            "mongodb://localhost/?proxyHost=localhost&proxyPort=1234",
            proxyHost="[::1]",
            proxyPort=5678,
            proxyUsername="",
            proxyPassword="",
            connect=False,
        )
        try:
            self.assertEqual(client.options.pool_options._proxy_options, _ProxyOptions("::1", 5678))
        finally:
            client.close()

    def test_bad_versions_and_unexpected_eof_fail(self) -> None:
        cases = (
            (_Socks5Server(method_version=4), ("localhost", 27017), None),
            (_Socks5Server(reply_version=4), ("localhost", 27017), None),
            (_Socks5Server(eof_stage="greeting"), ("localhost", 27017), None),
            (
                _Socks5Server(method=2, eof_stage="auth"),
                ("localhost", 27017),
                ("user", "password"),
            ),
            (_Socks5Server(eof_stage="connect"), ("localhost", 27017), None),
        )
        for server, address, credentials in cases:
            with self.subTest(stage=server.eof_stage, version=server.method_version):
                with self.assertRaises(_Socks5Error):
                    self._connect(server, address, *(credentials or (None, None)))
            server.close()

    def test_timeout_covers_the_entire_handshake(self) -> None:
        server = _Socks5Server(stall=True)
        with self.assertRaises(socket.timeout):
            self._connect(server, ("localhost", 27017), timeout=0.1)
        server.close()

    @unittest.skipIf(_IS_SYNC, "Async cancellation is tested in the synchronous driver")
    def test_cancellation_closes_proxy_socket(self) -> None:
        server = _Socks5Server(stall=True)
        task = asyncio.create_task(cast(Any, self._connect(server, ("localhost", 27017))))
        asyncio.to_thread(server.started.wait, 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            task
        server.close()

    def test_tls_sni_targets_destination_not_proxy(self) -> None:
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        target_address = listener.getsockname()
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.load_cert_chain(f"{CERT_PATH}/server.pem")
        sni_names: list[Optional[str]] = []
        tls_context.set_servername_callback(lambda _sock, name, _ctx: sni_names.append(name))

        def serve_tls() -> None:
            try:
                raw, _ = listener.accept()
                with tls_context.wrap_socket(raw, server_side=True) as conn:
                    conn.recv(1)
            except OSError:
                pass
            finally:
                listener.close()

        tls_thread = threading.Thread(target=serve_tls, daemon=True)
        tls_thread.start()
        proxy = _Socks5Server(target=target_address)
        client_context = ssl.create_default_context()
        client_context.check_hostname = False
        client_context.verify_mode = ssl.CERT_NONE
        sock = _configured_socket(
            ("mongodb.example", target_address[1]),
            PoolOptions(
                connect_timeout=2,
                ssl_context=cast(Any, client_context),
                proxy_options=_ProxyOptions(proxy.address[0], proxy.address[1]),
            ),
        )
        sock.close()
        proxy.close()
        tls_thread.join(timeout=3)
        self.assertEqual(sni_names, ["mongodb.example"])

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_kms_request_uses_client_proxy_options(self) -> None:
        import pymongo.synchronous.encryption as encryption_module
        from pymongo.synchronous.encryption import _EncryptionIO
        from pymongo.synchronous.mongo_client import MongoClient

        class FakeOpts:
            _kms_connect_callback = None

            @staticmethod
            def _kms_ssl_contexts(_is_sync: bool) -> dict[str, object]:
                return {"aws": object()}

        client = MongoClient("mongodb://localhost/?proxyHost=127.0.0.1", connect=False)
        expected = _ProxyOptions("127.0.0.1")
        io = _EncryptionIO(client, client.keyvault.datakeys, None, FakeOpts())  # type: ignore[arg-type]
        connection = mock.MagicMock()

        def connect(_address, options, _callback, _timeout):
            self.assertEqual(options._proxy_options, expected)
            return connection

        def sendall(_connection, _message):
            pass

        try:
            with (
                mock.patch.object(encryption_module, "_connect_kms", new=connect),
                mock.patch.object(encryption_module, "sendall", new=sendall),
            ):
                io.kms_request(
                    SimpleNamespace(
                        endpoint="kms.example:443",
                        message=b"request",
                        kms_provider="aws",
                        usleep=0,
                        bytes_needed=0,
                    )
                )
        finally:
            client.close()
        connection.close.assert_called_once_with()

    def test_proxy_settings_are_stored_in_pool_options(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient

        client = MongoClient("mongodb://localhost/?proxyHost=[::1]", connect=False)
        try:
            self.assertEqual(client.options.pool_options._proxy_options, _ProxyOptions("::1"))
        finally:
            client.close()
