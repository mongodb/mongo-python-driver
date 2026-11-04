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

    def test_proxy_password_is_redacted_in_repr(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient

        for client in (
            MongoClient(
                "mongodb://user:dbsecret@localhost/?proxyHost=127.0.0.1"
                "&proxyUsername=proxyuser&proxyPassword=hunter2",
                connect=False,
            ),
            MongoClient(
                "mongodb://localhost/",
                username="user",
                password="dbsecret",
                proxyHost="127.0.0.1",
                proxyUsername="proxyuser",
                proxyPassword="hunter2",
                connect=False,
            ),
        ):
            try:
                client_repr = repr(client)
                proxy_repr = repr(client.options.pool_options._proxy_options)
                self.assertNotIn("hunter2", client_repr)
                self.assertNotIn("proxyuser", client_repr)
                self.assertNotIn("dbsecret", client_repr)
                self.assertNotIn("hunter2", proxy_repr)
                self.assertIn("127.0.0.1", proxy_repr)
            finally:
                client.close()

    def test_split_proxy_options_across_uri_and_kwargs(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient

        client1 = MongoClient(
            "mongodb://localhost/?proxyPort=1081",
            proxyHost="proxy.example",
            connect=False,
        )
        client2 = MongoClient(
            "mongodb://localhost/?proxyHost=proxy.example&proxyUsername=u",
            proxyPassword="p",
            connect=False,
        )
        try:
            self.assertEqual(
                client1.options.pool_options._proxy_options,
                _ProxyOptions("proxy.example", 1081),
            )
            self.assertEqual(
                client2.options.pool_options._proxy_options,
                _ProxyOptions("proxy.example", 1080, "u", "p"),
            )
        finally:
            client1.close()
            client2.close()

    def test_malformed_proxy_uri_options_raise_instead_of_warning(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient

        for uri in (
            "mongodb://localhost/?proxyHost=",
            "mongodb://localhost/?proxyHost=proxy%3A1080",
            "mongodb://localhost/?proxyHost=local%20host",
            "mongodb://localhost/?proxyHost=localhost&proxyPort=0",
            "mongodb://localhost/?proxyHost=localhost&proxyPort=abc",
        ):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                MongoClient(uri, connect=False)

    def test_socks5_errors_are_not_labeled_system_overloaded(self) -> None:
        from pymongo.errors import AutoReconnect, NetworkTimeout
        from pymongo.pool_shared import _raise_connection_failure, _Socks5Timeout
        from pymongo.synchronous.pool import Pool

        pool = Pool(("localhost", 27017), PoolOptions(connect_timeout=1))
        for cause in (_Socks5Error("boom"), _Socks5Timeout("timed out")):
            try:
                _raise_connection_failure(("localhost", 27017), cause)
            except (AutoReconnect, NetworkTimeout) as wrapped:
                pool._handle_connection_error(wrapped)
                self.assertFalse(wrapped.has_error_label("SystemOverloadedError"))
                self.assertFalse(wrapped.has_error_label("RetryableError"))

    def test_multi_address_proxy_fallback_on_connect_error(self) -> None:
        server = _Socks5Server()
        self.addCleanup(server.close)
        dead_sock = socket.socket()
        dead_sock.bind(("127.0.0.1", 0))
        dead_addr = dead_sock.getsockname()
        dead_sock.close()

        fake_results = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", dead_addr),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", server.address),
        ]
        target = (
            "pymongo.pool_shared.socket.getaddrinfo"
            if _IS_SYNC
            else "pymongo.pool_shared._getaddrinfo"
        )

        def async_fake_getaddrinfo(*_args: Any, **_kwargs: Any) -> Any:
            return fake_results

        replacement: Any = (lambda *_a, **_kw: fake_results) if _IS_SYNC else async_fake_getaddrinfo
        with mock.patch(target, new=replacement):
            sock = self._connect(server, ("localhost", 27017))
            sock.close()

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_kms_proxy_uses_parent_client_even_with_bypass_auto_encryption(self) -> None:
        from pymongo.synchronous.encryption import _EncryptionIO
        from pymongo.synchronous.mongo_client import MongoClient

        class FakeOpts:
            _kms_connect_callback = None

            @staticmethod
            def _kms_ssl_contexts(_is_sync: bool) -> dict[str, object]:
                return {"aws": object()}

        parent_client = MongoClient("mongodb://localhost/?proxyHost=10.0.0.1", connect=False)
        kv_client = MongoClient("mongodb://localhost/?proxyHost=10.0.0.2", connect=False)
        try:
            io_bypass = _EncryptionIO(
                None,
                kv_client.keyvault.datakeys,
                None,
                FakeOpts(),  # type: ignore[arg-type]
                proxy_client=parent_client,
            )
            self.assertEqual(io_bypass._proxy_options, _ProxyOptions("10.0.0.1"))

            io_explicit = _EncryptionIO(
                None,
                kv_client.keyvault.datakeys,
                None,
                FakeOpts(),  # type: ignore[arg-type]
                proxy_client=kv_client,
            )
            self.assertEqual(io_explicit._proxy_options, _ProxyOptions("10.0.0.2"))
        finally:
            parent_client.close()
            kv_client.close()

    def test_socks5_helper_edge_cases(self) -> None:
        from pymongo.common import validate_proxy_host, validate_proxy_port
        from pymongo.pool_shared import (
            _proxy_deadline,
            _proxy_remaining,
            _socks5_check_connect_reply,
            _socks5_request,
            _Socks5Timeout,
        )

        self.assertIsNone(validate_proxy_host("proxyHost", None))
        self.assertEqual(validate_proxy_host("proxyHost", "::1"), "::1")
        with self.assertRaises(ValueError):
            validate_proxy_host("proxyHost", "[not-ipv6]")
        with self.assertRaises(ValueError):
            validate_proxy_host("proxyHost", "bad/host")
        self.assertIsNone(validate_proxy_port("proxyPort", None))
        with self.assertRaises(TypeError):
            validate_proxy_port("proxyPort", True)

        self.assertIsNone(_proxy_deadline(PoolOptions(connect_timeout=None)))
        self.assertIsNone(_proxy_remaining(None))
        with self.assertRaises(_Socks5Timeout):
            _proxy_remaining(0.0)
        with (
            mock.patch("pymongo.pool_shared._csot.remaining", return_value=0.0),
            self.assertRaises(_Socks5Timeout),
        ):
            _proxy_deadline(PoolOptions(connect_timeout=1))

        with self.assertRaisesRegex(_Socks5Error, "invalid domain name length"):
            _socks5_request(("a" * 256, 27017))
        with self.assertRaisesRegex(_Socks5Error, "invalid domain name"):
            _socks5_request((".münich.example", 27017))
        with self.assertRaisesRegex(_Socks5Error, "destination port is required"):
            _socks5_request(("localhost", None))  # type: ignore[arg-type]
        with self.assertRaisesRegex(_Socks5Error, "invalid reserved byte"):
            _socks5_check_connect_reply(b"\x05\x00\x01\x01")
        with self.assertRaisesRegex(_Socks5Error, "invalid address type"):
            _socks5_check_connect_reply(b"\x05\x00\x00\x09")

    def test_proxy_connection_error_paths(self) -> None:
        proxy_opts = PoolOptions(
            connect_timeout=1, proxy_options=_ProxyOptions("proxy.invalid", 1080)
        )
        target = (
            "pymongo.pool_shared.socket.getaddrinfo"
            if _IS_SYNC
            else "pymongo.pool_shared._getaddrinfo"
        )

        # 1. getaddrinfo raises OSError
        def raise_gai(*_a: Any, **_kw: Any) -> Any:
            raise socket.gaierror("resolution failed")

        with (
            mock.patch(target, side_effect=raise_gai),
            self.assertRaisesRegex(_Socks5Error, "proxy connection failed"),
        ):
            _create_connection(("localhost", 27017), proxy_opts)

        # 2. getaddrinfo returns empty list
        def empty_async(*_a: Any, **_kw: Any) -> Any:
            return []

        replacement_empty: Any = (lambda *_a, **_kw: []) if _IS_SYNC else empty_async
        with (
            mock.patch(target, new=replacement_empty),
            self.assertRaisesRegex(_Socks5Error, "returned no addresses"),
        ):
            _create_connection(("localhost", 27017), proxy_opts)

        # 3. All proxy addresses refuse connection
        dead_sock = socket.socket()
        dead_sock.bind(("127.0.0.1", 0))
        dead_addr = dead_sock.getsockname()
        dead_sock.close()
        dead_results = [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", dead_addr)]

        def dead_async(*_a: Any, **_kw: Any) -> Any:
            return dead_results

        replacement_dead: Any = (lambda *_a, **_kw: dead_results) if _IS_SYNC else dead_async
        with (
            mock.patch(target, new=replacement_dead),
            self.assertRaisesRegex(_Socks5Error, "proxy connection failed"),
        ):
            _create_connection(("localhost", 27017), proxy_opts)

    def test_in_process_socks5_server_forwarding(self) -> None:
        from test.test_socks5_prose import Socks5Server

        echo_listener = socket.socket()
        echo_listener.bind(("127.0.0.1", 0))
        echo_listener.listen(2)
        echo_addr = echo_listener.getsockname()

        def serve_echo() -> None:
            for _ in range(2):
                try:
                    conn, _ = echo_listener.accept()
                except OSError:
                    return
                with conn:
                    data = conn.recv(64)
                    if data:
                        conn.sendall(b"ack:" + data)

        echo_thread = threading.Thread(target=serve_echo, daemon=True)
        echo_thread.start()
        self.addCleanup(echo_listener.close)

        mapping = {("mapped.example", 12345): echo_addr}
        auth_proxy = Socks5Server(0, mapping, ("user", "secret"))
        noauth_proxy = Socks5Server(0, mapping)
        self.addCleanup(auth_proxy.close)
        self.addCleanup(noauth_proxy.close)

        # Test authenticated domain mapping + bidirectional forwarding
        sock1 = _create_connection(
            ("mapped.example", 12345),
            PoolOptions(
                connect_timeout=2,
                proxy_options=_ProxyOptions("127.0.0.1", auth_proxy.port, "user", "secret"),
            ),
        )
        try:
            sock1.settimeout(2)
            sock1.sendall(b"hello")
            self.assertEqual(sock1.recv(64), b"ack:hello")
        finally:
            sock1.close()

        # Test unauthenticated IPv4 connect + bidirectional forwarding
        sock2 = _create_connection(
            echo_addr,
            PoolOptions(
                connect_timeout=2,
                proxy_options=_ProxyOptions("127.0.0.1", noauth_proxy.port),
            ),
        )
        try:
            sock2.settimeout(2)
            sock2.sendall(b"world")
            self.assertEqual(sock2.recv(64), b"ack:world")
        finally:
            sock2.close()

        # Test bad auth and unreachable target against Socks5Server
        with self.assertRaises(_Socks5Error):
            _create_connection(
                ("mapped.example", 12345),
                PoolOptions(
                    connect_timeout=2,
                    proxy_options=_ProxyOptions("127.0.0.1", auth_proxy.port, "user", "wrong"),
                ),
            )
        with self.assertRaises(_Socks5Error):
            _create_connection(
                ("127.0.0.1", 1),
                PoolOptions(
                    connect_timeout=2,
                    proxy_options=_ProxyOptions("127.0.0.1", noauth_proxy.port),
                ),
            )
        echo_thread.join(timeout=2)
