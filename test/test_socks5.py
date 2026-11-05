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
import os
import socket
import ssl
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Optional, cast
from unittest import mock
from urllib.parse import quote_plus

import pytest

sys.path[0:0] = [""]

from pymongo import monitoring
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT
from pymongo.hello import HelloCompat
from pymongo.pool_options import PoolOptions, _ProxyOptions
from pymongo.pool_shared import _configured_socket, _create_connection
from pymongo.socks5_shared import _Socks5Error
from test import (
    IntegrationTest,
    PyMongoTestCase,
    client_context,
    unittest,
)
from test.helpers_shared import CERT_PATH, TLS_OPTIONS
from test.socks5_helpers import _Socks5Server
from test.utils import wait_until

_IS_SYNC = True
pytestmark = pytest.mark.default


@contextmanager
def _refuse_connection(address: tuple[str, int]) -> Iterator[None]:
    if _IS_SYNC:
        original_connect = socket.socket.connect

        def connect(sock: socket.socket, target: tuple[str, int]) -> None:
            if target == address:
                raise ConnectionRefusedError("connection refused")
            original_connect(sock, target)

        with mock.patch.object(socket.socket, "connect", new=connect):
            yield
    else:
        loop = asyncio.get_running_loop()
        original_sock_connect = loop.sock_connect

        def sock_connect(sock: socket.socket, target: tuple[str, int]) -> None:
            if target == address:
                raise ConnectionRefusedError("connection refused")
            original_sock_connect(sock, target)

        with mock.patch.object(loop, "sock_connect", new=sock_connect):
            yield


class TestSocks5(PyMongoTestCase):
    def _create_server(self, **kwargs: Any) -> _Socks5Server:
        server = _Socks5Server(**kwargs)
        self.addCleanup(server.close)
        return server

    def _connect(
        self,
        server: _Socks5Server,
        address: tuple[str, int],
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 2,
    ) -> socket.socket:
        sock = _create_connection(
            address,
            PoolOptions(
                connect_timeout=timeout,
                proxy_options=_ProxyOptions(
                    server.address[0], server.address[1], username, password
                ),
            ),
        )
        self.addCleanup(sock.close)
        return sock

    def test_greeting_and_authenticated_connect(self) -> None:
        server = self._create_server(method=2)
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
        server = self._create_server(method=0)
        sock = self._connect(server, ("127.0.0.1", 27017), "user", "password")
        sock.close()
        server.close()
        self.assertEqual(server.greeting, b"\x05\x02\x00\x02")
        self.assertEqual(server.auth, b"")
        self.assertEqual(server.request, b"\x05\x01\x00\x01\x7f\x00\x00\x01\x69\x89")

    def test_no_credentials_only_offer_no_authentication(self) -> None:
        server = self._create_server(method=2)
        with self.assertRaises(_Socks5Error):
            self._connect(server, ("localhost", 27017))
        server.close()
        self.assertEqual(server.greeting, b"\x05\x01\x00")

    def test_rejects_all_unacceptable_methods(self) -> None:
        for method in (0x01, 0xFF):
            server = self._create_server(method=method)
            with self.subTest(method=method), self.assertRaises(_Socks5Error):
                self._connect(server, ("localhost", 27017))
            server.close()

    def test_rejects_authentication_failure(self) -> None:
        server = self._create_server(method=2, auth_status=1)
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
            server = self._create_server()
            sock = self._connect(server, (host, 27017))
            sock.close()
            server.close()
            self.assertEqual(server.request, b"\x05\x01\x00" + expected + b"\x69\x89")

    def test_drains_bound_address_and_preserves_early_payload(self) -> None:
        for atyp in (1, 3, 4):
            server = self._create_server(bound_atyp=atyp, payload=b"mongo")
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
            server = self._create_server(reply=reply)
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
            "mongodb://localhost/?proxyHost=localhost&proxyPort=1234"
            "&proxyUsername=uri-user&proxyPassword=uri-password",
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

    def test_client_equality_includes_proxy_options(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient

        clients = [
            MongoClient("mongodb://localhost/?proxyHost=proxy.example", connect=False),
            MongoClient("mongodb://localhost/?proxyHost=proxy.example", connect=False),
            MongoClient("mongodb://localhost/?proxyHost=other.example", connect=False),
        ]
        try:
            self.assertEqual(clients[0], clients[1])
            self.assertNotEqual(clients[0], clients[2])
        finally:
            for client in clients:
                client.close()

    def test_bad_versions_and_unexpected_eof_fail(self) -> None:
        cases = (
            (self._create_server(method_version=4), ("localhost", 27017), None),
            (self._create_server(reply_version=4), ("localhost", 27017), None),
            (self._create_server(eof_stage="greeting"), ("localhost", 27017), None),
            (
                self._create_server(method=2, eof_stage="auth"),
                ("localhost", 27017),
                ("user", "password"),
            ),
            (self._create_server(eof_stage="connect"), ("localhost", 27017), None),
        )
        for server, address, credentials in cases:
            with self.subTest(stage=server.eof_stage, version=server.method_version):
                with self.assertRaises(_Socks5Error):
                    self._connect(server, address, *(credentials or (None, None)))
            server.close()

    def test_timeout_covers_the_entire_handshake(self) -> None:
        server = self._create_server(stall=True)
        with self.assertRaises(socket.timeout):
            self._connect(server, ("localhost", 27017), timeout=0.1)
        server.close()

    @unittest.skipIf(_IS_SYNC, "Async cancellation only applies to async connections")
    def test_cancellation_closes_proxy_socket(self) -> None:
        server = self._create_server(stall=True)
        task = asyncio.create_task(cast(Any, self._connect(server, ("localhost", 27017))))
        wait_until(server.started.is_set, "receive the SOCKS5 greeting", timeout=2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            task

    @unittest.skipIf(_IS_SYNC, "Async cancellation only applies to async connections")
    def test_cancellation_when_proxy_send_completes_closes_socket(self) -> None:
        server = self._create_server(stall=True)
        loop = asyncio.get_running_loop()
        original_sendall = loop.sock_sendall
        sockets: list[socket.socket] = []

        def cancel_after_send(sock: socket.socket, data: bytes) -> None:
            sockets.append(sock)
            original_sendall(sock, data)
            task.cancel()

        with mock.patch.object(loop, "sock_sendall", new=cancel_after_send):
            task = asyncio.create_task(cast(Any, self._connect(server, ("localhost", 27017))))
            with self.assertRaises(asyncio.CancelledError):
                task
        self.assertEqual(len(sockets), 1)
        self.assertEqual(sockets[0].fileno(), -1)

    @client_context.require_async  # type: ignore[untyped-decorator]
    def test_tls_sni_targets_destination_not_proxy(self) -> None:
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.load_cert_chain(f"{CERT_PATH}/server.pem")
        sni_names: list[Optional[str]] = []
        tls_context.set_servername_callback(lambda _sock, name, _ctx: sni_names.append(name))

        def serve_tls(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                reader.read(1)
            finally:
                writer.close()
                writer.wait_closed()

        tls_server: Any = asyncio.start_server(serve_tls, "127.0.0.1", 0, ssl=tls_context)

        def close_tls_server() -> None:
            tls_server.close()
            tls_server.wait_closed()

        self.addCleanup(close_tls_server)
        tls_sockets = tls_server.sockets
        assert tls_sockets is not None
        target_address = tls_sockets[0].getsockname()
        proxy = self._create_server(target=target_address)
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
        self.addCleanup(sock.close)
        sock.close()
        self.assertEqual(sni_names, ["mongodb.example"])

    @client_context.require_sync  # type: ignore[untyped-decorator]
    def test_tls_sni_targets_destination_not_proxy_sync(self) -> None:
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.load_cert_chain(f"{CERT_PATH}/server.pem")
        sni_names: list[Optional[str]] = []
        tls_context.set_servername_callback(lambda _sock, name, _ctx: sni_names.append(name))
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        target_address = listener.getsockname()

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

        def close_tls_server() -> None:
            listener.close()
            tls_thread.join(timeout=3)

        self.addCleanup(close_tls_server)
        proxy = self._create_server(target=target_address)
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
        self.addCleanup(sock.close)
        sock.close()
        self.assertEqual(sni_names, ["mongodb.example"])

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_kms_request_uses_proxy_options(self) -> None:
        import pymongo.synchronous.encryption as encryption_module
        from pymongo.synchronous.encryption import _EncryptionIO
        from pymongo.synchronous.mongo_client import MongoClient

        class FakeOpts:
            _kms_connect_callback = None

            @staticmethod
            def _kms_ssl_contexts(_is_sync: bool) -> dict[str, object]:
                return {"aws": object()}

        client = MongoClient("mongodb://localhost/", connect=False)
        try:
            for expected in (None, _ProxyOptions("127.0.0.1")):
                with self.subTest(proxy_options=expected):
                    io = _EncryptionIO(
                        client,
                        client.keyvault.datakeys,
                        None,
                        FakeOpts(),  # type: ignore[arg-type]
                        proxy_options=expected,
                    )
                    connection = mock.MagicMock()

                    def connect(_address, options, _callback, _timeout):
                        self.assertIs(options._proxy_options, expected)
                        return connection

                    def sendall(_connection, _message):
                        pass

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
                    connection.close.assert_called_once_with()
        finally:
            client.close()

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
        from pymongo.pool_shared import _raise_connection_failure
        from pymongo.socks5_shared import _Socks5Timeout
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
        server = self._create_server()
        dead_addr = ("127.0.0.1", 1)

        fake_results = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", dead_addr),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", server.address),
        ]
        target = (
            "pymongo.socks5_shared.socket.getaddrinfo"
            if _IS_SYNC
            else "pymongo.socks5_shared._getaddrinfo"
        )

        def async_fake_getaddrinfo(*_args: Any, **_kwargs: Any) -> Any:
            return fake_results

        replacement: Any = (lambda *_a, **_kw: fake_results) if _IS_SYNC else async_fake_getaddrinfo
        with mock.patch(target, new=replacement), _refuse_connection(dead_addr):
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
                proxy_options=parent_client.options.pool_options._proxy_options,
            )
            self.assertEqual(io_bypass._proxy_options, _ProxyOptions("10.0.0.1"))

            io_explicit = _EncryptionIO(
                None,
                kv_client.keyvault.datakeys,
                None,
                FakeOpts(),  # type: ignore[arg-type]
                proxy_options=kv_client.options.pool_options._proxy_options,
            )
            self.assertEqual(io_explicit._proxy_options, _ProxyOptions("10.0.0.2"))
        finally:
            parent_client.close()
            kv_client.close()

    def test_proxy_host_validation(self) -> None:
        from pymongo.common import validate_proxy_host

        self.assertIsNone(validate_proxy_host("proxyHost", None))
        self.assertEqual(validate_proxy_host("proxyHost", "::1"), "::1")
        with self.assertRaises(ValueError):
            validate_proxy_host("proxyHost", "[not-ipv6]")
        with self.assertRaises(ValueError):
            validate_proxy_host("proxyHost", "bad/host")

    def test_proxy_port_validation(self) -> None:
        from pymongo.common import validate_proxy_port

        self.assertIsNone(validate_proxy_port("proxyPort", None))
        with self.assertRaises(TypeError):
            validate_proxy_port("proxyPort", True)

    def test_proxy_deadline(self) -> None:
        from pymongo.socks5_shared import _proxy_deadline, _proxy_remaining, _Socks5Timeout

        self.assertIsNone(_proxy_deadline(PoolOptions(connect_timeout=None)))
        self.assertIsNone(_proxy_remaining(None))
        with self.assertRaises(_Socks5Timeout):
            _proxy_remaining(0.0)
        with (
            mock.patch("pymongo.socks5_shared._csot.remaining", return_value=0.0),
            self.assertRaises(_Socks5Timeout),
        ):
            _proxy_deadline(PoolOptions(connect_timeout=1))

    def test_proxy_socket_timeout_rejects_expired_deadline(self) -> None:
        from pymongo.socks5_shared import _proxy_socket_timeout, _Socks5Timeout

        with (
            mock.patch("pymongo.socks5_shared._csot.get_timeout", return_value=True),
            mock.patch("pymongo.socks5_shared._csot.remaining", return_value=-0.1),
            self.assertRaises(_Socks5Timeout),
        ):
            _proxy_socket_timeout(PoolOptions(connect_timeout=1))

    def test_socks5_request_rejects_long_domain(self) -> None:
        from pymongo.socks5_shared import _socks5_request

        with self.assertRaisesRegex(_Socks5Error, "invalid domain name length"):
            _socks5_request(("a" * 256, 27017))

    def test_socks5_request_rejects_invalid_domain(self) -> None:
        from pymongo.socks5_shared import _socks5_request

        with self.assertRaisesRegex(_Socks5Error, "invalid domain name"):
            _socks5_request((".münich.example", 27017))

    def test_socks5_request_checks_port_before_encoding_domain(self) -> None:
        from pymongo.socks5_shared import _socks5_request

        with self.assertRaisesRegex(_Socks5Error, "destination port is required"):
            _socks5_request(("a" * 256, None))  # type: ignore[arg-type]

    def test_socks5_connect_reply_rejects_invalid_reserved_byte(self) -> None:
        from pymongo.socks5_shared import _socks5_check_connect_reply

        with self.assertRaisesRegex(_Socks5Error, "invalid reserved byte"):
            _socks5_check_connect_reply(b"\x05\x00\x01\x01")

    def test_socks5_connect_reply_rejects_invalid_address_type(self) -> None:
        from pymongo.socks5_shared import _socks5_check_connect_reply

        with self.assertRaisesRegex(_Socks5Error, "invalid address type"):
            _socks5_check_connect_reply(b"\x05\x00\x00\x09")

    def test_proxy_connection_error_paths(self) -> None:
        proxy_opts = PoolOptions(
            connect_timeout=1, proxy_options=_ProxyOptions("proxy.invalid", 1080)
        )
        target = (
            "pymongo.socks5_shared.socket.getaddrinfo"
            if _IS_SYNC
            else "pymongo.socks5_shared._getaddrinfo"
        )

        def raise_gai(*_a: Any, **_kw: Any) -> Any:
            raise socket.gaierror("resolution failed")

        with (
            mock.patch(target, side_effect=raise_gai),
            self.assertRaisesRegex(_Socks5Error, "proxy connection failed"),
        ):
            _create_connection(("localhost", 27017), proxy_opts)

        def empty_async(*_a: Any, **_kw: Any) -> Any:
            return []

        replacement_empty: Any = (lambda *_a, **_kw: []) if _IS_SYNC else empty_async
        with (
            mock.patch(target, new=replacement_empty),
            self.assertRaisesRegex(_Socks5Error, "returned no addresses"),
        ):
            _create_connection(("localhost", 27017), proxy_opts)

        dead_addr = ("127.0.0.1", 1)
        dead_results = [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", dead_addr)]

        def dead_async(*_a: Any, **_kw: Any) -> Any:
            return dead_results

        replacement_dead: Any = (lambda *_a, **_kw: dead_results) if _IS_SYNC else dead_async
        with (
            mock.patch(target, new=replacement_dead),
            _refuse_connection(dead_addr),
            self.assertRaisesRegex(_Socks5Error, "proxy connection failed"),
        ):
            _create_connection(("localhost", 27017), proxy_opts)


class _CommandListener(monitoring.CommandListener):
    def __init__(self) -> None:
        self.connection_ids: list[tuple[str, Optional[int]]] = []

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        self.connection_ids.append(event.connection_id)

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        self.connection_ids.append(event.connection_id)

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        self.connection_ids.append(event.connection_id)


class TestSocks5Prose(IntegrationTest):
    @client_context.require_replica_set
    def setUp(self) -> None:
        if not os.environ.get("SOCKS5_PROXIES_STARTED"):
            self.skipTest("SOCKS5 proxies must be started by the Evergreen SOCKS5 tasks.")
        super().setUp()
        self.nodes = sorted(client_context.nodes)
        self.credentials = getattr(self, "credentials", {})
        self.auth_port = 1080
        self.noauth_port = 1081

    def _hosts(self, mapped: bool) -> str:
        if mapped:
            return "localhost:12345"
        return ",".join(
            f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"
            for host, port in self.nodes
        )

    def _run_case(
        self,
        hosts: str,
        port: int,
        credentials: Optional[tuple[str, str]],
        succeeds: bool,
        use_uri: bool,
        listener: Optional[_CommandListener] = None,
    ) -> None:
        from pymongo.synchronous.mongo_client import MongoClient

        proxy_options = {"proxyHost": "localhost", "proxyPort": port}
        if credentials:
            proxy_options["proxyUsername"], proxy_options["proxyPassword"] = credentials
        tls_options = dict(TLS_OPTIONS) if client_context.tls else {}
        options = dict(tls_options)
        options.update(self.credentials)
        options["serverSelectionTimeoutMS"] = 2500
        if hosts == "localhost:12345":
            options["directConnection"] = True
        if listener is not None:
            options["event_listeners"] = [listener]
        if use_uri:
            uri_options = "&".join(
                f"{key}={quote_plus(str(value))}" for key, value in proxy_options.items()
            )
            hosts = f"mongodb://{hosts}/?{uri_options}"
        else:
            options.update(proxy_options)
        client = MongoClient(hosts, **options)
        try:
            if succeeds:
                client.admin.command(HelloCompat.CMD)
                if listener is not None:
                    from pymongo.errors import OperationFailure

                    with self.assertRaises(OperationFailure):
                        client.admin.command("nonexistent_socks5_test_command")
            else:
                from pymongo.errors import ConnectionFailure

                with self.assertRaises(ConnectionFailure):
                    client.admin.command(HelloCompat.CMD)
        finally:
            client.close()

    def test_connection_string_prose_table(self) -> None:
        cases = (
            (True, self.auth_port, None, False),
            (True, self.noauth_port, None, True),
            (False, self.auth_port, None, False),
            (False, self.noauth_port, None, True),
            (True, self.auth_port, ("nonexistentuser", "badauth"), False),
            (True, self.noauth_port, ("nonexistentuser", "badauth"), True),
            (False, self.noauth_port, ("nonexistentuser", "badauth"), True),
            (True, self.auth_port, ("username", "p4ssw0rd"), True),
            (True, self.noauth_port, None, True),
            (False, self.auth_port, ("username", "p4ssw0rd"), True),
            (False, self.noauth_port, None, True),
        )
        mapped_listener = _CommandListener()
        rs_listener = _CommandListener()
        for index, (mapped, proxy_port, credentials, succeeds) in enumerate(cases):
            for use_uri in (True, False):
                listener = None
                if not use_uri:
                    if index == 1:
                        listener = mapped_listener
                    elif index == 3:
                        listener = rs_listener
                with self.subTest(
                    case=index,
                    mapped=mapped,
                    proxy_port=proxy_port,
                    credentials=credentials,
                    use_uri=use_uri,
                ):
                    self._run_case(
                        self._hosts(mapped),
                        proxy_port,
                        credentials,
                        succeeds,
                        use_uri,
                        listener,
                    )
        proxy_addresses = {
            ("localhost", self.auth_port),
            ("localhost", self.noauth_port),
            ("127.0.0.1", self.auth_port),
            ("127.0.0.1", self.noauth_port),
        }
        self.assertGreaterEqual(len(mapped_listener.connection_ids), 4)
        for connection_id in mapped_listener.connection_ids:
            self.assertNotIn(connection_id, proxy_addresses)
            self.assertEqual(connection_id, ("localhost", 12345))
        self.assertGreaterEqual(len(rs_listener.connection_ids), 4)
        for connection_id in rs_listener.connection_ids:
            self.assertNotIn(connection_id, proxy_addresses)
            self.assertIn(connection_id, self.nodes)
