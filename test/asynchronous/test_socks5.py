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
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, NamedTuple, Optional, cast
from unittest import mock
from urllib.parse import quote_plus

import pytest

sys.path[0:0] = [""]

from pymongo import monitoring
from pymongo.hello import HelloCompat
from pymongo.pool_options import PoolOptions, _ProxyOptions
from pymongo.pool_shared import _async_configured_socket, _async_create_connection
from pymongo.socks5_shared import (
    _SOCKS5_ATYP_DOMAIN,
    _SOCKS5_ATYP_IPV4,
    _SOCKS5_ATYP_IPV6,
    _SOCKS5_AUTH_METHOD_COUNT_NO_CREDENTIALS,
    _SOCKS5_AUTH_METHOD_COUNT_WITH_CREDENTIALS,
    _SOCKS5_AUTH_NO_ACCEPTABLE,
    _SOCKS5_AUTH_NO_AUTH,
    _SOCKS5_AUTH_USER_PASS,
    _SOCKS5_AUTH_VERSION,
    _SOCKS5_CMD_CONNECT,
    _SOCKS5_RESERVED,
    _SOCKS5_VERSION,
    _Socks5Error,
    _Socks5Timeout,
)
from test.asynchronous import (
    AsyncIntegrationTest,
    AsyncPyMongoTestCase,
    async_client_context,
    unittest,
)
from test.asynchronous.utils import async_wait_until
from test.helpers_shared import TLS_OPTIONS
from test.socks5_helpers import _Socks5Server, _TLSServer

_IS_SYNC = False
pytestmark = pytest.mark.default_async

_SOCKS5_AUTH_GSSAPI_METHOD = 0x01
_SOCKS5_AUTH_FAILURE_STATUS = 0x01
_SOCKS4_VERSION = 0x04


class _ProseCase(NamedTuple):
    mapped: bool
    proxy_port: int
    credentials: Optional[tuple[str, str]]
    succeeds: bool


@contextmanager
def _fail_connection(
    address: tuple[str, int], error_type: type[OSError] = ConnectionRefusedError
) -> Iterator[None]:
    if _IS_SYNC:
        original_connect = socket.socket.connect

        def connect(sock: socket.socket, target: tuple[str, int]) -> None:
            if target == address:
                raise error_type("connection failed")
            original_connect(sock, target)

        with mock.patch.object(socket.socket, "connect", new=connect):
            yield
    else:
        loop = asyncio.get_running_loop()
        original_sock_connect = loop.sock_connect

        async def sock_connect(sock: socket.socket, target: tuple[str, int]) -> None:
            if target == address:
                raise error_type("connection failed")
            await original_sock_connect(sock, target)

        with mock.patch.object(loop, "sock_connect", new=sock_connect):
            yield


class TestSocks5(AsyncPyMongoTestCase):
    def _create_server(self, **kwargs: Any) -> _Socks5Server:
        server = _Socks5Server(**kwargs)
        self.addAsyncCleanup(server.close)
        return server

    async def _connect(
        self,
        server: _Socks5Server,
        address: tuple[str, int],
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 2,
    ) -> socket.socket:
        sock = await _async_create_connection(
            address,
            PoolOptions(
                connect_timeout=timeout,
                proxy_options=_ProxyOptions(
                    server.address[0], server.address[1], username, password
                ),
            ),
        )
        self.addAsyncCleanup(sock.close)
        return sock

    async def test_greeting_and_authenticated_connect(self) -> None:
        server = self._create_server(method=_SOCKS5_AUTH_USER_PASS)
        sock = await self._connect(server, ("mongodb.example", 27017), "üser", "påss")
        sock.close()
        server.close()
        self.assertEqual(
            server.greeting,
            bytes(
                (
                    _SOCKS5_VERSION,
                    _SOCKS5_AUTH_METHOD_COUNT_WITH_CREDENTIALS,
                    _SOCKS5_AUTH_NO_AUTH,
                    _SOCKS5_AUTH_USER_PASS,
                )
            ),
        )
        self.assertEqual(
            server.auth,
            bytes((_SOCKS5_AUTH_VERSION, len("üser".encode())))
            + "üser".encode()
            + bytes((len("påss".encode()),))
            + "påss".encode(),
        )
        self.assertEqual(
            server.request[:4],
            bytes((_SOCKS5_VERSION, _SOCKS5_CMD_CONNECT, _SOCKS5_RESERVED, _SOCKS5_ATYP_DOMAIN)),
        )
        self.assertEqual(
            server.request[4:],
            bytes((len(b"mongodb.example"),)) + b"mongodb.example" + (27017).to_bytes(2, "big"),
        )

    async def test_no_auth_method_is_preferred_when_credentials_exist(self) -> None:
        server = self._create_server(method=_SOCKS5_AUTH_NO_AUTH)
        sock = await self._connect(server, ("127.0.0.1", 27017), "user", "password")
        sock.close()
        server.close()
        self.assertEqual(
            server.greeting,
            bytes(
                (
                    _SOCKS5_VERSION,
                    _SOCKS5_AUTH_METHOD_COUNT_WITH_CREDENTIALS,
                    _SOCKS5_AUTH_NO_AUTH,
                    _SOCKS5_AUTH_USER_PASS,
                )
            ),
        )
        self.assertEqual(server.auth, b"")
        self.assertEqual(
            server.request,
            bytes((_SOCKS5_VERSION, _SOCKS5_CMD_CONNECT, _SOCKS5_RESERVED, _SOCKS5_ATYP_IPV4))
            + socket.inet_aton("127.0.0.1")
            + (27017).to_bytes(2, "big"),
        )

    async def test_no_credentials_only_offer_no_authentication(self) -> None:
        server = self._create_server(method=_SOCKS5_AUTH_USER_PASS)
        with self.assertRaises(_Socks5Error):
            await self._connect(server, ("localhost", 27017))
        server.close()
        self.assertEqual(
            server.greeting,
            bytes(
                (_SOCKS5_VERSION, _SOCKS5_AUTH_METHOD_COUNT_NO_CREDENTIALS, _SOCKS5_AUTH_NO_AUTH)
            ),
        )

    async def test_rejects_all_unacceptable_methods(self) -> None:
        for method in (_SOCKS5_AUTH_GSSAPI_METHOD, _SOCKS5_AUTH_NO_ACCEPTABLE):
            server = self._create_server(method=method)
            with self.subTest(method=method), self.assertRaises(_Socks5Error):
                await self._connect(server, ("localhost", 27017))
            server.close()

    async def test_rejects_authentication_failure(self) -> None:
        server = self._create_server(
            method=_SOCKS5_AUTH_USER_PASS, auth_status=_SOCKS5_AUTH_FAILURE_STATUS
        )
        with self.assertRaises(_Socks5Error):
            await self._connect(server, ("localhost", 27017), "user", "password")
        server.close()
        self.assertEqual(
            server.auth,
            bytes((_SOCKS5_AUTH_VERSION, len(b"user")))
            + b"user"
            + bytes((len(b"password"),))
            + b"password",
        )

    async def test_connect_request_address_types(self) -> None:
        cases = (
            ("192.0.2.1", bytes((_SOCKS5_ATYP_IPV4,)) + socket.inet_aton("192.0.2.1")),
            (
                "2001:db8::1",
                bytes((_SOCKS5_ATYP_IPV6,)) + socket.inet_pton(socket.AF_INET6, "2001:db8::1"),
            ),
            (
                "münich.example",
                bytes((_SOCKS5_ATYP_DOMAIN, len("xn--mnich-kva.example")))
                + "xn--mnich-kva.example".encode("ascii"),
            ),
        )
        for host, expected in cases:
            server = self._create_server()
            sock = await self._connect(server, (host, 27017))
            sock.close()
            server.close()
            self.assertEqual(
                server.request,
                bytes((_SOCKS5_VERSION, _SOCKS5_CMD_CONNECT, _SOCKS5_RESERVED))
                + expected
                + (27017).to_bytes(2, "big"),
            )

    async def test_drains_bound_address_and_preserves_early_payload(self) -> None:
        for atyp in (_SOCKS5_ATYP_IPV4, _SOCKS5_ATYP_DOMAIN, _SOCKS5_ATYP_IPV6):
            server = self._create_server(bound_atyp=atyp, payload=b"mongo")
            sock = await self._connect(server, ("localhost", 27017))
            if _IS_SYNC:
                payload = sock.recv(5)
            else:
                sock.setblocking(False)
                payload = await asyncio.get_running_loop().sock_recv(sock, 5)
            sock.close()
            server.close()
            self.assertEqual(payload, b"mongo")

    async def test_rejects_socks_reply_codes(self) -> None:
        for reply in range(1, 9):
            server = self._create_server(reply=reply)
            with self.subTest(reply=reply), self.assertRaises(_Socks5Error):
                await self._connect(server, ("localhost", 27017))
            server.close()

    async def test_proxy_options_reach_server_and_monitor_pools(self) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient
        from pymongo.asynchronous.topology import Topology

        client = AsyncMongoClient("mongodb://localhost/?proxyHost=127.0.0.1", connect=False)
        try:
            topology = Topology(client._topology_settings)
            server_pool = topology._create_pool_for_server(("localhost", 27017))
            monitor_pool = topology._create_pool_for_monitor(("localhost", 27017))
            expected = _ProxyOptions("127.0.0.1")
            self.assertEqual(server_pool.opts._proxy_options, expected)
            self.assertEqual(monitor_pool.opts._proxy_options, expected)
        finally:
            await client.close()

    async def test_kwargs_override_uri_proxy_options_and_empty_values_are_ignored(self) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient

        client = AsyncMongoClient(
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
            await client.close()

    async def test_client_equality_includes_proxy_options(self) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient

        clients = [
            AsyncMongoClient("mongodb://localhost/?proxyHost=proxy.example", connect=False),
            AsyncMongoClient("mongodb://localhost/?proxyHost=proxy.example", connect=False),
            AsyncMongoClient("mongodb://localhost/?proxyHost=other.example", connect=False),
        ]
        try:
            self.assertEqual(clients[0], clients[1])
            self.assertNotEqual(clients[0], clients[2])
        finally:
            for client in clients:
                await client.close()

    async def test_bad_versions_and_unexpected_eof_fail(self) -> None:
        cases = (
            (self._create_server(method_version=_SOCKS4_VERSION), ("localhost", 27017), None),
            (self._create_server(reply_version=_SOCKS4_VERSION), ("localhost", 27017), None),
            (self._create_server(eof_stage="greeting"), ("localhost", 27017), None),
            (
                self._create_server(method=_SOCKS5_AUTH_USER_PASS, eof_stage="auth"),
                ("localhost", 27017),
                ("user", "password"),
            ),
            (self._create_server(eof_stage="connect"), ("localhost", 27017), None),
        )
        for server, address, credentials in cases:
            with self.subTest(stage=server.eof_stage, version=server.method_version):
                with self.assertRaises(_Socks5Error):
                    await self._connect(server, address, *(credentials or (None, None)))
            server.close()

    async def test_timeout_covers_the_entire_handshake(self) -> None:
        server = self._create_server(stall=True)
        with self.assertRaisesRegex(socket.timeout, "SOCKS5 proxy timed out"):
            await self._connect(server, ("localhost", 27017), timeout=0.1)
        server.close()

    @unittest.skipIf(_IS_SYNC, "Async cancellation only applies to async connections")
    async def test_cancellation_closes_proxy_socket(self) -> None:
        server = self._create_server(stall=True)
        task = asyncio.create_task(cast(Any, self._connect(server, ("localhost", 27017))))
        await async_wait_until(server.started.is_set, "receive the SOCKS5 greeting", timeout=2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    @unittest.skipIf(_IS_SYNC, "Async cancellation only applies to async connections")
    async def test_cancellation_when_proxy_send_completes_closes_socket(self) -> None:
        server = self._create_server(stall=True)
        loop = asyncio.get_running_loop()
        original_sendall = loop.sock_sendall
        sockets: list[socket.socket] = []

        async def cancel_after_send(sock: socket.socket, data: bytes) -> None:
            sockets.append(sock)
            await original_sendall(sock, data)
            task.cancel()

        with mock.patch.object(loop, "sock_sendall", new=cancel_after_send):
            task = asyncio.create_task(cast(Any, self._connect(server, ("localhost", 27017))))
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(sockets), 1)
        self.assertEqual(sockets[0].fileno(), -1)

    async def test_tls_sni_targets_destination_not_proxy(self) -> None:
        sni_names: list[Optional[str]] = []
        tls_server = _TLSServer(sni_names)
        self.addAsyncCleanup(tls_server.close)
        proxy = self._create_server(target=tls_server.address)
        client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_context.check_hostname = False
        client_context.verify_mode = ssl.CERT_NONE
        sock = await _async_configured_socket(
            ("mongodb.example", tls_server.address[1]),
            PoolOptions(
                connect_timeout=2,
                ssl_context=cast(Any, client_context),
                proxy_options=_ProxyOptions(proxy.address[0], proxy.address[1]),
            ),
        )
        self.addAsyncCleanup(sock.close)
        sock.close()
        self.assertEqual(sni_names, ["mongodb.example"])

    async def test_proxy_password_is_redacted_in_repr(self) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient

        for client in (
            AsyncMongoClient(
                "mongodb://user:dbsecret@localhost/?proxyHost=127.0.0.1"
                "&proxyUsername=proxyuser&proxyPassword=hunter2",
                connect=False,
            ),
            AsyncMongoClient(
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
                await client.close()

    async def test_split_proxy_options_across_uri_and_kwargs(self) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient

        client1 = AsyncMongoClient(
            "mongodb://localhost/?proxyPort=1081",
            proxyHost="proxy.example",
            connect=False,
        )
        client2 = AsyncMongoClient(
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
            await client1.close()
            await client2.close()

    async def test_socks5_errors_are_not_labeled_system_overloaded(self) -> None:
        from pymongo.asynchronous.pool import Pool
        from pymongo.errors import AutoReconnect, NetworkTimeout
        from pymongo.pool_shared import _raise_connection_failure
        from pymongo.socks5_shared import _Socks5Timeout

        pool = Pool(("localhost", 27017), PoolOptions(connect_timeout=1))
        self.addAsyncCleanup(pool.close)
        for cause, overloaded in (
            (_Socks5Error("boom"), False),
            (_Socks5Timeout("SOCKS5 proxy timed out"), False),
            (OSError("boom"), True),
        ):
            with self.subTest(cause=type(cause).__name__):
                with self.assertRaises((AutoReconnect, NetworkTimeout)) as raised:
                    _raise_connection_failure(("localhost", 27017), cause)
                wrapped = raised.exception
                pool._handle_connection_error(wrapped)
                self.assertEqual(wrapped.has_error_label("SystemOverloadedError"), overloaded)
                self.assertEqual(wrapped.has_error_label("RetryableError"), overloaded)

    async def test_multi_address_proxy_fallback_on_connect_error(self) -> None:
        dead_addr = ("127.0.0.1", 1)
        target = (
            "pymongo.socks5_shared.socket.getaddrinfo"
            if _IS_SYNC
            else "pymongo.socks5_shared._getaddrinfo"
        )
        for error_type in (ConnectionRefusedError, socket.timeout):
            server = self._create_server()
            fake_results = [
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", dead_addr),
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", server.address),
            ]

            async def async_fake_getaddrinfo(*_args: Any, **_kwargs: Any) -> Any:
                return fake_results

            replacement: Any = (
                (lambda *_a, **_kw: fake_results) if _IS_SYNC else async_fake_getaddrinfo
            )
            with (
                self.subTest(error_type=error_type.__name__),
                mock.patch(target, new=replacement),
                _fail_connection(dead_addr, error_type),
            ):
                sock = await self._connect(server, ("localhost", 27017))
                sock.close()

    async def test_proxy_connect_timeout_does_not_retry_after_deadline(self) -> None:
        import pymongo.asynchronous.socks5_support as socks5_module
        from pymongo.socks5_shared import _Socks5Timeout

        addresses = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port))
            for port in (1, 2)
        ]
        with (
            mock.patch.object(socks5_module, "_proxy_deadline", return_value=0),
            mock.patch.object(socks5_module, "_async_proxy_getaddrinfo", return_value=addresses),
            mock.patch.object(
                socks5_module, "_async_proxy_connect", side_effect=socket.timeout("timed out")
            ) as connect,
            self.assertRaisesRegex(_Socks5Timeout, "SOCKS5 proxy timed out") as raised,
        ):
            await _async_create_connection(
                ("mongodb.example", 27017),
                PoolOptions(proxy_options=_ProxyOptions("proxy.example")),
            )
        connect.assert_called_once()
        self.assertEqual(connect.call_args.args[0].fileno(), -1)
        self.assertIsInstance(raised.exception.__cause__, socket.timeout)

    async def test_proxy_connection_error_paths(self) -> None:
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
            await _async_create_connection(("localhost", 27017), proxy_opts)

        with (
            mock.patch(target, side_effect=socket.timeout("timed out")),
            self.assertRaisesRegex(_Socks5Timeout, "SOCKS5 proxy timed out"),
        ):
            await _async_create_connection(("localhost", 27017), proxy_opts)

        async def empty_async(*_a: Any, **_kw: Any) -> Any:
            return []

        replacement_empty: Any = (lambda *_a, **_kw: []) if _IS_SYNC else empty_async
        with (
            mock.patch(target, new=replacement_empty),
            self.assertRaisesRegex(_Socks5Error, "returned no addresses"),
        ):
            await _async_create_connection(("localhost", 27017), proxy_opts)

        dead_addr = ("127.0.0.1", 1)
        dead_results = [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", dead_addr)]

        async def dead_async(*_a: Any, **_kw: Any) -> Any:
            return dead_results

        replacement_dead: Any = (lambda *_a, **_kw: dead_results) if _IS_SYNC else dead_async
        with (
            mock.patch(target, new=replacement_dead),
            _fail_connection(dead_addr),
            self.assertRaisesRegex(_Socks5Error, "proxy connection failed"),
        ):
            await _async_create_connection(("localhost", 27017), proxy_opts)

        with (
            mock.patch(target, new=replacement_dead),
            _fail_connection(dead_addr, socket.timeout),
            self.assertRaisesRegex(_Socks5Timeout, "SOCKS5 proxy timed out"),
        ):
            await _async_create_connection(("localhost", 27017), proxy_opts)

        server = self._create_server()
        handshake_options = PoolOptions(
            connect_timeout=1,
            proxy_options=_ProxyOptions(server.address[0], server.address[1]),
        )
        sendall_target = (
            "pymongo.synchronous.socks5_support._socks5_sendall"
            if _IS_SYNC
            else "pymongo.asynchronous.socks5_support._async_socks5_sendall"
        )
        with (
            mock.patch(sendall_target, side_effect=OSError("handshake failed")),
            self.assertRaisesRegex(_Socks5Error, "SOCKS5 handshake failed"),
        ):
            await _async_create_connection(("localhost", 27017), handshake_options)
        server.close()

        server = self._create_server()
        handshake_options = PoolOptions(
            connect_timeout=1,
            proxy_options=_ProxyOptions(server.address[0], server.address[1]),
        )
        with (
            mock.patch(sendall_target, side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            await _async_create_connection(("localhost", 27017), handshake_options)
        server.close()


class _CommandListener(monitoring.CommandListener):
    def __init__(self) -> None:
        self.connection_ids: list[tuple[str, Optional[int]]] = []

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        self.connection_ids.append(event.connection_id)

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        self.connection_ids.append(event.connection_id)

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        self.connection_ids.append(event.connection_id)


class TestSocks5Prose(AsyncIntegrationTest):
    @async_client_context.require_replica_set
    async def asyncSetUp(self) -> None:
        if not os.environ.get("SOCKS5_PROXIES_STARTED"):
            self.skipTest("SOCKS5 proxies must be started by the Evergreen SOCKS5 tasks.")
        await super().asyncSetUp()
        self.nodes = sorted(async_client_context.nodes)
        self.auth_port = 1080
        self.noauth_port = 1081

    def _hosts(self, mapped: bool) -> str:
        if mapped:
            return "localhost:12345"
        return ",".join(
            f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"
            for host, port in self.nodes
        )

    async def _run_case(
        self,
        hosts: str,
        port: int,
        credentials: Optional[tuple[str, str]],
        succeeds: bool,
        use_uri: bool,
        listener: Optional[_CommandListener] = None,
    ) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient

        proxy_options = {"proxyHost": "localhost", "proxyPort": port}
        if credentials:
            proxy_options["proxyUsername"], proxy_options["proxyPassword"] = credentials
        tls_options = dict(TLS_OPTIONS) if async_client_context.tls else {}
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
        client = AsyncMongoClient(hosts, **options)
        try:
            if succeeds:
                await client.admin.command(HelloCompat.CMD)
                if listener is not None:
                    from pymongo.errors import OperationFailure

                    with self.assertRaises(OperationFailure):
                        await client.admin.command("nonexistent_socks5_test_command")
            else:
                from pymongo.errors import ConnectionFailure

                with self.assertRaises(ConnectionFailure):
                    await client.admin.command(HelloCompat.CMD)
        finally:
            await client.close()

    async def test_connection_string_prose_table(self) -> None:
        cases = (
            _ProseCase(mapped=True, proxy_port=self.auth_port, credentials=None, succeeds=False),
            _ProseCase(mapped=True, proxy_port=self.noauth_port, credentials=None, succeeds=True),
            _ProseCase(mapped=False, proxy_port=self.auth_port, credentials=None, succeeds=False),
            _ProseCase(mapped=False, proxy_port=self.noauth_port, credentials=None, succeeds=True),
            _ProseCase(
                mapped=True,
                proxy_port=self.auth_port,
                credentials=("nonexistentuser", "badauth"),
                succeeds=False,
            ),
            _ProseCase(
                mapped=True,
                proxy_port=self.noauth_port,
                credentials=("nonexistentuser", "badauth"),
                succeeds=True,
            ),
            _ProseCase(
                mapped=False,
                proxy_port=self.noauth_port,
                credentials=("nonexistentuser", "badauth"),
                succeeds=True,
            ),
            _ProseCase(
                mapped=True,
                proxy_port=self.auth_port,
                credentials=("username", "p4ssw0rd"),
                succeeds=True,
            ),
            _ProseCase(
                mapped=False,
                proxy_port=self.auth_port,
                credentials=("username", "p4ssw0rd"),
                succeeds=True,
            ),
        )
        for case in cases:
            for use_uri in (True, False):
                with self.subTest(
                    mapped=case.mapped,
                    proxy_port=case.proxy_port,
                    credentials=case.credentials,
                    use_uri=use_uri,
                ):
                    await self._run_case(
                        hosts=self._hosts(case.mapped),
                        port=case.proxy_port,
                        credentials=case.credentials,
                        succeeds=case.succeeds,
                        use_uri=use_uri,
                    )

    async def test_command_monitoring_does_not_reference_proxy(self) -> None:
        proxy_addresses = {
            ("localhost", self.auth_port),
            ("localhost", self.noauth_port),
            ("127.0.0.1", self.auth_port),
            ("127.0.0.1", self.noauth_port),
        }
        for mapped in (True, False):
            listener = _CommandListener()
            with self.subTest(mapped=mapped):
                await self._run_case(
                    self._hosts(mapped), self.noauth_port, None, True, False, listener
                )
                self.assertGreaterEqual(len(listener.connection_ids), 4)
                for connection_id in listener.connection_ids:
                    self.assertNotIn(connection_id, proxy_addresses)
                    if mapped:
                        self.assertEqual(connection_id, ("localhost", 12345))
                    else:
                        self.assertIn(connection_id, self.nodes)
