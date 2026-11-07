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
import select
import socket
import ssl
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, NamedTuple, Optional, cast
from unittest import mock
from urllib.parse import quote_plus

import pytest

sys.path[0:0] = [""]

from pymongo import monitoring
from pymongo.hello import HelloCompat
from pymongo.pool_options import PoolOptions, _ProxyOptions
from pymongo.pool_shared import (
    _async_configured_socket,
    _async_create_connection,
    _configured_socket,
    _create_connection,
)
from pymongo.socks5_shared import (
    _SOCKS5_ATYP_DOMAIN,
    _SOCKS5_ATYP_IPV4,
    _SOCKS5_ATYP_IPV6,
    _SOCKS5_AUTH_NO_ACCEPTABLE,
    _SOCKS5_AUTH_NO_AUTH,
    _SOCKS5_AUTH_SUCCESS,
    _SOCKS5_AUTH_USER_PASS,
    _SOCKS5_AUTH_VERSION,
    _SOCKS5_CMD_CONNECT,
    _SOCKS5_REPLY_SUCCESS,
    _SOCKS5_RESERVED,
    _SOCKS5_VERSION,
    _Socks5Error,
    _Socks5Timeout,
)
from test import IntegrationTest, PyMongoTestCase, client_context
from test.asynchronous import AsyncIntegrationTest, AsyncPyMongoTestCase, async_client_context
from test.asynchronous.utils import async_wait_until
from test.helpers_shared import CERT_PATH, TLS_OPTIONS

pytestmark = pytest.mark.default

_SOCKS5_AUTH_GSSAPI_METHOD = 0x01
_SOCKS5_AUTH_FAILURE_STATUS = 0x01
_SOCKS4_VERSION = 0x04


class _ProseCase(NamedTuple):
    mapped: bool
    proxy_port: int
    credentials: Optional[tuple[str, str]]
    succeeds: bool


class TestSocks5(PyMongoTestCase):
    def _create_server(self, **kwargs: Any) -> _Socks5Server:
        server = _Socks5Server(**kwargs)
        self.addCleanup(server.close)
        return server

    def _connect(
        self,
        server: _Socks5Server,
        address: tuple[str, int] = ("localhost", 27017),
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

    def test_greeting_and_auth(self) -> None:
        server = self._create_server(method=_SOCKS5_AUTH_USER_PASS)
        self._connect(server, ("mongodb.example", 27017), "üser", "påss").close()
        server.close()
        self.assertEqual(
            server.greeting,
            bytes((_SOCKS5_VERSION, 2, _SOCKS5_AUTH_NO_AUTH, _SOCKS5_AUTH_USER_PASS)),
        )
        self.assertEqual(
            server.auth,
            bytes((_SOCKS5_AUTH_VERSION, len("üser".encode())))
            + "üser".encode()
            + bytes((len("påss".encode()),))
            + "påss".encode(),
        )
        self.assertEqual(
            server.request,
            bytes(
                (
                    _SOCKS5_VERSION,
                    _SOCKS5_CMD_CONNECT,
                    _SOCKS5_RESERVED,
                    _SOCKS5_ATYP_DOMAIN,
                    len(b"mongodb.example"),
                )
            )
            + b"mongodb.example"
            + (27017).to_bytes(2, "big"),
        )

        server = self._create_server(method=_SOCKS5_AUTH_NO_AUTH)
        self._connect(server, ("127.0.0.1", 27017), "user", "password").close()
        server.close()
        self.assertEqual(server.auth, b"")

        for method in (
            _SOCKS5_AUTH_USER_PASS,
            _SOCKS5_AUTH_GSSAPI_METHOD,
            _SOCKS5_AUTH_NO_ACCEPTABLE,
        ):
            server = self._create_server(method=method)
            with self.subTest(method=method), self.assertRaises(_Socks5Error):
                self._connect(server)
            server.close()
            self.assertEqual(server.greeting, bytes((_SOCKS5_VERSION, 1, _SOCKS5_AUTH_NO_AUTH)))

        server = self._create_server(
            method=_SOCKS5_AUTH_USER_PASS, auth_status=_SOCKS5_AUTH_FAILURE_STATUS
        )
        with self.assertRaises(_Socks5Error):
            self._connect(server, username="user", password="password")
        server.close()

    def test_connect_request_and_bound_replies(self) -> None:
        for host, expected in (
            ("192.0.2.1", bytes((_SOCKS5_ATYP_IPV4,)) + socket.inet_aton("192.0.2.1")),
            (
                "2001:db8::1",
                bytes((_SOCKS5_ATYP_IPV6,)) + socket.inet_pton(socket.AF_INET6, "2001:db8::1"),
            ),
            (
                "münich.example",
                bytes((_SOCKS5_ATYP_DOMAIN, len("xn--mnich-kva.example")))
                + b"xn--mnich-kva.example",
            ),
        ):
            server = self._create_server()
            self._connect(server, (host, 27017)).close()
            server.close()
            self.assertEqual(
                server.request,
                bytes((_SOCKS5_VERSION, _SOCKS5_CMD_CONNECT, _SOCKS5_RESERVED))
                + expected
                + (27017).to_bytes(2, "big"),
            )

        for atyp in (_SOCKS5_ATYP_IPV4, _SOCKS5_ATYP_DOMAIN, _SOCKS5_ATYP_IPV6):
            server = self._create_server(bound_atyp=atyp, payload=b"mongo")
            sock = self._connect(server)
            self.assertEqual(sock.recv(5), b"mongo")
            sock.close()
            server.close()

        for reply in range(1, 9):
            server = self._create_server(reply=reply)
            with self.subTest(reply=reply), self.assertRaises(_Socks5Error):
                self._connect(server)
            server.close()

    def test_tcp_socket_without_cloexec_falls_back(self) -> None:
        from pymongo import pool_shared

        sock = socket.socket()
        self.addCleanup(sock.close)
        with mock.patch.object(
            pool_shared.socket, "socket", side_effect=[OSError("unsupported"), sock]
        ):
            self.assertIs(pool_shared._new_tcp_socket(socket.AF_INET, socket.SOCK_STREAM, 0), sock)

    def test_client_proxy_options_and_repr(self) -> None:
        from pymongo.synchronous.mongo_client import MongoClient
        from pymongo.synchronous.topology import Topology

        c1 = MongoClient("mongodb://localhost/?proxyHost=127.0.0.1", connect=False)
        c2 = MongoClient("mongodb://localhost/?proxyHost=127.0.0.1", connect=False)
        c3 = MongoClient(
            "mongodb://localhost/?proxyHost=localhost&proxyPort=1234"
            "&proxyUsername=uri-user&proxyPassword=uri-password",
            proxyHost="[::1]",
            proxyPort=5678,
            proxyUsername="",
            proxyPassword="",
            connect=False,
        )
        c4 = MongoClient(
            "mongodb://user:pass@localhost/?proxyHost=127.0.0.1"
            "&proxyUsername=proxyuser&proxyPassword=hunter2",
            connect=False,
        )
        c5 = MongoClient(
            "mongodb://localhost/?proxyPort=1081", proxyHost="proxy.example", connect=False
        )
        c6 = MongoClient(
            "mongodb://localhost/?proxyHost=proxy.example&proxyUsername=u",
            proxyPassword="p",
            connect=False,
        )
        for client in (c1, c2, c3, c4, c5, c6):
            self.addCleanup(client.close)

        topology = Topology(c1._topology_settings)
        expected = _ProxyOptions("127.0.0.1")
        self.assertEqual(
            topology._create_pool_for_server(("localhost", 27017)).opts._proxy_options, expected
        )
        self.assertEqual(
            topology._create_pool_for_monitor(("localhost", 27017)).opts._proxy_options, expected
        )
        self.assertEqual(c1, c2)
        self.assertNotEqual(c1, c3)
        self.assertEqual(c3.options.pool_options._proxy_options, _ProxyOptions("::1", 5678))
        self.assertEqual(
            c5.options.pool_options._proxy_options, _ProxyOptions("proxy.example", 1081)
        )
        self.assertEqual(
            c6.options.pool_options._proxy_options, _ProxyOptions("proxy.example", 1080, "u", "p")
        )
        for secret in ("hunter2", "proxyuser", "pass"):
            self.assertNotIn(secret, repr(c4))
        self.assertNotIn("hunter2", repr(c4.options.pool_options._proxy_options))

    def test_bad_versions_eof_timeout_and_tls_sni(self) -> None:
        for server, creds in (
            (self._create_server(method_version=_SOCKS4_VERSION), (None, None)),
            (self._create_server(reply_version=_SOCKS4_VERSION), (None, None)),
            (self._create_server(eof_stage="greeting"), (None, None)),
            (self._create_server(method=_SOCKS5_AUTH_USER_PASS, eof_stage="auth"), ("u", "p")),
            (self._create_server(eof_stage="connect"), (None, None)),
        ):
            with self.assertRaises(_Socks5Error):
                self._connect(server, username=creds[0], password=creds[1])
            server.close()

        server = self._create_server(stall=True)
        with self.assertRaisesRegex(socket.timeout, "SOCKS5 proxy timed out"):
            self._connect(server, timeout=0.1)
        server.close()

        sni_names: list[Optional[str]] = []
        tls_server = _TLSServer(sni_names)
        self.addCleanup(tls_server.close)
        proxy = self._create_server(target=tls_server.address)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        sock = _configured_socket(
            ("mongodb.example", tls_server.address[1]),
            PoolOptions(
                connect_timeout=2,
                ssl_context=cast(Any, ctx),
                proxy_options=_ProxyOptions(proxy.address[0], proxy.address[1]),
            ),
        )
        sock.close()
        self.assertEqual(sni_names, ["mongodb.example"])

    def test_socks5_errors_are_not_labeled_system_overloaded(self) -> None:
        from pymongo.errors import AutoReconnect, NetworkTimeout
        from pymongo.pool_shared import _raise_connection_failure
        from pymongo.synchronous.pool import Pool

        pool = Pool(("localhost", 27017), PoolOptions(connect_timeout=1))
        self.addCleanup(pool.close)
        for cause, overloaded in (
            (_Socks5Error("boom"), False),
            (_Socks5Timeout("SOCKS5 proxy timed out"), False),
            (OSError("boom"), True),
        ):
            with self.assertRaises((AutoReconnect, NetworkTimeout)) as raised:
                _raise_connection_failure(("localhost", 27017), cause)
            pool._handle_connection_error(raised.exception)
            self.assertEqual(raised.exception.has_error_label("SystemOverloadedError"), overloaded)
            self.assertEqual(raised.exception.has_error_label("RetryableError"), overloaded)

    def test_sync_proxy_error_and_fallback_paths(self) -> None:
        import pymongo.synchronous.socks5_support as socks5_module

        options = PoolOptions(connect_timeout=1, proxy_options=_ProxyOptions("proxy.invalid", 1080))
        target = "pymongo.socks5_shared.socket.getaddrinfo"
        for err, exc_type in (
            (socket.gaierror("failed"), _Socks5Error),
            (socket.timeout("timed out"), _Socks5Timeout),
        ):
            with mock.patch(target, side_effect=err), self.assertRaises(exc_type):
                _create_connection(("localhost", 27017), options)

        with (
            mock.patch(target, return_value=[]),
            self.assertRaisesRegex(_Socks5Error, "no addresses"),
        ):
            _create_connection(("localhost", 27017), options)

        dead = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", 1))
        for err, exc_type in (
            (ConnectionRefusedError("refused"), _Socks5Error),
            (socket.timeout(), _Socks5Timeout),
        ):
            with (
                mock.patch(target, return_value=[dead]),
                mock.patch.object(socks5_module, "_proxy_connect", side_effect=err),
                self.assertRaises(exc_type),
            ):
                _create_connection(("localhost", 27017), options)

        server = self._create_server()
        live = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", server.address)
        orig_connect = socks5_module._proxy_connect

        def fail_first(sock: socket.socket, addr: Any, deadline: Optional[float]) -> None:
            if addr == ("127.0.0.1", 1):
                raise ConnectionRefusedError("refused")
            orig_connect(sock, addr, deadline)

        with (
            mock.patch(target, return_value=[dead, live]),
            mock.patch.object(socks5_module, "_proxy_connect", side_effect=fail_first),
        ):
            self._connect(server).close()
        server.close()

        with (
            mock.patch.object(socks5_module, "_proxy_deadline", return_value=0),
            mock.patch.object(socks5_module, "_proxy_getaddrinfo", return_value=[dead, dead]),
            mock.patch.object(
                socks5_module, "_proxy_connect", side_effect=socket.timeout("timed out")
            ) as connect,
            self.assertRaises(_Socks5Timeout),
        ):
            _create_connection(("mongodb.example", 27017), options)
        connect.assert_called_once()

        for error in (OSError("handshake failed"), KeyboardInterrupt):
            server = self._create_server()
            opts = PoolOptions(connect_timeout=1, proxy_options=_ProxyOptions(*server.address))
            with (
                mock.patch.object(socks5_module, "_socks5_sendall", side_effect=error),
                self.assertRaises(
                    _Socks5Error if isinstance(error, OSError) else KeyboardInterrupt
                ),
            ):
                _create_connection(("localhost", 27017), opts)
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


def _start_socks5_proxies(nodes: list[tuple[str, int]]) -> list[subprocess.Popen[bytes]]:
    tools = Path(os.environ.get("DRIVERS_TOOLS", "drivers-evergreen-tools"))
    script = tools / ".evergreen" / "socks5srv.py"
    host, port = nodes[0]
    seed = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    mapping = f"localhost:12345 to {seed}"
    processes = []
    try:
        for proxy_port, auth in ((1080, True), (1081, False)):
            args = [sys.executable, str(script), "--port", str(proxy_port), "--map", mapping]
            if auth:
                args.extend(("--auth", "username:p4ssw0rd"))
            process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            processes.append(process)
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"SOCKS5 proxy on port {proxy_port} exited")
                try:
                    with socket.create_connection(("127.0.0.1", proxy_port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"SOCKS5 proxy on port {proxy_port} did not start")
                    time.sleep(0.1)
        return processes
    except BaseException:
        _stop_socks5_proxies(processes)
        raise


def _stop_socks5_proxies(processes: list[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


async def _maybe_await(value: Any) -> Any:
    return await value if hasattr(value, "__await__") else value


def _prose_hosts(nodes: list[tuple[str, int]], mapped: bool) -> str:
    if mapped:
        return "localhost:12345"
    return ",".join(
        f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"
        for host, port in nodes
    )


async def _run_prose_case(
    test: Any,
    async_api: bool,
    hosts: str,
    port: int,
    credentials: Optional[tuple[str, str]],
    succeeds: bool,
    use_uri: bool,
    listener: Optional[_CommandListener] = None,
) -> None:
    from pymongo.errors import ConnectionFailure, OperationFailure

    if async_api:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient as Client
    else:
        from pymongo.synchronous.mongo_client import MongoClient as Client
    proxy = {"proxyHost": "localhost", "proxyPort": port}
    if credentials:
        proxy.update(proxyUsername=credentials[0], proxyPassword=credentials[1])
    context = async_client_context if async_api else client_context
    options = dict(TLS_OPTIONS) if context.tls else {}
    options.update(test.credentials)
    options["serverSelectionTimeoutMS"] = 2500
    if hosts == "localhost:12345":
        options["directConnection"] = True
    if listener:
        options["event_listeners"] = [listener]
    if use_uri:
        query = "&".join(f"{key}={quote_plus(str(value))}" for key, value in proxy.items())
        hosts = f"mongodb://{hosts}/?{query}"
    else:
        options.update(proxy)
    client = Client(hosts, **options)
    try:
        try:
            await _maybe_await(client.admin.command(HelloCompat.CMD))
        except ConnectionFailure:
            if succeeds:
                raise
            return
        if not succeeds:
            raise AssertionError("Expected the SOCKS5 connection to fail")
        if listener:
            with pytest.raises(OperationFailure):
                await _maybe_await(client.admin.command("nonexistent_socks5_test_command"))
    finally:
        await _maybe_await(client.close())


async def _test_prose_table(test: Any, async_api: bool) -> None:
    cases = (
        _ProseCase(True, 1080, None, False),
        _ProseCase(True, 1081, None, True),
        _ProseCase(False, 1080, None, False),
        _ProseCase(False, 1081, None, True),
        _ProseCase(True, 1080, ("nonexistentuser", "badauth"), False),
        _ProseCase(True, 1081, ("nonexistentuser", "badauth"), True),
        _ProseCase(False, 1081, ("nonexistentuser", "badauth"), True),
        _ProseCase(True, 1080, ("username", "p4ssw0rd"), True),
        _ProseCase(False, 1080, ("username", "p4ssw0rd"), True),
    )
    for case in cases:
        for use_uri in (True, False):
            await _run_prose_case(
                test,
                async_api,
                _prose_hosts(test.nodes, case.mapped),
                case.proxy_port,
                case.credentials,
                case.succeeds,
                use_uri,
            )


async def _test_command_monitoring(test: Any, async_api: bool) -> None:
    proxy_addresses = {(host, port) for host in ("localhost", "127.0.0.1") for port in (1080, 1081)}
    for mapped in (True, False):
        listener = _CommandListener()
        await _run_prose_case(
            test, async_api, _prose_hosts(test.nodes, mapped), 1081, None, True, False, listener
        )
        assert len(listener.connection_ids) >= 4
        for address in listener.connection_ids:
            assert address not in proxy_addresses
            assert address == (("localhost", 12345) if mapped else address)
            if not mapped:
                assert address in test.nodes


class TestSocks5Prose(IntegrationTest):
    @client_context.require_replica_set
    def setUp(self) -> None:
        super().setUp()
        if not os.environ.get("TEST_SOCKS5"):
            self.skipTest("SOCKS5 proxies are started only by the SOCKS5 task")
        self.nodes = sorted(client_context.nodes)
        self.addCleanup(_stop_socks5_proxies, _start_socks5_proxies(self.nodes))

    def test_connection_string_prose_table(self) -> None:
        asyncio.run(_test_prose_table(self, False))

    def test_command_monitoring_does_not_reference_proxy(self) -> None:
        asyncio.run(_test_command_monitoring(self, False))


class _TLSServer:
    def __init__(self, sni_names: list[Optional[str]]) -> None:
        self.sni_names = sni_names
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.settimeout(3)
        self.address = self.listener.getsockname()
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(f"{CERT_PATH}/server.pem")
            context.set_servername_callback(lambda _sock, name, _ctx: self.sni_names.append(name))
            with self.listener.accept()[0] as raw:
                with context.wrap_socket(raw, server_side=True) as conn:
                    conn.recv(1)
        except OSError:
            pass
        finally:
            self.listener.close()

    def close(self) -> None:
        self.listener.close()
        self.thread.join(timeout=3)


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
                version, nmethods = self._read(conn, 2)
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
                        conn.sendall(bytes((_SOCKS5_AUTH_VERSION,)))
                        return
                    conn.sendall(bytes((_SOCKS5_AUTH_VERSION, self.auth_status)))
                    if self.auth_status:
                        return
                version, command, reserved, atyp = self._read(conn, 4)
                if atyp == _SOCKS5_ATYP_IPV4:
                    addr = self._read(conn, 4)
                elif atyp == _SOCKS5_ATYP_IPV6:
                    addr = self._read(conn, 16)
                else:
                    length = self._read(conn, 1)
                    addr = length + self._read(conn, length[0])
                self.request = (
                    bytes((version, command, reserved, atyp)) + addr + self._read(conn, 2)
                )
                if self.eof_stage == "connect":
                    conn.sendall(bytes((_SOCKS5_VERSION, _SOCKS5_REPLY_SUCCESS)))
                    return
                if self.target is not None:
                    upstream = socket.create_connection(self.target, timeout=3)
                if self.bound_atyp == _SOCKS5_ATYP_IPV4:
                    bound = bytes((_SOCKS5_ATYP_IPV4,)) + socket.inet_aton("127.0.0.1") + bytes(2)
                elif self.bound_atyp == _SOCKS5_ATYP_IPV6:
                    bound = (
                        bytes((_SOCKS5_ATYP_IPV6,))
                        + socket.inet_pton(socket.AF_INET6, "::1")
                        + bytes(2)
                    )
                else:
                    host = b"x"
                    bound = bytes((_SOCKS5_ATYP_DOMAIN, len(host))) + host + bytes(2)
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


@pytest.mark.default_async
class TestAsyncSocks5(AsyncPyMongoTestCase):
    """Exercise the asynchronous proxy paths without duplicating sync scenarios."""

    def _create_server(self, **kwargs: Any) -> _Socks5Server:
        server = _Socks5Server(**kwargs)
        self.addAsyncCleanup(server.close)
        return server

    async def _connect(
        self,
        server: _Socks5Server,
        address: tuple[str, int] = ("localhost", 27017),
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 2,
    ) -> socket.socket:
        from pymongo.pool_shared import _async_create_connection

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

    async def test_protocol_and_connection_errors(self) -> None:
        for host, atyp in (
            ("127.0.0.1", _SOCKS5_ATYP_IPV4),
            ("2001:db8::1", _SOCKS5_ATYP_IPV6),
            ("münich.example", _SOCKS5_ATYP_DOMAIN),
        ):
            server = self._create_server()
            sock = await self._connect(server, (host, 27017))
            sock.close()
            server.close()
            self.assertEqual(server.request[3], atyp)

        server = self._create_server(method=_SOCKS5_AUTH_USER_PASS)
        sock = await self._connect(server, username="üser", password="påss")
        sock.close()
        server.close()
        self.assertIn("üser".encode(), server.auth)

        for method in (_SOCKS5_AUTH_GSSAPI_METHOD, _SOCKS5_AUTH_NO_ACCEPTABLE):
            server = self._create_server(method=method)
            with self.assertRaises(_Socks5Error):
                await self._connect(server)
            server.close()
        server = self._create_server(
            method=_SOCKS5_AUTH_USER_PASS, auth_status=_SOCKS5_AUTH_FAILURE_STATUS
        )
        with self.assertRaises(_Socks5Error):
            await self._connect(server, username="u", password="p")
        server.close()
        for reply in range(1, 9):
            server = self._create_server(reply=reply)
            with self.assertRaises(_Socks5Error):
                await self._connect(server)
            server.close()
        server = self._create_server(eof_stage="greeting")
        with self.assertRaisesRegex(_Socks5Error, "unexpected EOF"):
            await self._connect(server)
        server.close()

    async def test_async_client_options_and_configured_socket(self) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient
        from pymongo.asynchronous.uri_parser import parse_uri
        from pymongo.pool_shared import _async_configured_socket

        self.assertEqual(
            (await parse_uri("mongodb://localhost/?proxyHost=localhost"))["options"]["proxyHost"],
            "localhost",
        )

        server = self._create_server()
        client = AsyncMongoClient(
            "mongodb://localhost/?proxyHost=localhost&proxyPort=1234",
            proxyHost=server.address[0],
            proxyPort=server.address[1],
            connect=False,
        )
        self.assertEqual(
            client.options.pool_options._proxy_options,
            _ProxyOptions(server.address[0], server.address[1]),
        )
        await client.close()

        tls = _TLSServer([])
        self.addAsyncCleanup(tls.close)
        proxy = self._create_server(target=tls.address)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        sock = await _async_configured_socket(
            ("mongodb.example", tls.address[1]),
            PoolOptions(
                connect_timeout=2,
                ssl_context=cast(Any, context),
                proxy_options=_ProxyOptions(proxy.address[0], proxy.address[1]),
            ),
        )
        sock.close()
        self.assertEqual(tls.sni_names, ["mongodb.example"])

    async def test_async_proxy_error_paths(self) -> None:
        from pymongo.asynchronous import socks5_support
        from pymongo.pool_shared import _async_create_connection

        options = PoolOptions(proxy_options=_ProxyOptions("proxy.invalid", 1080))
        for error, expected in (
            (TimeoutError("timed out"), _Socks5Timeout),
            (OSError("resolution failed"), _Socks5Error),
        ):
            with (
                mock.patch.object(socks5_support, "_async_proxy_getaddrinfo", side_effect=error),
                self.assertRaises(expected),
            ):
                await _async_create_connection(("localhost", 27017), options)

        addresses = [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", 1))]

        async def fake_addresses(*_args: Any, **_kwargs: Any) -> Any:
            return addresses

        for error, expected in (
            (TimeoutError("timed out"), _Socks5Timeout),
            (OSError("refused"), _Socks5Error),
        ):
            with (
                mock.patch.object(socks5_support, "_async_proxy_getaddrinfo", new=fake_addresses),
                mock.patch.object(socks5_support, "_async_proxy_connect", side_effect=error),
                self.assertRaises(expected),
            ):
                await _async_create_connection(("localhost", 27017), options)
        with (
            mock.patch.object(socks5_support, "_async_proxy_getaddrinfo", new=fake_addresses),
            mock.patch.object(socks5_support, "_async_proxy_connect", side_effect=socket.timeout()),
            mock.patch.object(
                socks5_support, "_proxy_remaining", side_effect=_Socks5Timeout("expired")
            ),
            self.assertRaises(_Socks5Timeout),
        ):
            await _async_create_connection(("localhost", 27017), options)

        async def no_addresses(*_args: Any, **_kwargs: Any) -> Any:
            return []

        with (
            mock.patch.object(socks5_support, "_async_proxy_getaddrinfo", new=no_addresses),
            self.assertRaisesRegex(_Socks5Error, "returned no addresses"),
        ):
            await _async_create_connection(("localhost", 27017), options)

        for error in (OSError("handshake failed"), KeyboardInterrupt):
            server = self._create_server()
            options = PoolOptions(proxy_options=_ProxyOptions(server.address[0], server.address[1]))
            with (
                mock.patch.object(socks5_support, "_async_socks5_sendall", side_effect=error),
                self.assertRaisesRegex(
                    _Socks5Error if isinstance(error, OSError) else KeyboardInterrupt,
                    "handshake failed" if isinstance(error, OSError) else None,
                ),
            ):
                await _async_create_connection(("localhost", 27017), options)
            server.close()

    async def test_async_timeout_cancellation_and_proxy_fallback(self) -> None:
        from pymongo.pool_shared import _async_create_connection

        server = self._create_server(stall=True)
        with self.assertRaisesRegex(socket.timeout, "SOCKS5 proxy timed out"):
            await self._connect(server, timeout=0.1)
        server.close()

        server = self._create_server(stall=True)
        task = asyncio.create_task(self._connect(server))
        await async_wait_until(server.started.is_set, "receive SOCKS5 greeting", timeout=2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        server.close()

        dead = ("127.0.0.1", 1)
        server = self._create_server()
        fake_results = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", dead),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", server.address),
        ]
        loop = asyncio.get_running_loop()
        orig_connect = loop.sock_connect

        async def fail_first(sock: socket.socket, target: tuple[str, int]) -> None:
            if target == dead:
                raise ConnectionRefusedError("connection failed")
            await orig_connect(sock, target)

        with (
            mock.patch(
                "pymongo.socks5_shared._getaddrinfo", new=mock.AsyncMock(return_value=fake_results)
            ),
            mock.patch.object(loop, "sock_connect", new=fail_first),
        ):
            (await self._connect(server)).close()
        server.close()

        with (
            mock.patch("pymongo.socks5_shared._getaddrinfo", side_effect=socket.gaierror("failed")),
            self.assertRaises(_Socks5Error),
        ):
            await _async_create_connection(
                ("localhost", 27017),
                PoolOptions(proxy_options=_ProxyOptions("proxy.invalid", 1080)),
            )


@pytest.mark.default_async
class TestSocks5ProseAsync(AsyncIntegrationTest):
    @async_client_context.require_replica_set
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        if not os.environ.get("TEST_SOCKS5"):
            self.skipTest("SOCKS5 proxies are started only by the SOCKS5 task")
        self.nodes = sorted(async_client_context.nodes)
        self.addAsyncCleanup(_stop_socks5_proxies, _start_socks5_proxies(self.nodes))

    async def test_connection_string_prose_table(self) -> None:
        await _test_prose_table(self, True)

    async def test_command_monitoring_does_not_reference_proxy(self) -> None:
        await _test_command_monitoring(self, True)
