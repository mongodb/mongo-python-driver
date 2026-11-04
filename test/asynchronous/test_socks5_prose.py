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

"""SOCKS5 connection-string prose tests."""

from __future__ import annotations

import os
import select
import socket
import threading
from typing import Optional
from urllib.parse import quote_plus

import pytest

from pymongo import monitoring
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.hello import HelloCompat
from test.asynchronous import AsyncIntegrationTest, async_client_context
from test.helpers_shared import TLS_OPTIONS

_IS_SYNC = False
pytestmark = pytest.mark.default_async


class _CommandListener(monitoring.CommandListener):
    def __init__(self) -> None:
        self.connection_ids: list[tuple[str, Optional[int]]] = []

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        self.connection_ids.append(event.connection_id)

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        self.connection_ids.append(event.connection_id)

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        self.connection_ids.append(event.connection_id)


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


class TestSocks5Prose(AsyncIntegrationTest):
    @async_client_context.require_replica_set
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.nodes = sorted(async_client_context.nodes)
        self.mapping = {("localhost", 12345): self.nodes[0]}
        self.credentials = getattr(self, "credentials", {})
        if os.environ.get("SOCKS5_PROXIES_STARTED"):
            self.auth_port = 1080
            self.noauth_port = 1081
        else:
            auth_proxy = Socks5Server(1080, self.mapping, ("username", "p4ssw0rd"))
            noauth_proxy = Socks5Server(1081, self.mapping)
            self.addCleanup(auth_proxy.close)
            self.addCleanup(noauth_proxy.close)
            self.auth_port = auth_proxy.port
            self.noauth_port = noauth_proxy.port

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
        proxy_options = {"proxyHost": "localhost", "proxyPort": port}
        if credentials:
            proxy_options["proxyUsername"], proxy_options["proxyPassword"] = credentials
        options = dict(TLS_OPTIONS)
        options.update(self.credentials)
        options.update(proxy_options)
        options["serverSelectionTimeoutMS"] = 2500
        if hosts == "localhost:12345":
            options["directConnection"] = True
        if listener is not None:
            options["event_listeners"] = [listener]
        if use_uri:
            uri_options = "&".join(
                f"{key}={quote_plus(str(value))}" for key, value in proxy_options.items()
            )
            uri_options += "&serverSelectionTimeoutMS=2500"
            if hosts == "localhost:12345":
                uri_options += "&directConnection=true"
            if TLS_OPTIONS:
                uri_options += "&" + "&".join(
                    f"{key}={quote_plus(str(value))}" for key, value in TLS_OPTIONS.items()
                )
            auth = ""
            if self.credentials:
                auth = f"{quote_plus(self.credentials['username'])}:{quote_plus(self.credentials['password'])}@"
            client = AsyncMongoClient(
                f"mongodb://{auth}{hosts}/?{uri_options}",
                event_listeners=options.get("event_listeners"),
            )
        else:
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
                await self._run_case(
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
