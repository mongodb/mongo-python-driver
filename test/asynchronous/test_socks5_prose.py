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
from test.socks5_helpers import Socks5Server

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
