"""Tests for the KMS connect callback and HTTP proxy support."""

from __future__ import annotations

import asyncio
import dataclasses
import http.client
import socket
import ssl
import threading
import time
import unittest
from asyncio.trsock import TransportSocket
from typing import Any
from unittest import mock

import pytest

from bson.binary import Binary
from pymongo.asynchronous.encryption import (
    AsyncClientEncryption,
    _connect_kms,
    _EncryptionIO,
    _wrap_encryption_errors,
)
from pymongo.encryption_options import (
    AsyncHTTPProxyKMSConnect,
    AutoEncryptionOpts,
    HTTPProxyKMSConnect,
    KMSConnectContext,
)
from pymongo.errors import ConfigurationError, EncryptionError
from pymongo.pool_options import PoolOptions
from pymongo.ssl_support import get_ssl_context
from test.asynchronous import AsyncPyMongoTestCase
from test.asynchronous.test_encryption import OPTS, AsyncEncryptionIntegrationTest
from test.helpers_shared import AWS_CREDS, CA_PEM, CLIENT_PEM

_IS_SYNC = False

pytestmark = pytest.mark.encryption

KMS_PROXY_HOST = "127.0.0.1"
KMS_PROXY_PORT = 9004
KMS_TLS_PROXY_PORT = 9005

AWS_MASTER_KEY = {
    "region": "us-east-1",
    "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
}


class TestKmsConnectCallbackUnit(AsyncPyMongoTestCase):
    """Contract checks for kms_connect_callback that need no KMS server."""

    @staticmethod
    def _pool_options():
        return PoolOptions(connect_timeout=10, socket_timeout=10, ssl_context=None)

    async def test_init_kms_connect_callback(self):
        opts = AutoEncryptionOpts({}, "k.d")
        self.assertIsNone(opts._kms_connect_callback)

        async def callback(context):
            raise AssertionError("not called")

        opts = AutoEncryptionOpts({}, "k.d", kms_connect_callback=callback)
        self.assertIs(opts._kms_connect_callback, callback)

        for bad in [1, "not-callable", object()]:
            with self.assertRaisesRegex(TypeError, "kms_connect_callback must be callable"):
                AutoEncryptionOpts({}, "k.d", kms_connect_callback=bad)  # type: ignore[arg-type]

        context = KMSConnectContext(host="kms.example.com", port=443, timeout=9.5)
        self.assertEqual(context.host, "kms.example.com")
        self.assertEqual(context.port, 443)
        self.assertEqual(context.timeout, 9.5)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            context.host = "evil.example.com"  # type: ignore[misc]

    async def test_non_socket_return_raises_configuration_error(self):
        async def callback(context):
            return "not-a-socket"

        with self.assertRaisesRegex(ConfigurationError, "must return a connected"):
            await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 10.0)

    async def test_already_wrapped_socket_is_rejected(self):
        # ssl.SSLSocket passes isinstance but cannot be TLS-wrapped again.
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        left, right = socket.socketpair()
        self.addCleanup(right.close)
        # No peer needed to produce a genuine ssl.SSLSocket.
        wrapped = ctx.wrap_socket(left, do_handshake_on_connect=False, server_hostname="x")
        self.addCleanup(wrapped.close)

        async def callback(context):
            return wrapped

        with self.assertRaisesRegex(ConfigurationError, "unwrapped"):
            await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 10.0)

    async def test_context_receives_host_port_and_timeout(self):
        received = []
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)

        async def callback(context):
            received.append(context)
            return left

        # ssl_context=None returns the socket unchanged, so a plain socket is accepted.
        conn = await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 12.5)
        self.assertIs(conn, left)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].host, "kms.example.com")
        self.assertEqual(received[0].port, 443)
        self.assertEqual(received[0].timeout, 12.5)

    async def test_non_blocking_socket_from_callback_is_accepted(self):
        # Without the driver normalizing the mode, this raises ValueError.
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(CLIENT_PEM)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def serve():
            try:
                conn, _ = listener.accept()
                server_ctx.wrap_socket(conn, server_side=True).close()
            except OSError:
                pass

        threading.Thread(target=serve, daemon=True).start()

        # Built as the driver does, for the flavor-correct type; the local cert won't verify.
        client_ctx = get_ssl_context(None, None, None, None, True, True, False, _IS_SYNC)
        options = PoolOptions(connect_timeout=10, socket_timeout=10, ssl_context=client_ctx)

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            sock.setblocking(False)
            return sock

        async def callback(context):
            if _IS_SYNC:
                return connect()
            return await asyncio.get_running_loop().run_in_executor(None, connect)

        conn = await _connect_kms(listener.getsockname(), options, callback, 10.0)
        self.addCleanup(conn.close)
        self.assertIsNotNone(conn.gettimeout())

    async def test_asyncio_transport_socket_is_rejected(self):
        # get_extra_info("socket") is a TransportSocket, not a socket.socket.
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)

        async def callback(context):
            return TransportSocket(left)

        with self.assertRaisesRegex(ConfigurationError, "TransportSocket"):
            await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 10.0)

    async def test_http_proxy_helper_tunnels_and_reports_refusal(self):
        # Covers the CONNECT handshake without KMS credentials.
        accepted = []

        def stub(listener, reply):
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            accepted.append(conn.recv(4096))
            conn.sendall(reply)
            conn.close()

        def run_stub(reply):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            self.addCleanup(listener.close)
            threading.Thread(target=stub, args=(listener, reply), daemon=True).start()
            return listener.getsockname()

        host, port = run_stub(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        callback = AsyncHTTPProxyKMSConnect(host, port)
        context = KMSConnectContext(host="kms.example.com", port=443, timeout=10)
        sock = await callback(context)
        self.addCleanup(sock.close)
        self.assertIsInstance(sock, socket.socket)
        self.assertEqual(accepted[0].split(b"\r\n")[0], b"CONNECT kms.example.com:443 HTTP/1.1")

        host, port = run_stub(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
        with self.assertRaisesRegex(OSError, "refused CONNECT"):
            await AsyncHTTPProxyKMSConnect(host, port)(context)

        # Any 2xx status is a successful tunnel, not just HTTP/1.1 200.
        host, port = run_stub(b"HTTP/1.0 200 Connection Established\r\n\r\n")
        sock = await AsyncHTTPProxyKMSConnect(host, port)(context)
        self.addCleanup(sock.close)
        self.assertIsInstance(sock, socket.socket)

        # A status code must be exactly three digits; both a four-digit code
        # and a zero-padded code are malformed.
        for reply in (b"HTTP/1.1 2000 Evil\r\n\r\n", b"HTTP/1.1 00200 Evil\r\n\r\n"):
            host, port = run_stub(reply)
            with self.assertRaisesRegex(OSError, "refused CONNECT"):
                await AsyncHTTPProxyKMSConnect(host, port)(context)

    async def test_control_characters_in_kms_host_are_rejected(self):
        # The host is configurable, so reject CR/LF before it reaches the
        # CONNECT request or Host header.
        callback = AsyncHTTPProxyKMSConnect("proxy.example.com", 8080)
        context = KMSConnectContext(host="kms.example.com\r\nX-Injected: 1", port=443, timeout=10)
        with self.assertRaisesRegex(ConfigurationError, "control characters"):
            await callback(context)

    async def test_cancelled_tls_wrap_closes_late_socket(self):
        # Cancelling a TLS wrap can leave the executor producing an SSLSocket
        # after the task is gone; the done callback must close that result.
        if _IS_SYNC:
            raise unittest.SkipTest("the cancel-safe wrap is an async path")
        from pymongo.pool_shared import _close_late_socket

        left, right = socket.socketpair()
        future = asyncio.get_running_loop().create_future()
        future.set_result(left)
        self.assertNotEqual(left.fileno(), -1)
        _close_late_socket(future)
        self.assertEqual(left.fileno(), -1)
        self.addCleanup(right.close)

    async def test_tls_proxy_helper_bridges_the_tunnel(self):
        # Covers the TLS-proxy path and the socketpair relay without KMS creds.
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(CLIENT_PEM)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def stub_proxy():
            conn = None
            try:
                conn, _ = listener.accept()
                tls = server_ctx.wrap_socket(conn, server_side=True)
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = tls.recv(4096)
                    if not chunk:
                        return
                    request += chunk
                tls.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                # The tunnelled peer speaks only after the client does, as a
                # TLS server would.
                tls.sendall(b"echo:" + tls.recv(64))
                tls.close()
            except OSError:
                pass
            finally:
                if conn is not None:
                    conn.close()

        threading.Thread(target=stub_proxy, daemon=True).start()

        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE
        host, port = listener.getsockname()
        context = KMSConnectContext(host="kms.example.com", port=443, timeout=10)

        sock = await AsyncHTTPProxyKMSConnect(host, port, client_ctx)(context)
        self.addCleanup(sock.close)
        sock.settimeout(10)
        if _IS_SYNC:
            sock.sendall(b"ping")
            self.assertEqual(sock.recv(64), b"echo:ping")
        else:
            await asyncio.get_running_loop().run_in_executor(None, sock.sendall, b"ping")
            data = await asyncio.get_running_loop().run_in_executor(None, sock.recv, 64)
            self.assertEqual(data, b"echo:ping")

    async def test_bridge_does_not_inherit_the_connect_deadline(self):
        # The CONNECT deadline can be much shorter than the KMS request that
        # follows through the tunnel; the relay must outlast it.
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(CLIENT_PEM)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def stub_proxy():
            conn = None
            try:
                conn, _ = listener.accept()
                tls = server_ctx.wrap_socket(conn, server_side=True)
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = tls.recv(4096)
                    if not chunk:
                        return
                    request += chunk
                tls.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                # Outlast the connect-phase deadline before the KMS side speaks.
                time.sleep(0.4)
                tls.sendall(b"echo:" + tls.recv(64))
                tls.close()
            except OSError:
                pass
            finally:
                if conn is not None:
                    conn.close()

        threading.Thread(target=stub_proxy, daemon=True).start()

        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE
        host, port = listener.getsockname()
        context = KMSConnectContext(host="kms.example.com", port=443, timeout=0.2)

        sock = await AsyncHTTPProxyKMSConnect(host, port, client_ctx)(context)
        self.addCleanup(sock.close)
        sock.settimeout(10)
        if _IS_SYNC:
            sock.sendall(b"ping")
            self.assertEqual(sock.recv(64), b"echo:ping")
        else:
            await asyncio.get_running_loop().run_in_executor(None, sock.sendall, b"ping")
            data = await asyncio.get_running_loop().run_in_executor(None, sock.recv, 64)
            self.assertEqual(data, b"echo:ping")

    async def test_non_coroutine_callback_is_rejected(self):
        # The async API needs a coroutine function; a plain def must be
        # rejected before it runs its blocking connect on the event loop.
        if _IS_SYNC:
            raise unittest.SkipTest("a regular function is correct for the sync API")

        entered = []

        def callback(context):
            entered.append(context)
            return None

        with self.assertRaisesRegex(ConfigurationError, "coroutine function"):
            await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 10.0)
        self.assertEqual(entered, [], "invalid callback must not be entered")

    async def test_proxy_closing_before_connect_reply_raises(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def stub_proxy():
            try:
                conn, _ = listener.accept()
                # Read the CONNECT request, then hang up without replying.
                conn.recv(4096)
                conn.close()
            except OSError:
                pass

        threading.Thread(target=stub_proxy, daemon=True).start()

        host, port = listener.getsockname()
        context = KMSConnectContext(host="kms.example.com", port=443, timeout=10)
        with self.assertRaisesRegex(OSError, "proxy closed the connection"):
            await AsyncHTTPProxyKMSConnect(host, port)(context)

    async def test_tunnel_keeps_bytes_sent_with_the_connect_reply(self):
        # A proxy may coalesce its 200 with tunnelled bytes; reading past the
        # header would silently drop them.
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def stub_proxy():
            try:
                conn, _ = listener.accept()
                conn.recv(4096)
                conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\nearly-bytes")
                conn.close()
            except OSError:
                pass

        threading.Thread(target=stub_proxy, daemon=True).start()

        host, port = listener.getsockname()
        context = KMSConnectContext(host="kms.example.com", port=443, timeout=10)
        sock = await AsyncHTTPProxyKMSConnect(host, port)(context)
        self.addCleanup(sock.close)
        sock.settimeout(10)
        if _IS_SYNC:
            data = sock.recv(64)
        else:
            data = await asyncio.get_running_loop().run_in_executor(None, sock.recv, 64)
        self.assertEqual(data, b"early-bytes")

    async def test_unconnected_socket_from_callback_is_rejected(self):
        # The contract says connected; an unconnected socket would otherwise
        # fail later as a transient error and be retried.
        bare = socket.socket()
        self.addCleanup(bare.close)

        async def callback(context):
            return bare

        with self.assertRaisesRegex(ConfigurationError, "already connected"):
            await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 10.0)

    async def test_ipv6_host_is_bracketed_in_connect(self):
        accepted = []
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def stub_proxy():
            try:
                conn, _ = listener.accept()
                accepted.append(conn.recv(4096))
                conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                conn.close()
            except OSError:
                pass

        threading.Thread(target=stub_proxy, daemon=True).start()

        host, port = listener.getsockname()
        context = KMSConnectContext(host="::1", port=443, timeout=10)
        sock = await AsyncHTTPProxyKMSConnect(host, port)(context)
        self.addCleanup(sock.close)
        self.assertEqual(accepted[0].split(b"\r\n")[0], b"CONNECT [::1]:443 HTTP/1.1")

    async def test_oversized_connect_response_is_rejected(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def stub_proxy():
            conn = None
            try:
                conn, _ = listener.accept()
                conn.recv(4096)
                # Never sends the terminator.
                while True:
                    conn.sendall(b"x" * 1024)
            except OSError:
                pass
            finally:
                if conn is not None:
                    conn.close()

        threading.Thread(target=stub_proxy, daemon=True).start()

        host, port = listener.getsockname()
        context = KMSConnectContext(host="kms.example.com", port=443, timeout=10)
        with self.assertRaisesRegex(OSError, "oversized CONNECT response"):
            await AsyncHTTPProxyKMSConnect(host, port)(context)

    async def test_remaining_raises_once_the_deadline_passes(self):
        from pymongo.encryption_options import _remaining

        self.assertGreater(_remaining(time.monotonic() + 5), 0)
        with self.assertRaises(socket.timeout):
            _remaining(time.monotonic() - 1)

    async def test_datagram_socket_from_callback_is_rejected(self):
        # A connected UDP socket passes isinstance and getpeername, but TLS
        # then raises NotImplementedError, which would be retried.
        left = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        right = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        right.bind(("127.0.0.1", 0))
        left.connect(right.getsockname())

        async def callback(context):
            return left

        with self.assertRaisesRegex(ConfigurationError, "stream socket"):
            await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 10.0)

    async def test_kms_request_does_not_retry_a_contract_violation(self):
        # _connect_kms has no retry loop; the no-retry guarantee is in
        # kms_request, so exercise that instead.
        calls = []

        async def callback(context):
            calls.append(context)
            return "not-a-socket"

        opts = AutoEncryptionOpts({}, "k.d", kms_connect_callback=callback)
        io = _EncryptionIO(None, mock.MagicMock(), None, opts)

        class StubKmsContext:
            endpoint = "kms.example.com:443"
            message = b"request"
            kms_provider = "aws"
            usleep = 0
            bytes_needed = 1

            def feed(self, data):
                raise AssertionError("should not reach the socket")

            def fail(self):
                raise AssertionError("a contract violation must not be retried")

        with self.assertRaises(ConfigurationError):
            await io.kms_request(StubKmsContext())
        self.assertEqual(len(calls), 1)

    async def test_contract_violation_surfaces_as_encryption_error(self):
        # Public operations run under _wrap_encryption_errors, so callers see
        # EncryptionError with ConfigurationError as its cause.
        with self.assertRaises(EncryptionError) as caught:
            with _wrap_encryption_errors():
                raise ConfigurationError("kms_connect_callback must return ...")
        self.assertIsInstance(caught.exception.__cause__, ConfigurationError)

    async def test_bridge_failure_closes_the_proxy_socket(self):
        # A failure inside _bridge must not strand the connected proxy socket.
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(CLIENT_PEM)

        def stub_proxy():
            conn = None
            try:
                conn, _ = listener.accept()
                tls = server_ctx.wrap_socket(conn, server_side=True)
                tls.recv(4096)
                tls.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                tls.close()
            except OSError:
                pass
            finally:
                if conn is not None:
                    conn.close()

        threading.Thread(target=stub_proxy, daemon=True).start()

        captured = []

        def failing_bridge(self, proxy):
            captured.append(proxy)
            raise OSError("no file descriptors")

        host, port = listener.getsockname()
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        context = KMSConnectContext(host="kms.example.com", port=443, timeout=10)

        with mock.patch.object(HTTPProxyKMSConnect, "_bridge", failing_bridge):
            with self.assertRaisesRegex(OSError, "no file descriptors"):
                await AsyncHTTPProxyKMSConnect(host, port, ctx)(context)

        self.assertEqual(captured[0].fileno(), -1, "proxy socket was left open")

    async def test_network_error_from_callback_propagates(self):
        async def callback(context):
            raise OSError("proxy unreachable")

        # Not a ConfigurationError, so kms_request retries it.
        with self.assertRaises(OSError):
            await _connect_kms(("kms.example.com", 443), self._pool_options(), callback, 10.0)

    async def test_client_encryption_accepts_callback(self):
        async def callback(context):
            raise AssertionError("not called")

        client = self.simple_client()
        encryption = AsyncClientEncryption(
            {"local": {"key": b"\x00" * 96}},
            "keyvault.datakeys",
            client,
            OPTS,
            kms_connect_callback=callback,
        )
        self.addAsyncCleanup(encryption.close)
        self.assertIs(encryption._io_callbacks.opts._kms_connect_callback, callback)

    async def test_client_encryption_rejects_non_callable(self):
        client = self.simple_client()
        with self.assertRaisesRegex(TypeError, "kms_connect_callback must be callable"):
            AsyncClientEncryption(
                {"local": {"key": b"\x00" * 96}},
                "keyvault.datakeys",
                client,
                OPTS,
                kms_connect_callback="not-callable",  # type: ignore[arg-type]
            )


class TestKmsConnectCallbackProse(AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.callback_calls: list[Any] = []

    async def plain_callback(self, context):
        self.callback_calls.append(context)
        return await AsyncHTTPProxyKMSConnect(KMS_PROXY_HOST, KMS_PROXY_PORT)(context)

    async def tls_callback(self, context):
        self.callback_calls.append(context)
        ctx = ssl.create_default_context(cafile=CA_PEM)
        ctx.check_hostname = False
        # PYTHON-5040 tracks re-enabling verification once the test CA cert is
        # fixed; the evergreen-tools CA lacks an Authority Key Identifier that
        # newer OpenSSL requires, so verification fails on Windows 3.14.
        ctx.verify_mode = ssl.CERT_NONE
        callback = AsyncHTTPProxyKMSConnect(KMS_PROXY_HOST, KMS_TLS_PROXY_PORT, ctx)
        return await callback(context)

    async def proxy_request(self, method, path, tls=False):
        """Call the proxy's control endpoints and return the body."""
        if _IS_SYNC:
            return self._proxy_request(method, path, tls)
        return await asyncio.get_running_loop().run_in_executor(
            None, self._proxy_request, method, path, tls
        )

    def _proxy_request(self, method, path, tls=False):
        if tls:
            ctx = ssl.create_default_context(cafile=CA_PEM)
            ctx.check_hostname = False
            # PYTHON-5040 tracks re-enabling verification once the test CA cert
            # is fixed; the evergreen-tools CA lacks an Authority Key Identifier
            # that newer OpenSSL requires, so verification fails on Windows 3.14.
            ctx.verify_mode = ssl.CERT_NONE
            conn = http.client.HTTPSConnection(
                f"{KMS_PROXY_HOST}:{KMS_TLS_PROXY_PORT}", context=ctx
            )
        else:
            conn = http.client.HTTPConnection(f"{KMS_PROXY_HOST}:{KMS_PROXY_PORT}")
        try:
            conn.request(method, path)
            return conn.getresponse().read().decode()
        finally:
            conn.close()

    async def connect_count(self, tls=False):
        body = await self.proxy_request("GET", "/metrics", tls=tls)
        # One "key value" per line; the server also emits connect_target.
        for line in body.splitlines():
            key, _, value = line.partition(" ")
            if key == "connect_count":
                return int(value)
        raise AssertionError(f"no connect_count in metrics body: {body!r}")

    async def test_01_plain_http_proxy(self):
        await self.proxy_request("POST", "/reset")
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(await self.connect_count(), 1)

    async def test_02_https_proxy(self):
        await self.proxy_request("POST", "/reset", tls=True)
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.tls_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(await self.connect_count(tls=True), 1)

    async def test_03_auto_encryption_through_proxy(self):
        await self.client.keyvault.datakeys.drop()
        await self.client.db.coll.drop()

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        data_key_id = await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        schema = {
            "bsonType": "object",
            "properties": {
                "encrypted_string": {
                    "encrypt": {
                        "keyId": [data_key_id],
                        "bsonType": "string",
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    }
                }
            },
        }

        await self.proxy_request("POST", "/reset")
        opts = AutoEncryptionOpts(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            schema_map={"db.coll": schema},
            kms_connect_callback=self.plain_callback,
        )
        client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        await client_encrypted.db.coll.insert_one({"_id": 1, "encrypted_string": "hello"})
        decrypted = await client_encrypted.db.coll.find_one({"_id": 1})
        self.assertEqual(decrypted["encrypted_string"], "hello")

        raw = await self.client.db.coll.find_one({"_id": 1})
        self.assertIsInstance(raw["encrypted_string"], Binary)

        # The decrypt reuses the cached key, so exactly one KMS request follows
        # the reset.
        self.assertEqual(await self.connect_count(), 1)

    async def test_04_callback_error(self):
        async def failing_callback(context):
            raise OSError("proxy is on fire")

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=failing_callback,
        )
        with self.assertRaisesRegex(EncryptionError, "proxy is on fire"):
            await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)

    @unittest.skip(
        "PYTHON-6037 ClientEncryption does not support timeoutMS, so the "
        "callback always receives the default KMS connect timeout"
    )
    async def test_05_callback_receives_timeout(self):
        key_vault_client = await self.async_rs_or_single_client(timeoutMS=1000)
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            key_vault_client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)

        self.assertTrue(self.callback_calls, "callback was never invoked")
        for context in self.callback_calls:
            # Checks only the spec's non-zero requirement, which cannot fail.
            self.assertIsNotNone(context.timeout)
            self.assertGreater(context.timeout, 0)

    async def test_06_retry_after_network_error(self):
        state = {"calls": 0}

        async def flaky_callback(context):
            state["calls"] += 1
            if state["calls"] == 1:
                raise OSError("first attempt fails")
            return await AsyncHTTPProxyKMSConnect(KMS_PROXY_HOST, KMS_PROXY_PORT)(context)

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=flaky_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(state["calls"], 2)
