"""Tests for the KMS connect callback and HTTP proxy support."""

from __future__ import annotations

import asyncio
import dataclasses
import http.client
import os
import socket
import ssl
import threading
import time
import unittest
from asyncio.trsock import TransportSocket
from typing import Any
from unittest import mock

import pytest

import pymongo
from bson.binary import Binary
from pymongo.encryption_options import (
    AutoEncryptionOpts,
    HTTPProxyKMSConnect,
    KMSConnectContext,
    SyncHTTPProxyKMSConnect,
)
from pymongo.errors import ConfigurationError, ConnectionFailure, EncryptionError, NetworkTimeout
from pymongo.pool_options import PoolOptions
from pymongo.ssl_support import get_ssl_context
from pymongo.synchronous.encryption import (
    ClientEncryption,
    _connect_kms,
    _EncryptionIO,
    _wrap_encryption_errors,
)
from test import PyMongoTestCase
from test.helpers_shared import AWS_CREDS, CA_PEM, CERT_PATH, CLIENT_PEM
from test.test_encryption import OPTS, EncryptionIntegrationTest

_IS_SYNC = True

pytestmark = pytest.mark.encryption

_KMS_ADDRESS = ("kms.example.com", 443)

KMS_PROXY_HOST = "127.0.0.1"
KMS_PROXY_PORT = 9004
KMS_TLS_PROXY_PORT = 9005

AWS_MASTER_KEY = {
    "region": "us-east-1",
    "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
}


def _tls_server_context(cert=CLIENT_PEM):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert)
    return ctx


def _client_tls_context(verify=False):
    # verify=False matches the driver's test mode: the local certs don't verify.
    if verify:
        return get_ssl_context(None, None, CA_PEM, None, False, False, False, _IS_SYNC)
    return get_ssl_context(None, None, None, None, True, True, False, _IS_SYNC)


def _run_blocking(func, *args):
    """Run a blocking callable off the event loop (inline in the sync version)."""
    if _IS_SYNC:
        return func(*args)
    return asyncio.get_running_loop().run_in_executor(None, func, *args)


def _callback_returning(value):
    """A kms_connect_callback that always produces ``value``."""

    def callback(context):
        return value

    return callback


def _kms_context(host="kms.example.com", port=443, timeout=10):
    """A KMSConnectContext with the defaults used throughout these tests."""
    return KMSConnectContext(host=host, port=port, timeout=timeout)


def _read_http_request(conn):
    """Read until the blank line that ends a CONNECT request, or None on EOF."""
    request = b""
    while b"\r\n\r\n" not in request:
        chunk = conn.recv(4096)
        if not chunk:
            return None
        request += chunk
    return request


def _insecure_client_context():
    # PYTHON-5040 tracks re-enabling verification: the evergreen-tools CA
    # lacks an Authority Key Identifier newer OpenSSL requires.
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class TestKmsConnectCallbackUnit(PyMongoTestCase):
    """Contract checks for kms_connect_callback that need no KMS server."""

    @staticmethod
    def _pool_options(ssl_context=None):
        return PoolOptions(connect_timeout=10, socket_timeout=10, ssl_context=ssl_context)

    def _listen(self, backlog=1):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(backlog)
        self.addCleanup(listener.close)
        return listener

    def _socketpair(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        return left, right

    def _start_proxy(self, handler, backlog=1):
        """Serve each accepted connection with ``handler(conn)`` in a daemon thread."""
        listener = self._listen(backlog)

        def serve():
            for _ in range(backlog):
                try:
                    conn, _ = listener.accept()
                except OSError:
                    return
                try:
                    handler(conn)
                except OSError:
                    pass
                finally:
                    conn.close()

        threading.Thread(target=serve, daemon=True).start()
        return listener.getsockname()

    def _record_and_reply(self, accepted, reply):
        """A proxy that records each CONNECT request, replies ``reply``, and closes."""

        def handler(conn):
            request = _read_http_request(conn)
            if request is None:
                return
            accepted.append(request)
            conn.sendall(reply)

        return self._start_proxy(handler)

    def _tls_echo_proxy(self, delay=0):
        """A TLS CONNECT proxy that replies 200, then echoes one tunneled read."""
        server_ctx = _tls_server_context()

        def handler(conn):
            tls = server_ctx.wrap_socket(conn, server_side=True)
            request = _read_http_request(tls)
            if request is None:
                return
            tls.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            # The tunneled peer speaks only after the client does, as a TLS
            # server would; ``delay`` lets the reply outlast the CONNECT deadline.
            if delay:
                time.sleep(delay)
            tls.sendall(b"echo:" + tls.recv(64))
            tls.close()

        return self._start_proxy(handler)

    def _echo_over_tunnel(self, sock):
        sock.settimeout(10)
        _run_blocking(sock.sendall, b"ping")
        data = _run_blocking(sock.recv, 64)
        self.assertEqual(data, b"echo:ping")

    def test_init_kms_connect_callback(self):
        opts = AutoEncryptionOpts({}, "k.d")
        self.assertIsNone(opts._kms_connect_callback)

        def callback(context):
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

    def test_non_socket_return_raises_configuration_error(self):
        with self.assertRaisesRegex(ConfigurationError, "must return a connected"):
            _connect_kms(
                _KMS_ADDRESS, self._pool_options(), _callback_returning("not-a-socket"), 10.0
            )

    def test_already_wrapped_socket_is_rejected(self):
        # ssl.SSLSocket passes isinstance but cannot be TLS-wrapped again.
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        left, _right = self._socketpair()
        # No peer needed to produce a genuine ssl.SSLSocket.
        wrapped = ctx.wrap_socket(left, do_handshake_on_connect=False, server_hostname="x")
        self.addCleanup(wrapped.close)

        with self.assertRaisesRegex(ConfigurationError, "unwrapped"):
            _connect_kms(_KMS_ADDRESS, self._pool_options(), _callback_returning(wrapped), 10.0)

    def test_context_receives_host_port_and_timeout(self):
        received = []
        left, _right = self._socketpair()

        def callback(context):
            received.append(context)
            return left

        # ssl_context=None returns the socket unchanged, so a plain socket is accepted.
        conn = _connect_kms(_KMS_ADDRESS, self._pool_options(), callback, 12.5)
        self.assertIs(conn, left)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].host, "kms.example.com")
        self.assertEqual(received[0].port, 443)
        self.assertEqual(received[0].timeout, 12.5)

    def test_non_blocking_socket_from_callback_is_accepted(self):
        # Without the driver normalizing the mode, this raises ValueError.
        server_ctx = _tls_server_context()
        listener = self._listen()

        def serve():
            try:
                conn, _ = listener.accept()
                server_ctx.wrap_socket(conn, server_side=True).close()
            except OSError:
                pass

        threading.Thread(target=serve, daemon=True).start()

        options = self._pool_options(_client_tls_context())

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            sock.setblocking(False)
            return sock

        def callback(context):
            return _run_blocking(connect)

        conn = _connect_kms(listener.getsockname(), options, callback, 10.0)
        self.addCleanup(conn.close)
        self.assertIsNotNone(conn.gettimeout())

    def test_tls_verification_targets_the_kms_host(self):
        # The handshake must verify against the KMS address, not the peer the
        # callback connected to. The server cert covers 127.0.0.1 (the peer)
        # and localhost, but not the KMS hostname used below, so only
        # address-based verification produces this outcome.
        server_ctx = _tls_server_context(os.path.join(CERT_PATH, "server.pem"))
        listener = self._listen(2)

        def serve():
            for _ in range(2):
                try:
                    conn, _ = listener.accept()
                    server_ctx.wrap_socket(conn, server_side=True).close()
                except (OSError, ssl.SSLError):
                    # The mismatched-name attempt fails mid-handshake.
                    pass

        threading.Thread(target=serve, daemon=True).start()

        # Full verification: trusted CA, invalid certs and hostnames rejected.
        options = self._pool_options(_client_tls_context(verify=True))

        created = []

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            created.append(sock)
            return sock

        def callback(context):
            return _run_blocking(connect)

        port = listener.getsockname()[1]
        # The cert covers localhost: verifying against the KMS address succeeds.
        conn = _connect_kms(("localhost", port), options, callback, 10.0)
        self.addCleanup(conn.close)
        # TLS-wrapped in either SSL flavor: a new object, not the plain socket.
        self.assertIsNot(conn, created[0])
        # The cert does not cover this name: verification must fail even
        # though the peer (127.0.0.1) presents a cert valid for itself.
        with self.assertRaises(ConnectionFailure):
            _connect_kms(("kms.example.com", port), options, callback, 10.0)

    def test_asyncio_transport_socket_is_rejected(self):
        # get_extra_info("socket") is a TransportSocket, not a socket.socket.
        left, _right = self._socketpair()

        with self.assertRaisesRegex(ConfigurationError, "TransportSocket"):
            _connect_kms(
                _KMS_ADDRESS, self._pool_options(), _callback_returning(TransportSocket(left)), 10.0
            )

    def test_cancelled_tls_wrap_closes_late_socket(self):
        # A cancelled wrap can leave the executor producing an SSLSocket; the
        # done callback must close it.
        if _IS_SYNC:
            raise unittest.SkipTest("the cancel-safe wrap is an async path")
        from pymongo.pool_shared import _close_late_socket

        left, _right = self._socketpair()
        future = asyncio.get_running_loop().create_future()
        future.set_result(left)
        self.assertNotEqual(left.fileno(), -1)
        _close_late_socket(future)
        self.assertEqual(left.fileno(), -1)

    def test_http_proxy_helper_tunnels_and_reports_refusal(self):
        # Covers the CONNECT handshake without KMS credentials.
        accepted: list[bytes] = []
        context = _kms_context()

        host, port = self._record_and_reply(
            accepted, b"HTTP/1.1 200 Connection Established\r\n\r\n"
        )
        sock = SyncHTTPProxyKMSConnect(host, port)(context)
        self.addCleanup(sock.close)
        self.assertIsInstance(sock, socket.socket)
        self.assertEqual(accepted[0].split(b"\r\n")[0], b"CONNECT kms.example.com:443 HTTP/1.1")

        host, port = self._record_and_reply(
            accepted, b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"
        )
        with self.assertRaisesRegex(OSError, "refused CONNECT"):
            SyncHTTPProxyKMSConnect(host, port)(context)

        # Any 2xx status is a successful tunnel, not just HTTP/1.1 200.
        host, port = self._record_and_reply(
            accepted, b"HTTP/1.0 200 Connection Established\r\n\r\n"
        )
        sock = SyncHTTPProxyKMSConnect(host, port)(context)
        self.addCleanup(sock.close)
        self.assertIsInstance(sock, socket.socket)

        # A status code must be exactly three digits, with no zero padding.
        for reply in (b"HTTP/1.1 2000 Evil\r\n\r\n", b"HTTP/1.1 00200 Evil\r\n\r\n"):
            host, port = self._record_and_reply(accepted, reply)
            with self.assertRaisesRegex(OSError, "refused CONNECT"):
                SyncHTTPProxyKMSConnect(host, port)(context)

    def test_control_characters_in_kms_host_are_rejected(self):
        # Reject CR/LF in the configurable host before it reaches CONNECT.
        callback = SyncHTTPProxyKMSConnect("proxy.example.com", 8080)
        context = _kms_context(host="kms.example.com\r\nX-Injected: 1")
        with self.assertRaisesRegex(ConfigurationError, "control characters or whitespace"):
            callback(context)
        # Whitespace would split the request line into extra tokens.
        context = _kms_context(host="kms.example.com ")
        with self.assertRaisesRegex(ConfigurationError, "control characters or whitespace"):
            callback(context)

    def test_http_proxy_helper_sends_custom_headers(self):
        # Extra CONNECT headers reach the proxy verbatim.
        accepted: list[bytes] = []
        host, port = self._record_and_reply(
            accepted, b"HTTP/1.1 200 Connection Established\r\n\r\n"
        )
        headers = {"Proxy-Authorization": "Basic dXNlcjpwYXNz", "X-Trace-Id": "abc123"}
        sock = SyncHTTPProxyKMSConnect(host, port, headers=headers)(_kms_context())
        self.addCleanup(sock.close)
        request = accepted[0]
        self.assertEqual(request.split(b"\r\n")[0], b"CONNECT kms.example.com:443 HTTP/1.1")
        self.assertIn(b"\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n", request)
        self.assertIn(b"\r\nX-Trace-Id: abc123\r\n", request)
        self.assertEqual(request.count(b"\r\nHost: "), 1)

    def test_http_proxy_helper_authenticates_to_the_proxy(self):
        # The motivating case: 407 without credentials, 200 with them.
        def handler(conn):
            request = _read_http_request(conn)
            if request is None:
                return
            if b"\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n" in request:
                conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            else:
                conn.sendall(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")

        host, port = self._start_proxy(handler, backlog=2)
        context = _kms_context()
        with self.assertRaisesRegex(OSError, "refused CONNECT"):
            SyncHTTPProxyKMSConnect(host, port)(context)
        headers = {"Proxy-Authorization": "Basic dXNlcjpwYXNz"}
        sock = SyncHTTPProxyKMSConnect(host, port, headers=headers)(context)
        self.addCleanup(sock.close)
        self.assertIsInstance(sock, socket.socket)

    def test_http_proxy_helper_rejects_bad_headers(self):
        for headers in [
            {"Bad\r\nName": "x"},
            {"Bad Name": "x"},
            {"Bad\tName": "x"},
            {"X-Ok": "ok\r\nInjected: 1"},
            {"Host": "evil.example.com"},
            {"host": "evil.example.com"},
            {"": "x"},
            {"Bad:Name": "x"},
        ]:
            with self.assertRaisesRegex(ConfigurationError, "proxy header|Host CONNECT header"):
                SyncHTTPProxyKMSConnect("proxy.example.com", 8080, headers=headers)

        for headers in [{1: "x"}, {"X-Ok": 1}, {None: "x"}, {"X-Ok": None}]:
            with self.assertRaisesRegex(TypeError, "must be strings"):
                SyncHTTPProxyKMSConnect("proxy.example.com", 8080, headers=headers)

    def test_http_proxy_helper_accepts_legal_header_values(self):
        # Colons and spaces are legal in values (e.g. auth schemes); only
        # CR/LF would let a value inject a request line.
        callback = SyncHTTPProxyKMSConnect(
            "proxy.example.com",
            8080,
            headers={"Proxy-Authorization": "Basic dXNlcjpwYXNz", "X-Token": "a: b"},
        )
        self.assertEqual(
            callback.headers,
            {"Proxy-Authorization": "Basic dXNlcjpwYXNz", "X-Token": "a: b"},
        )

    def test_tls_proxy_helper_bridges_the_tunnel(self):
        # Covers the TLS-proxy path and the socketpair relay without KMS creds.
        host, port = self._tls_echo_proxy()
        sock = HTTPProxyKMSConnect(host, port, _insecure_client_context())(_kms_context())
        self.addCleanup(sock.close)
        self._echo_over_tunnel(sock)

    def test_bridge_does_not_inherit_the_connect_deadline(self):
        # The relay must outlast the much shorter CONNECT deadline.
        host, port = self._tls_echo_proxy(delay=3.0)
        sock = HTTPProxyKMSConnect(host, port, _insecure_client_context())(
            _kms_context(timeout=2.0)
        )
        self.addCleanup(sock.close)
        self._echo_over_tunnel(sock)

    def test_proxy_closing_before_connect_reply_raises(self):
        def handler(conn):
            # Read the CONNECT request, then hang up without replying.
            conn.recv(4096)

        host, port = self._start_proxy(handler)
        with self.assertRaisesRegex(OSError, "proxy closed the connection"):
            SyncHTTPProxyKMSConnect(host, port)(_kms_context())

    def test_cancelled_proxy_connect_closes_the_late_socket(self):
        # A cancelled connect must close the socket the executor thread
        # produces after the cancellation.
        if _IS_SYNC:
            raise unittest.SkipTest("cancellation is an async-only behavior")

        requested = threading.Event()
        reply = threading.Event()

        def handler(conn):
            conn.recv(4096)
            requested.set()
            if not reply.wait(10):
                return
            conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            # Keep the connection open so the tunnel can complete its reads.
            time.sleep(0.1)

        host, port = self._start_proxy(handler)

        tunneled: list[socket.socket] = []
        original_tunnel = HTTPProxyKMSConnect._tunnel

        def spy_tunnel(self, sock, context, deadline):
            tunneled.append(sock)
            original_tunnel(self, sock, context, deadline)

        with mock.patch.object(HTTPProxyKMSConnect, "_tunnel", spy_tunnel):
            task = asyncio.create_task(SyncHTTPProxyKMSConnect(host, port)(_kms_context()))
            waited = _run_blocking(requested.wait, 10)
            self.assertTrue(waited, "proxy never received the CONNECT request")
            task.cancel("no longer needed")
            with self.assertRaises(asyncio.CancelledError):
                task
            # Let the stub reply, completing the executor's future late.
            reply.set()
            time.sleep(0.5)

        self.assertEqual(len(tunneled), 1)
        self.assertEqual(tunneled[0].fileno(), -1, "late socket was left open")

    def test_connect_timeout_is_not_reclassified(self):
        # A connect that times out keeps its socket.timeout type instead of
        # being reported as a generic connect error.
        def timeout_connect(self, address):
            raise socket.timeout("timed out")

        with mock.patch.object(socket.socket, "connect", timeout_connect):
            with self.assertRaises(socket.timeout):
                HTTPProxyKMSConnect("127.0.0.1", 9999)._connect_proxy(time.monotonic() + 10)

    def test_tunnel_keeps_bytes_sent_with_the_connect_reply(self):
        # A proxy may coalesce its 200 with tunneled bytes; reading past the header would drop them.
        def handler(conn):
            conn.recv(4096)
            conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\nearly-bytes")

        host, port = self._start_proxy(handler)
        sock = SyncHTTPProxyKMSConnect(host, port)(_kms_context())
        self.addCleanup(sock.close)
        sock.settimeout(10)
        data = _run_blocking(sock.recv, 64)
        self.assertEqual(data, b"early-bytes")

    def test_ipv6_host_is_bracketed_in_connect(self):
        accepted: list[bytes] = []
        host, port = self._record_and_reply(
            accepted, b"HTTP/1.1 200 Connection Established\r\n\r\n"
        )
        sock = SyncHTTPProxyKMSConnect(host, port)(_kms_context(host="::1"))
        self.addCleanup(sock.close)
        self.assertEqual(accepted[0].split(b"\r\n")[0], b"CONNECT [::1]:443 HTTP/1.1")

    def test_oversized_connect_response_is_rejected(self):
        def handler(conn):
            conn.recv(4096)
            # Never sends the terminator.
            while True:
                conn.sendall(b"x" * 1024)

        host, port = self._start_proxy(handler)
        with self.assertRaisesRegex(OSError, "oversized CONNECT response"):
            SyncHTTPProxyKMSConnect(host, port)(_kms_context())

    def test_remaining_raises_once_the_deadline_passes(self):
        from pymongo.encryption_options import _remaining

        self.assertGreater(_remaining(time.monotonic() + 5), 0)
        with self.assertRaises(socket.timeout):
            _remaining(time.monotonic() - 1)

    def test_bridge_failure_closes_the_proxy_socket(self):
        # A failure inside _bridge must not strand the connected proxy socket.
        server_ctx = _tls_server_context()

        def handler(conn):
            tls = server_ctx.wrap_socket(conn, server_side=True)
            tls.recv(4096)
            tls.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            tls.close()

        captured = []

        def failing_bridge(self, proxy):
            captured.append(proxy)
            raise OSError("no file descriptors")

        host, port = self._start_proxy(handler)
        context = _kms_context()

        with mock.patch.object(HTTPProxyKMSConnect, "_bridge", failing_bridge):
            with self.assertRaisesRegex(OSError, "no file descriptors"):
                HTTPProxyKMSConnect(host, port, _insecure_client_context())(context)

        self.assertEqual(captured[0].fileno(), -1, "proxy socket was left open")

    def test_non_coroutine_callback_is_rejected(self):
        # A plain def must be rejected before it blocks the event loop.
        if _IS_SYNC:
            raise unittest.SkipTest("a regular function is correct for the sync API")

        entered = []

        def callback(context):
            entered.append(context)
            return None

        with self.assertRaisesRegex(ConfigurationError, "coroutine function"):
            _connect_kms(_KMS_ADDRESS, self._pool_options(), callback, 10.0)
        self.assertEqual(entered, [], "invalid callback must not be entered")

    def test_unconnected_socket_from_callback_is_rejected(self):
        # An unconnected socket would fail later as a transient error and be retried.
        bare = socket.socket()
        self.addCleanup(bare.close)

        with self.assertRaisesRegex(ConfigurationError, "already connected"):
            _connect_kms(_KMS_ADDRESS, self._pool_options(), _callback_returning(bare), 10.0)

    def test_datagram_socket_from_callback_is_rejected(self):
        # TLS on a connected UDP socket raises NotImplementedError, which would be retried.
        left = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        right = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        right.bind(("127.0.0.1", 0))
        left.connect(right.getsockname())

        with self.assertRaisesRegex(ConfigurationError, "stream socket"):
            _connect_kms(_KMS_ADDRESS, self._pool_options(), _callback_returning(left), 10.0)

    def test_kms_request_does_not_retry_a_contract_violation(self):
        # _connect_kms has no retry loop; the no-retry guarantee is in
        # kms_request, so exercise that instead.
        calls = []

        def callback(context):
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
            io.kms_request(StubKmsContext())
        self.assertEqual(len(calls), 1)

    def test_contract_violation_surfaces_as_encryption_error(self):
        # Callers see EncryptionError with ConfigurationError as its cause.
        with self.assertRaises(EncryptionError) as caught:
            with _wrap_encryption_errors():
                raise ConfigurationError("kms_connect_callback must return ...")
        self.assertIsInstance(caught.exception.__cause__, ConfigurationError)

    def test_network_error_from_callback_propagates(self):
        def callback(context):
            raise OSError("proxy unreachable")

        # Not a ConfigurationError, so kms_request retries it.
        with self.assertRaises(OSError):
            _connect_kms(_KMS_ADDRESS, self._pool_options(), callback, 10.0)

    def test_csot_deadline_stops_a_hung_callback(self):
        # A callback that ignores the timeout cannot block past the CSOT
        # deadline, and a socket it yields later must be closed.
        if _IS_SYNC:
            raise unittest.SkipTest("the sync API cannot interrupt a callback")

        left, _right = self._socketpair()

        def hung_callback(context):
            time.sleep(0.5)
            return left

        with self.assertRaises(NetworkTimeout):
            with pymongo.timeout(0.1):
                _connect_kms(_KMS_ADDRESS, self._pool_options(), hung_callback, 10.0)
        self.assertNotEqual(left.fileno(), -1)
        # Let the shielded callback finish; the driver closes the late result.
        time.sleep(0.75)
        self.assertEqual(left.fileno(), -1)

    def test_cancelling_kms_connect_closes_the_callback_socket(self):
        # Cancelling during the TLS handshake must close the callback's socket,
        # so a TLS proxy's relay threads wind down.
        if _IS_SYNC:
            raise unittest.SkipTest("cancellation is an async-only behavior")

        server_ctx = _tls_server_context()
        listener = self._listen()
        gate = threading.Event()
        eof = threading.Event()

        def stub_server():
            conn = None
            try:
                conn, _ = listener.accept()
                # The cancel may land before or after the executor starts the
                # handshake. Peek for the ClientHello without consuming it, or
                # for EOF if the driver closed it, before wrap_socket detaches conn.
                while True:
                    data = conn.recv(4096, socket.MSG_PEEK)
                    if not data:
                        eof.set()
                        return
                    if data[:1] == b"\x16":  # TLS handshake record
                        break
                # Hold the handshake open until the test has cancelled.
                if not gate.wait(5):
                    return
                tls = server_ctx.wrap_socket(conn, server_side=True, do_handshake_on_connect=False)
                try:
                    tls.do_handshake()
                    # A discarded connection may end in a reset rather than a
                    # clean EOF; either proves the driver closed it.
                    while tls.recv(4096):
                        pass
                except OSError:
                    pass
                eof.set()
                tls.close()
            except OSError:
                pass
            finally:
                if conn is not None:
                    conn.close()

        threading.Thread(target=stub_server, daemon=True).start()

        options = self._pool_options(_client_tls_context())
        socks = []

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            socks.append(sock)
            return sock

        def callback(context):
            return _run_blocking(connect)

        # The sync version returns a socket instead of a coroutine, so both
        # error codes are needed depending on the version being checked.
        pending = _connect_kms(listener.getsockname(), options, callback, 10.0)
        task = asyncio.ensure_future(pending)  # type: ignore[type-var,arg-type]
        for _ in range(100):
            if socks:
                break
            time.sleep(0.01)
        self.assertTrue(socks, "callback was never invoked")
        # Bias the cancel to land mid-handshake; the stub handles the earlier
        # window too.
        time.sleep(0.1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            task
        # The late SSLSocket (or raw socket) must be closed; the stub sees EOF.
        gate.set()
        for _ in range(50):
            if eof.is_set():
                break
            time.sleep(0.1)
        self.assertTrue(eof.is_set(), "driver never closed the callback socket")

    def test_client_encryption_accepts_callback(self):
        def callback(context):
            raise AssertionError("not called")

        client = self.simple_client()
        encryption = ClientEncryption(
            {"local": {"key": b"\x00" * 96}},
            "keyvault.datakeys",
            client,
            OPTS,
            kms_connect_callback=callback,
        )
        self.addCleanup(encryption.close)
        self.assertIs(encryption._io_callbacks.opts._kms_connect_callback, callback)

    def test_client_encryption_rejects_non_callable(self):
        client = self.simple_client()
        with self.assertRaisesRegex(TypeError, "kms_connect_callback must be callable"):
            ClientEncryption(
                {"local": {"key": b"\x00" * 96}},
                "keyvault.datakeys",
                client,
                OPTS,
                kms_connect_callback="not-callable",  # type: ignore[arg-type]
            )


class TestKmsConnectCallbackProse(EncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def setUp(self):
        super().setUp()
        self.callback_calls: list[Any] = []

    def plain_callback(self, context):
        self.callback_calls.append(context)
        return SyncHTTPProxyKMSConnect(KMS_PROXY_HOST, KMS_PROXY_PORT)(context)

    def _proxy_tls_context(self):
        ctx = ssl.create_default_context(cafile=CA_PEM)
        ctx.check_hostname = False
        # PYTHON-5040 tracks re-enabling verification once the test CA cert
        # is fixed; the evergreen-tools CA lacks an Authority Key Identifier
        # that newer OpenSSL requires, so verification fails on Windows 3.14.
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def tls_callback(self, context):
        self.callback_calls.append(context)
        callback = SyncHTTPProxyKMSConnect(
            KMS_PROXY_HOST, KMS_TLS_PROXY_PORT, self._proxy_tls_context()
        )
        return callback(context)

    def proxy_request(self, method, path, tls=False):
        """Call the proxy's control endpoints and return the body."""
        if _IS_SYNC:
            return self._proxy_request(method, path, tls)
        return asyncio.get_running_loop().run_in_executor(
            None, self._proxy_request, method, path, tls
        )

    def _proxy_request(self, method, path, tls=False):
        if tls:
            conn = http.client.HTTPSConnection(
                f"{KMS_PROXY_HOST}:{KMS_TLS_PROXY_PORT}", context=self._proxy_tls_context()
            )
        else:
            conn = http.client.HTTPConnection(f"{KMS_PROXY_HOST}:{KMS_PROXY_PORT}")
        try:
            conn.request(method, path)
            return conn.getresponse().read().decode()
        finally:
            conn.close()

    def connect_count(self, tls=False):
        body = self.proxy_request("GET", "/metrics", tls=tls)
        # One "key value" per line; the server also emits connect_target.
        for line in body.splitlines():
            key, _, value = line.partition(" ")
            if key == "connect_count":
                return int(value)
        raise AssertionError(f"no connect_count in metrics body: {body!r}")

    def test_01_plain_http_proxy(self):
        self.proxy_request("POST", "/reset")
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(self.connect_count(), 1)

    def test_02_https_proxy(self):
        self.proxy_request("POST", "/reset", tls=True)
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.tls_callback,
        )
        encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(self.connect_count(tls=True), 1)

    def test_03_auto_encryption_through_proxy(self):
        self.client.keyvault.datakeys.drop()
        self.client.db.coll.drop()

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        data_key_id = encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
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

        self.proxy_request("POST", "/reset")
        opts = AutoEncryptionOpts(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            schema_map={"db.coll": schema},
            kms_connect_callback=self.plain_callback,
        )
        client_encrypted = self.rs_or_single_client(auto_encryption_opts=opts)

        client_encrypted.db.coll.insert_one({"_id": 1, "encrypted_string": "hello"})
        decrypted = client_encrypted.db.coll.find_one({"_id": 1})
        self.assertEqual(decrypted["encrypted_string"], "hello")

        raw = self.client.db.coll.find_one({"_id": 1})
        self.assertIsInstance(raw["encrypted_string"], Binary)

        # The decrypt reuses the cached key, so exactly one KMS request follows
        # the reset.
        self.assertEqual(self.connect_count(), 1)

    def test_04_callback_error(self):
        def failing_callback(context):
            raise OSError("proxy is on fire")

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=failing_callback,
        )
        with self.assertRaisesRegex(EncryptionError, "proxy is on fire"):
            encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)

    @unittest.skip(
        "PYTHON-6037 ClientEncryption does not support timeoutMS, so the "
        "callback always receives the default KMS connect timeout"
    )
    def test_05_callback_receives_timeout(self):
        key_vault_client = self.rs_or_single_client(timeoutMS=1000)
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            key_vault_client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)

        self.assertTrue(self.callback_calls, "callback was never invoked")
        for context in self.callback_calls:
            # Checks only the spec's non-zero requirement, which cannot fail.
            self.assertIsNotNone(context.timeout)
            self.assertGreater(context.timeout, 0)

    def test_06_retry_after_network_error(self):
        state = {"calls": 0}

        def flaky_callback(context):
            state["calls"] += 1
            if state["calls"] == 1:
                raise OSError("first attempt fails")
            return SyncHTTPProxyKMSConnect(KMS_PROXY_HOST, KMS_PROXY_PORT)(context)

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=flaky_callback,
        )
        encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(state["calls"], 2)
