"""Tests for the KMS connect callback."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import socket
import ssl
import threading
import time
import unittest
from asyncio.trsock import TransportSocket
from unittest import mock

import pytest

import pymongo
from pymongo.encryption_options import (
    AutoEncryptionOpts,
    KMSConnectContext,
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
from test.helpers_shared import CA_PEM, CERT_PATH, CLIENT_PEM
from test.test_encryption import OPTS

_IS_SYNC = True

pytestmark = pytest.mark.encryption

_KMS_ADDRESS = ("kms.example.com", 443)


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
