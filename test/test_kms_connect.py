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

"""Unit tests for the KMS connect callback.

This file is not processed by synchro: the tests are written once and
parameterized over both the asynchronous and synchronous APIs, selected with
the ``[async]``/``[sync]`` parametrize ids. Every test runs as a coroutine on
a pytest-asyncio loop. The ``[sync]`` variants call the blocking synchronous
APIs from within the coroutine, which is harmless for these self-contained
tests. The ``flavor`` parameter provides the per-API seams.

The tests must run single threaded under thread based parallelization such as
pytest-run-parallel. pytest-asyncio does not support the plugin's concurrent
replicas of one test, and the tests spin up real sockets and threads that
every replica would race on.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import socket
import ssl
import threading
from asyncio.trsock import TransportSocket
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any
from unittest import mock

import pytest

import pymongo
from bson.codec_options import CodecOptions
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT, AutoEncryptionOpts, KMSConnectContext
from pymongo.errors import ConfigurationError, ConnectionFailure, EncryptionError, NetworkTimeout
from pymongo.pool_options import PoolOptions
from pymongo.ssl_support import get_ssl_context
from test.helpers_shared import CA_PEM, CERT_PATH, CLIENT_PEM

pytestmark = [pytest.mark.encryption, pytest.mark.asyncio]

_KMS_ADDRESS = ("kms.example.com", 443)

OPTS = CodecOptions()


class Flavor:
    """The per-API seams, shared by the tests through the ``flavor`` parameter."""

    def __init__(self, is_async: bool) -> None:
        self.is_async = is_async

    async def maybe_await(self, result: Any) -> Any:
        """Await ``result`` in the asynchronous flavor (a no-op otherwise)."""
        if self.is_async:
            return await result
        return result

    async def offload(self, func: Callable[..., Any], *args: Any) -> Any:
        """Run a blocking callable off the event loop (inline when synchronous)."""
        if self.is_async:
            return await asyncio.get_running_loop().run_in_executor(None, func, *args)
        return func(*args)

    def encryption(self):
        """The flavor's ``encryption`` module."""
        if self.is_async:
            from pymongo.asynchronous import encryption
        else:
            from pymongo.synchronous import encryption
        return encryption

    def kms_connect(self):
        """The flavor's ``_kms_connect`` module."""
        if self.is_async:
            from pymongo.asynchronous import _kms_connect
        else:
            from pymongo.synchronous import _kms_connect
        return _kms_connect

    async def connect(self, address, pool_options, callback, timeout):
        """``_connect_kms`` for this flavor."""
        module = self.kms_connect()
        if self.is_async:
            return await module._connect_kms(address, pool_options, callback, timeout)
        return module._connect_kms(address, pool_options, callback, timeout)

    def callback(self, func):
        """Adapt a non-blocking ``func(context)`` to the flavor's callback form."""
        if self.is_async:

            async def callback(context):
                return func(context)

            return callback

        return func

    def blocking_callback(self, func):
        """Adapt a blocking ``func(context)``, offloaded in the async flavor."""
        if self.is_async:

            async def callback(context):
                return await asyncio.get_running_loop().run_in_executor(None, func, context)

            return callback

        return func

    def callback_returning(self, value):
        """A kms_connect_callback that always produces ``value``."""
        return self.callback(lambda context: value)

    def client_tls_context(self, verify=False):
        # verify=False matches the driver's test mode: the local certs don't verify.
        if verify:
            return get_ssl_context(None, None, CA_PEM, None, False, False, False, not self.is_async)
        return get_ssl_context(None, None, None, None, True, True, False, not self.is_async)

    def client_encryption(self, kms_providers, key_vault_namespace, client, kms_connect_callback):
        """A ClientEncryption for this flavor using a local key provider."""
        if self.is_async:
            from pymongo.asynchronous.encryption import AsyncClientEncryption

            return AsyncClientEncryption(
                kms_providers,
                key_vault_namespace,
                client,
                OPTS,
                kms_connect_callback=kms_connect_callback,
            )
        from pymongo.synchronous.encryption import ClientEncryption

        return ClientEncryption(
            kms_providers,
            key_vault_namespace,
            client,
            OPTS,
            kms_connect_callback=kms_connect_callback,
        )

    def simple_client(self):
        """A lazily-connecting client for this flavor."""
        if self.is_async:
            from pymongo.asynchronous.mongo_client import AsyncMongoClient

            return AsyncMongoClient()
        from pymongo import MongoClient

        return MongoClient()


ASYNC = Flavor(is_async=True)
SYNC = Flavor(is_async=False)

both_flavors = pytest.mark.parametrize("flavor", [ASYNC, SYNC], ids=["async", "sync"])
async_only = pytest.mark.parametrize("flavor", [ASYNC], ids=["async"])


def _pool_options(ssl_context=None):
    return PoolOptions(connect_timeout=10, socket_timeout=10, ssl_context=ssl_context)


def _tls_server_context(cert=CLIENT_PEM):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert)
    return ctx


@contextmanager
def _listen(backlog=1):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(backlog)
    try:
        yield listener
    finally:
        listener.close()


@contextmanager
def _socketpair():
    left, right = socket.socketpair()
    try:
        yield left, right
    finally:
        left.close()
        right.close()


@both_flavors
async def test_init_kms_connect_callback(flavor):
    opts = AutoEncryptionOpts({}, "k.d")
    assert opts._kms_connect_callback is None

    def action(context):
        raise AssertionError("not called")

    callback = flavor.callback(action)
    opts = AutoEncryptionOpts({}, "k.d", kms_connect_callback=callback)
    assert opts._kms_connect_callback is callback

    for bad in [1, "not-callable", object()]:
        with pytest.raises(TypeError, match="kms_connect_callback must be callable"):
            AutoEncryptionOpts({}, "k.d", kms_connect_callback=bad)  # type: ignore[arg-type]

    context = KMSConnectContext(host="kms.example.com", port=443, timeout=9.5)
    assert context.host == "kms.example.com"
    assert context.port == 443
    assert context.timeout == 9.5
    with pytest.raises(dataclasses.FrozenInstanceError):
        context.host = "evil.example.com"  # type: ignore[misc]


@both_flavors
async def test_non_socket_return_raises_configuration_error(flavor):
    with pytest.raises(ConfigurationError, match="must return a connected"):
        await flavor.connect(
            _KMS_ADDRESS, _pool_options(), flavor.callback_returning("not-a-socket"), 10.0
        )


@both_flavors
async def test_already_wrapped_socket_is_rejected(flavor):
    # ssl.SSLSocket passes isinstance but cannot be TLS-wrapped again.
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with _socketpair() as (left, _right):
        # No peer needed to produce a genuine ssl.SSLSocket. The driver closes
        # the wrapped socket when rejecting it.
        wrapped = ctx.wrap_socket(left, do_handshake_on_connect=False, server_hostname="x")
        with pytest.raises(ConfigurationError, match="unwrapped"):
            await flavor.connect(
                _KMS_ADDRESS, _pool_options(), flavor.callback_returning(wrapped), 10.0
            )


@both_flavors
async def test_context_receives_host_port_and_timeout(flavor):
    received = []
    with _socketpair() as (left, _right):

        def action(context):
            received.append(context)
            return left

        # ssl_context=None returns the socket unchanged, so a plain socket is accepted.
        conn = await flavor.connect(_KMS_ADDRESS, _pool_options(), flavor.callback(action), 12.5)
        assert conn is left

        assert len(received) == 1
        assert received[0].host == "kms.example.com"
        assert received[0].port == 443
        assert received[0].timeout == 12.5


@both_flavors
async def test_non_blocking_socket_from_callback_is_accepted(flavor):
    # Without the driver normalizing the mode, this raises ValueError.
    server_ctx = _tls_server_context()
    with _listen() as listener:

        def serve():
            try:
                conn, _ = listener.accept()
                server_ctx.wrap_socket(conn, server_side=True).close()
            except OSError:
                pass

        threading.Thread(target=serve, daemon=True).start()

        options = _pool_options(flavor.client_tls_context())

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            sock.setblocking(False)
            return sock

        conn = await flavor.connect(
            listener.getsockname(),
            options,
            flavor.blocking_callback(lambda context: connect()),
            10.0,
        )
        try:
            assert conn.gettimeout() is not None
        finally:
            conn.close()


@both_flavors
async def test_tls_verification_targets_the_kms_host(flavor):
    # The handshake must verify against the KMS address, not the peer the
    # callback connected to. The server cert covers 127.0.0.1 (the peer)
    # and localhost, but not the KMS hostname used below, so only
    # address-based verification produces this outcome.
    server_ctx = _tls_server_context(os.path.join(CERT_PATH, "server.pem"))
    with _listen(2) as listener:

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
        options = _pool_options(flavor.client_tls_context(verify=True))

        created = []

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            created.append(sock)
            return sock

        port = listener.getsockname()[1]
        # The cert covers localhost: verifying against the KMS address succeeds.
        conn = await flavor.connect(
            ("localhost", port),
            options,
            flavor.blocking_callback(lambda context: connect()),
            10.0,
        )
        try:
            # TLS-wrapped in either SSL flavor: a new object, not the plain socket.
            assert conn is not created[0]
        finally:
            conn.close()
        # The cert does not cover this name: verification must fail even
        # though the peer (127.0.0.1) presents a cert valid for itself.
        with pytest.raises(ConnectionFailure):
            await flavor.connect(
                ("kms.example.com", port),
                options,
                flavor.blocking_callback(lambda context: connect()),
                10.0,
            )


@both_flavors
async def test_asyncio_transport_socket_is_rejected(flavor):
    # get_extra_info("socket") is a TransportSocket, not a socket.socket.
    with _socketpair() as (left, _right):
        with pytest.raises(ConfigurationError, match="TransportSocket"):
            await flavor.connect(
                _KMS_ADDRESS,
                _pool_options(),
                flavor.callback_returning(TransportSocket(left)),
                10.0,
            )


@async_only
async def test_cancelled_tls_wrap_closes_late_socket(flavor):
    # A cancelled wrap can leave the executor producing an SSLSocket. The
    # done callback must close it.
    from pymongo.pool_shared import _close_late_socket

    with _socketpair() as (left, _right):
        future = asyncio.get_running_loop().create_future()
        future.set_result(left)
        assert left.fileno() != -1
        _close_late_socket(future)
        assert left.fileno() == -1


@async_only
async def test_non_coroutine_callback_is_rejected(flavor):
    # A plain def must be rejected before it blocks the event loop.
    entered = []

    def callback(context):
        entered.append(context)
        return None

    with pytest.raises(ConfigurationError, match="coroutine function"):
        await flavor.connect(_KMS_ADDRESS, _pool_options(), callback, 10.0)
    assert entered == [], "invalid callback must not be entered"


@both_flavors
async def test_unconnected_socket_from_callback_is_rejected(flavor):
    # An unconnected socket would fail later as a transient error and be retried.
    with socket.socket() as bare:
        with pytest.raises(ConfigurationError, match="already connected"):
            await flavor.connect(
                _KMS_ADDRESS, _pool_options(), flavor.callback_returning(bare), 10.0
            )


@both_flavors
async def test_datagram_socket_from_callback_is_rejected(flavor):
    # TLS on a connected UDP socket raises NotImplementedError, which would be retried.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as left:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as right:
            right.bind(("127.0.0.1", 0))
            left.connect(right.getsockname())

            with pytest.raises(ConfigurationError, match="stream socket"):
                await flavor.connect(
                    _KMS_ADDRESS, _pool_options(), flavor.callback_returning(left), 10.0
                )


@both_flavors
async def test_kms_request_does_not_retry_a_contract_violation(flavor):
    # _connect_kms has no retry loop. The no-retry guarantee is in
    # kms_request, so exercise that instead.
    calls = []

    def action(context):
        calls.append(context)
        return "not-a-socket"

    opts = AutoEncryptionOpts({}, "k.d", kms_connect_callback=flavor.callback(action))
    io = flavor.encryption()._EncryptionIO(None, mock.MagicMock(), None, opts)

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

    with pytest.raises(ConfigurationError):
        await flavor.maybe_await(io.kms_request(StubKmsContext()))
    assert len(calls) == 1


@both_flavors
async def test_contract_violation_surfaces_as_encryption_error(flavor):
    # Callers see EncryptionError with ConfigurationError as its cause.
    with pytest.raises(EncryptionError) as exc_info:
        with flavor.encryption()._wrap_encryption_errors():
            raise ConfigurationError("kms_connect_callback must return ...")
    assert isinstance(exc_info.value.__cause__, ConfigurationError)


@both_flavors
async def test_network_error_from_callback_propagates(flavor):
    def action(context):
        raise OSError("proxy unreachable")

    # Not a ConfigurationError, so kms_request retries it.
    with pytest.raises(OSError):
        await flavor.connect(_KMS_ADDRESS, _pool_options(), flavor.callback(action), 10.0)


@async_only
async def test_csot_deadline_stops_a_hung_callback(flavor):
    # A callback that ignores the timeout cannot block past the CSOT
    # deadline, and a socket it yields later must be closed.
    with _socketpair() as (left, _right):

        async def hung_callback(context):
            await asyncio.sleep(0.5)
            return left

        with pytest.raises(NetworkTimeout):
            with pymongo.timeout(0.1):
                await flavor.connect(_KMS_ADDRESS, _pool_options(), hung_callback, 10.0)
        assert left.fileno() != -1
        # Let the shielded callback finish. The driver closes the late result.
        await asyncio.sleep(0.75)
        assert left.fileno() == -1


@async_only
async def test_cancelling_kms_connect_closes_the_callback_socket(flavor):
    # Cancelling during the TLS handshake must close the callback's socket,
    # so a TLS proxy's relay threads wind down.
    server_ctx = _tls_server_context()
    gate = threading.Event()
    eof = threading.Event()

    def stub_server(listener):
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
                # clean EOF. Either outcome proves the driver closed it.
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

    with _listen() as listener:
        threading.Thread(target=stub_server, args=(listener,), daemon=True).start()

        options = _pool_options(flavor.client_tls_context())
        socks = []

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            socks.append(sock)
            return sock

        # Schedule the connect now and cancel it once the callback socket
        # exists, so the cancellation lands mid-handshake.
        task = asyncio.ensure_future(
            flavor.connect(
                listener.getsockname(),
                options,
                flavor.blocking_callback(lambda context: connect()),
                10.0,
            )
        )
        for _ in range(100):
            if socks:
                break
            await asyncio.sleep(0.01)
        assert socks, "callback was never invoked"
        # Bias the cancel to land mid-handshake. The stub handles the earlier
        # window too.
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The late SSLSocket (or raw socket) must be closed. The stub sees EOF.
        gate.set()
        for _ in range(50):
            if eof.is_set():
                break
            await asyncio.sleep(0.1)
        assert eof.is_set(), "driver never closed the callback socket"


@pytest.mark.skipif(not _HAVE_PYMONGOCRYPT, reason="pymongocrypt is not installed")
@both_flavors
async def test_client_encryption_accepts_callback(flavor):
    def action(context):
        raise AssertionError("not called")

    callback = flavor.callback(action)
    client = flavor.simple_client()
    encryption = flavor.client_encryption(
        {"local": {"key": b"\x00" * 96}}, "keyvault.datakeys", client, callback
    )
    try:
        assert encryption._io_callbacks.opts._kms_connect_callback is callback
    finally:
        await flavor.maybe_await(encryption.close())
        await flavor.maybe_await(client.close())


@pytest.mark.skipif(not _HAVE_PYMONGOCRYPT, reason="pymongocrypt is not installed")
@both_flavors
async def test_client_encryption_rejects_non_callable(flavor):
    client = flavor.simple_client()
    with pytest.raises(TypeError, match="kms_connect_callback must be callable"):
        flavor.client_encryption(
            {"local": {"key": b"\x00" * 96}},
            "keyvault.datakeys",
            client,
            "not-callable",
        )
    await flavor.maybe_await(client.close())
