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

Not processed by synchro: the tests are written once and parameterized over
the asynchronous and synchronous APIs with the ``[async]``/``[sync]`` ids.
Every test runs as a coroutine on a pytest-asyncio loop; the ``[sync]``
variants call the blocking synchronous APIs inline, which is harmless for
these self-contained tests. The ``api`` parameter provides the per-API
accessors.

The tests must run single threaded under thread based parallelization such
as pytest-run-parallel: pytest-asyncio does not support concurrent replicas,
and the tests spin up real sockets and threads that replicas would race on.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import os
import socket
import ssl
import threading
import time
from asyncio.trsock import TransportSocket
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any
from unittest import mock

import pytest

import pymongo
from bson.codec_options import CodecOptions
from pymongo.encryption_options import (
    _HAVE_PYMONGOCRYPT,
    AsyncHTTPProxyKMSConnect,
    AutoEncryptionOpts,
    HTTPProxyKMSConnect,
    KMSConnectContext,
)
from pymongo.errors import ConfigurationError, ConnectionFailure, EncryptionError, NetworkTimeout
from pymongo.pool_options import PoolOptions
from pymongo.ssl_support import get_ssl_context
from test.helpers_shared import CA_PEM, CERT_PATH, CLIENT_PEM

pytestmark = [pytest.mark.encryption, pytest.mark.asyncio]

_KMS_ADDRESS = ("kms.example.com", 443)

OPTS = CodecOptions()


class Facade:
    """The per-API accessors, shared by the tests through the ``api`` parameter."""

    def __init__(self, is_async: bool) -> None:
        self.is_async = is_async

    async def maybe_await(self, result: Any) -> Any:
        """Await ``result`` in the asynchronous API (a no-op otherwise)."""
        if self.is_async:
            return await result
        return result

    async def offload(self, func: Callable[..., Any], *args: Any) -> Any:
        """Run a blocking callable off the event loop (inline when synchronous)."""
        if self.is_async:
            return await asyncio.get_running_loop().run_in_executor(None, func, *args)
        return func(*args)

    def encryption(self):
        """The API's ``encryption`` module."""
        if self.is_async:
            from pymongo.asynchronous import encryption
        else:
            from pymongo.synchronous import encryption
        return encryption

    def kms_connect(self):
        """The API's ``_kms_connect`` module."""
        if self.is_async:
            from pymongo.asynchronous import _kms_connect
        else:
            from pymongo.synchronous import _kms_connect
        return _kms_connect

    async def connect(self, address, pool_options, callback, timeout):
        """``_connect_kms`` for this API."""
        module = self.kms_connect()
        if self.is_async:
            return await module._connect_kms(address, pool_options, callback, timeout)
        return module._connect_kms(address, pool_options, callback, timeout)

    def proxy(self, proxy_url, tls_context=None, headers=None):
        """The API's HTTP proxy KMS connect helper."""
        if self.is_async:
            return AsyncHTTPProxyKMSConnect(proxy_url, tls_context, headers=headers)
        return HTTPProxyKMSConnect(proxy_url, tls_context, headers=headers)

    def callback(self, func):
        """Adapt a non-blocking ``func(context)`` to the API's callback form."""
        if self.is_async:

            async def callback(context):
                return func(context)

            return callback

        return func

    def blocking_callback(self, func):
        """Adapt a blocking ``func(context)``, offloaded in the async API."""
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
        """A ClientEncryption for this API using a local key provider."""
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
        """A lazily-connecting client for this API."""
        if self.is_async:
            from pymongo.asynchronous.mongo_client import AsyncMongoClient

            return AsyncMongoClient()
        from pymongo import MongoClient

        return MongoClient()


ASYNC = Facade(is_async=True)
SYNC = Facade(is_async=False)

both_apis = pytest.mark.parametrize("api", [ASYNC, SYNC], ids=["async", "sync"])
async_only = pytest.mark.parametrize("api", [ASYNC], ids=["async"])


def _pool_options(ssl_context=None):
    return PoolOptions(connect_timeout=10, socket_timeout=10, ssl_context=ssl_context)


def _tls_server_context(cert=CLIENT_PEM):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert)
    return ctx


def _insecure_client_context():
    # PYTHON-5040 tracks re-enabling verification: the evergreen-tools CA
    # lacks an Authority Key Identifier newer OpenSSL requires.
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


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


@contextmanager
def _start_proxy(handler, backlog=1):
    """Serve each accepted connection with ``handler(conn)`` in a daemon thread."""
    with _listen(backlog) as listener:

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
        yield listener.getsockname()


@contextmanager
def _record_and_reply(accepted, reply):
    """A proxy that records each CONNECT request, replies ``reply``, and closes."""

    def handler(conn):
        request = _read_http_request(conn)
        if request is None:
            return
        accepted.append(request)
        conn.sendall(reply)

    with _start_proxy(handler) as addr:
        yield addr


@contextmanager
def _tls_echo_proxy(delay=0):
    """A TLS CONNECT proxy that replies 200, then echoes one tunneled read."""
    server_ctx = _tls_server_context()

    def handler(conn):
        tls = server_ctx.wrap_socket(conn, server_side=True)
        request = _read_http_request(tls)
        if request is None:
            return
        tls.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        # The tunneled peer speaks only after the client does, as a TLS
        # server would. The ``delay`` lets the reply outlast the CONNECT deadline.
        if delay:
            time.sleep(delay)
        tls.sendall(b"echo:" + tls.recv(64))
        tls.close()

    with _start_proxy(handler) as addr:
        yield addr


async def _echo_over_tunnel(api, sock):
    sock.settimeout(10)
    await api.offload(sock.sendall, b"ping")
    data = await api.offload(sock.recv, 64)
    assert data == b"echo:ping"


@both_apis
async def test_init_kms_connect_callback(api):
    opts = AutoEncryptionOpts({}, "k.d")
    assert opts._kms_connect_callback is None

    def action(context):
        raise AssertionError("not called")

    callback = api.callback(action)
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


@both_apis
async def test_non_socket_return_raises_configuration_error(api):
    with pytest.raises(ConfigurationError, match="must return a connected"):
        await api.connect(
            _KMS_ADDRESS, _pool_options(), api.callback_returning("not-a-socket"), 10.0
        )


@both_apis
async def test_already_wrapped_socket_is_rejected(api):
    # ssl.SSLSocket passes isinstance but cannot be TLS-wrapped again.
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with _socketpair() as (left, _right):
        # No peer is needed to produce a genuine ssl.SSLSocket.
        wrapped = ctx.wrap_socket(left, do_handshake_on_connect=False, server_hostname="x")
        with pytest.raises(ConfigurationError, match="unwrapped"):
            await api.connect(_KMS_ADDRESS, _pool_options(), api.callback_returning(wrapped), 10.0)


@both_apis
async def test_context_receives_host_port_and_timeout(api):
    received = []
    with _socketpair() as (left, _right):

        def action(context):
            received.append(context)
            return left

        # ssl_context=None returns the socket unchanged, so a plain socket is accepted.
        conn = await api.connect(_KMS_ADDRESS, _pool_options(), api.callback(action), 12.5)
        assert conn is left

        assert len(received) == 1
        assert received[0].host == "kms.example.com"
        assert received[0].port == 443
        assert received[0].timeout == 12.5


@both_apis
async def test_non_blocking_socket_from_callback_is_accepted(api):
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

        options = _pool_options(api.client_tls_context())

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            sock.setblocking(False)
            return sock

        conn = await api.connect(
            listener.getsockname(),
            options,
            api.blocking_callback(lambda context: connect()),
            10.0,
        )
        try:
            assert conn.gettimeout() is not None
        finally:
            conn.close()


@both_apis
async def test_tls_verification_targets_the_kms_host(api):
    # The handshake must verify against the KMS address, not the peer the
    # callback connected to: the cert covers 127.0.0.1 and localhost, but
    # not the KMS hostname used below.
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
        options = _pool_options(api.client_tls_context(verify=True))

        created = []

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            created.append(sock)
            return sock

        port = listener.getsockname()[1]
        # The cert covers localhost: verifying against the KMS address succeeds.
        conn = await api.connect(
            ("localhost", port),
            options,
            api.blocking_callback(lambda context: connect()),
            10.0,
        )
        try:
            # TLS-wrapped in either API: a new object, not the plain socket.
            assert conn is not created[0]
        finally:
            conn.close()
        # The cert does not cover this name, so verification fails even
        # though the peer's cert is valid for itself.
        with pytest.raises(ConnectionFailure):
            await api.connect(
                ("kms.example.com", port),
                options,
                api.blocking_callback(lambda context: connect()),
                10.0,
            )


@both_apis
async def test_asyncio_transport_socket_is_rejected(api):
    # get_extra_info("socket") is a TransportSocket, not a socket.socket.
    with _socketpair() as (left, _right):
        with pytest.raises(ConfigurationError, match="TransportSocket"):
            await api.connect(
                _KMS_ADDRESS,
                _pool_options(),
                api.callback_returning(TransportSocket(left)),
                10.0,
            )


@async_only
async def test_cancelled_tls_wrap_closes_late_socket(api):
    # A cancelled wrap can leave the executor producing an SSLSocket. The
    # done callback must close it.
    from pymongo.pool_shared import _close_late_socket

    with _socketpair() as (left, _right):
        future = asyncio.get_running_loop().create_future()
        future.set_result(left)
        assert left.fileno() != -1
        _close_late_socket(future)
        assert left.fileno() == -1


@both_apis
async def test_http_proxy_helper_tunnels_and_reports_refusal(api):
    # Covers the CONNECT handshake without KMS credentials.
    accepted: list[bytes] = []
    context = _kms_context()

    with _record_and_reply(accepted, b"HTTP/1.1 200 Connection Established\r\n\r\n") as (
        host,
        port,
    ):
        sock = await api.maybe_await(api.proxy(f"http://{host}:{port}")(context))
        with sock:
            assert isinstance(sock, socket.socket)
            assert accepted[0].split(b"\r\n")[0] == b"CONNECT kms.example.com:443 HTTP/1.1"

    with _record_and_reply(accepted, b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n") as (
        host,
        port,
    ):
        with pytest.raises(OSError, match="refused CONNECT"):
            await api.maybe_await(api.proxy(f"http://{host}:{port}")(context))

    # Any 2xx status is a successful tunnel, not just HTTP/1.1 200.
    with _record_and_reply(accepted, b"HTTP/1.0 200 Connection Established\r\n\r\n") as (
        host,
        port,
    ):
        sock = await api.maybe_await(api.proxy(f"http://{host}:{port}")(context))
        with sock:
            assert isinstance(sock, socket.socket)

    # A status code must be exactly three digits, with no zero padding.
    for reply in (b"HTTP/1.1 2000 Evil\r\n\r\n", b"HTTP/1.1 00200 Evil\r\n\r\n"):
        with _record_and_reply(accepted, reply) as (host, port):
            with pytest.raises(OSError, match="refused CONNECT"):
                await api.maybe_await(api.proxy(f"http://{host}:{port}")(context))


@both_apis
async def test_control_characters_in_kms_host_are_rejected(api):
    # Reject CR/LF in the configurable host before it reaches CONNECT.
    callback = api.proxy("http://proxy.example.com:8080")
    context = _kms_context(host="kms.example.com\r\nX-Injected: 1")
    with pytest.raises(ConfigurationError, match="control characters or whitespace"):
        await api.maybe_await(callback(context))
    # Whitespace would split the request line into extra tokens.
    context = _kms_context(host="kms.example.com ")
    with pytest.raises(ConfigurationError, match="control characters or whitespace"):
        await api.maybe_await(callback(context))


@both_apis
async def test_http_proxy_helper_sends_custom_headers(api):
    # Extra CONNECT headers reach the proxy verbatim.
    accepted: list[bytes] = []
    headers = {"Proxy-Authorization": "Basic dXNlcjpwYXNz", "X-Trace-Id": "abc123"}
    with _record_and_reply(accepted, b"HTTP/1.1 200 Connection Established\r\n\r\n") as (
        host,
        port,
    ):
        sock = await api.maybe_await(
            api.proxy(f"http://{host}:{port}", headers=headers)(_kms_context())
        )
        with sock:
            request = accepted[0]
            assert request.split(b"\r\n")[0] == b"CONNECT kms.example.com:443 HTTP/1.1"
            assert b"\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n" in request
            assert b"\r\nX-Trace-Id: abc123\r\n" in request
            assert request.count(b"\r\nHost: ") == 1


@both_apis
async def test_http_proxy_helper_authenticates_to_the_proxy(api):
    # The motivating case: 407 without credentials, 200 with them.
    def handler(conn):
        request = _read_http_request(conn)
        if request is None:
            return
        if b"\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n" in request:
            conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        else:
            conn.sendall(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")

    context = _kms_context()
    with _start_proxy(handler, backlog=2) as (host, port):
        with pytest.raises(OSError, match="refused CONNECT"):
            await api.maybe_await(api.proxy(f"http://{host}:{port}")(context))
        headers = {"Proxy-Authorization": "Basic dXNlcjpwYXNz"}
        sock = await api.maybe_await(api.proxy(f"http://{host}:{port}", headers=headers)(context))
        with sock:
            assert isinstance(sock, socket.socket)


@both_apis
async def test_http_proxy_helper_rejects_bad_headers(api):
    for headers in [
        {"Bad\r\nName": "x"},
        {"Bad Name": "x"},
        {"Bad\tName": "x"},
        # A legal token followed by a newline: re's $ can match just before
        # a trailing newline, so validation must require a full match.
        {"X-Ok\n": "x"},
        {"X-Ok": "ok\r\nInjected: 1"},
        {"Host": "evil.example.com"},
        {"host": "evil.example.com"},
        {"": "x"},
        {"Bad:Name": "x"},
    ]:
        with pytest.raises(ConfigurationError, match=r"proxy header|Host CONNECT header"):
            api.proxy("http://proxy.example.com:8080", headers=headers)

    for headers in [{1: "x"}, {"X-Ok": 1}, {None: "x"}, {"X-Ok": None}]:
        with pytest.raises(TypeError, match="must be strings"):
            api.proxy("http://proxy.example.com:8080", headers=headers)


@both_apis
async def test_http_proxy_helper_accepts_legal_header_values(api):
    # Colons and spaces are legal in values (e.g. auth schemes). Only
    # CR/LF would let a value inject a request line.
    callback = api.proxy(
        "http://proxy.example.com:8080",
        headers={"Proxy-Authorization": "Basic dXNlcjpwYXNz", "X-Token": "a: b"},
    )
    assert callback.headers == {
        "Proxy-Authorization": "Basic dXNlcjpwYXNz",
        "X-Token": "a: b",
    }


@both_apis
async def test_proxy_url_is_parsed(api):
    # The proxy URL has the same form as a proxy configured for urllib:
    # scheme, optional userinfo, host, and optional port with defaults.
    for url, host, port, tls in [
        ("http://proxy.example.com:8080", "proxy.example.com", 8080, False),
        ("http://proxy.example.com", "proxy.example.com", 80, False),
        ("https://proxy.example.com", "proxy.example.com", 443, True),
    ]:
        callback = api.proxy(url)
        assert callback.host == host
        assert callback.port == port
        if tls:
            assert callback.ssl_context is not None
        else:
            assert callback.ssl_context is None

    # An IPv6 literal is unwrapped for connecting.
    callback = api.proxy("http://[::1]:8080")
    assert callback.host == "::1"
    assert callback.port == 8080


@both_apis
async def test_https_proxy_url_defaults_to_the_default_context(api):
    # An https proxy URL implies TLS, like urllib, using the default
    # context unless one is passed.
    assert api.proxy("https://proxy.example.com").ssl_context is not None
    ctx = _insecure_client_context()
    assert api.proxy("https://proxy.example.com", ctx).ssl_context is ctx


@both_apis
async def test_proxy_url_userinfo_authenticates_to_the_proxy(api):
    # Userinfo becomes a Proxy-Authorization basic auth header,
    # percent-decoded like urllib decodes it.
    callback = api.proxy("http://user:p%40ss@proxy.example.com:8080")
    expected = base64.b64encode(b"user:p@ss").decode()
    assert callback.headers == {"Proxy-Authorization": f"Basic {expected}"}


@both_apis
async def test_proxy_url_is_validated(api):
    for url in [
        "proxy.example.com:8080",  # Missing scheme.
        "ftp://proxy.example.com",  # Not an HTTP(S) proxy.
        "http://",  # Missing host.
        "http://proxy.example.com/path",
        "http://proxy.example.com?x=1",
        "http://proxy.example.com#frag",
        "http://proxy.example.com:notaport",
        "http://proxy.example.com:99999",
        "http://[::1",  # Unmatched IPv6 bracket.
        "http://[example.com]",  # Invalid bracketed host.
    ]:
        with pytest.raises(ConfigurationError, match="proxy_url"):
            api.proxy(url)

    # A TLS context is only meaningful for an https proxy URL.
    with pytest.raises(ConfigurationError, match="https"):
        api.proxy("http://proxy.example.com", _insecure_client_context())

    # Userinfo and an explicit Proxy-Authorization header conflict.
    with pytest.raises(ConfigurationError, match="Proxy-Authorization"):
        api.proxy(
            "http://user:pass@proxy.example.com",
            headers={"Proxy-Authorization": "Basic dXNlcjpwYXNz"},
        )
    # Header validation runs before the userinfo handling, so a non-string
    # name is a TypeError even when userinfo would also add a header.
    with pytest.raises(TypeError, match="must be strings"):
        api.proxy("http://user:pass@proxy.example.com", headers={1: "x"})  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="proxy_url"):
        api.proxy(None)  # type: ignore[arg-type]


@both_apis
async def test_tls_proxy_helper_bridges_the_tunnel(api):
    # Covers the TLS-proxy path and the socketpair relay without KMS creds.
    with _tls_echo_proxy() as (host, port):
        sock = await api.maybe_await(
            api.proxy(f"https://{host}:{port}", _insecure_client_context())(_kms_context())
        )
        with sock:
            await _echo_over_tunnel(api, sock)


@both_apis
async def test_bridge_does_not_inherit_the_connect_deadline(api):
    # The relay must outlast the much shorter CONNECT deadline.
    with _tls_echo_proxy(delay=3.0) as (host, port):
        sock = await api.maybe_await(
            api.proxy(f"https://{host}:{port}", _insecure_client_context())(
                _kms_context(timeout=2.0)
            )
        )
        with sock:
            await _echo_over_tunnel(api, sock)


@both_apis
async def test_proxy_closing_before_connect_reply_raises(api):
    def handler(conn):
        # Read the CONNECT request, then hang up without replying.
        conn.recv(4096)

    with _start_proxy(handler) as (host, port):
        with pytest.raises(OSError, match="proxy closed the connection"):
            await api.maybe_await(api.proxy(f"http://{host}:{port}")(_kms_context()))


@async_only
async def test_cancelled_proxy_connect_closes_the_late_socket(api):
    # A cancelled connect must close the socket the executor thread
    # produces after the cancellation.
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

    with _start_proxy(handler) as (host, port):
        tunneled: list[socket.socket] = []
        original_tunnel = HTTPProxyKMSConnect._tunnel

        def spy_tunnel(self, sock, context, deadline):
            tunneled.append(sock)
            original_tunnel(self, sock, context, deadline)

        with mock.patch.object(HTTPProxyKMSConnect, "_tunnel", spy_tunnel):
            callback = api.proxy(f"http://{host}:{port}")
            task = asyncio.create_task(callback(_kms_context()))  # type: ignore[arg-type]
            waited = await api.offload(requested.wait, 10)
            assert waited, "proxy never received the CONNECT request"
            task.cancel("no longer needed")
            with pytest.raises(asyncio.CancelledError):
                await task
            # Let the stub reply, completing the executor's future late.
            reply.set()
            await asyncio.sleep(0.5)

        assert len(tunneled) == 1
        assert tunneled[0].fileno() == -1, "late socket was left open"


@both_apis
async def test_connect_timeout_is_not_reclassified(api):
    # A connect that times out keeps its socket.timeout type instead of
    # being reported as a generic connect error.
    def timeout_connect(self, address):
        raise socket.timeout("timed out")

    with mock.patch.object(socket.socket, "connect", timeout_connect):
        with pytest.raises(socket.timeout):
            HTTPProxyKMSConnect("http://127.0.0.1:9999")._connect_proxy(time.monotonic() + 10)


@both_apis
async def test_tunnel_keeps_bytes_sent_with_the_connect_reply(api):
    # A proxy may coalesce its 200 with tunneled bytes. Reading past the header would drop them.
    def handler(conn):
        conn.recv(4096)
        conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\nearly-bytes")

    with _start_proxy(handler) as (host, port):
        sock = await api.maybe_await(api.proxy(f"http://{host}:{port}")(_kms_context()))
        with sock:
            sock.settimeout(10)
            data = await api.offload(sock.recv, 64)
            assert data == b"early-bytes"


@both_apis
async def test_ipv6_host_is_bracketed_in_connect(api):
    accepted: list[bytes] = []
    with _record_and_reply(accepted, b"HTTP/1.1 200 Connection Established\r\n\r\n") as (
        host,
        port,
    ):
        sock = await api.maybe_await(api.proxy(f"http://{host}:{port}")(_kms_context(host="::1")))
        with sock:
            assert accepted[0].split(b"\r\n")[0] == b"CONNECT [::1]:443 HTTP/1.1"


@both_apis
async def test_oversized_connect_response_is_rejected(api):
    def handler(conn):
        conn.recv(4096)
        # Never sends the terminator.
        while True:
            conn.sendall(b"x" * 1024)

    with _start_proxy(handler) as (host, port):
        with pytest.raises(OSError, match="oversized CONNECT response"):
            await api.maybe_await(api.proxy(f"http://{host}:{port}")(_kms_context()))


@both_apis
async def test_remaining_raises_once_the_deadline_passes(api):
    from pymongo._kms_connect_shared import _remaining

    assert _remaining(time.monotonic() + 5) > 0
    with pytest.raises(socket.timeout):
        _remaining(time.monotonic() - 1)


@both_apis
async def test_bridge_failure_closes_the_proxy_socket(api):
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

    with _start_proxy(handler) as (host, port):
        context = _kms_context()

        with mock.patch.object(HTTPProxyKMSConnect, "_bridge", failing_bridge):
            with pytest.raises(OSError, match="no file descriptors"):
                await api.maybe_await(
                    api.proxy(f"https://{host}:{port}", _insecure_client_context())(context)
                )

        assert captured[0].fileno() == -1, "proxy socket was left open"


@async_only
async def test_non_coroutine_callback_is_rejected(api):
    # A plain def must be rejected before it blocks the event loop.
    entered = []

    def callback(context):
        entered.append(context)
        return None

    with pytest.raises(ConfigurationError, match="coroutine function"):
        await api.connect(_KMS_ADDRESS, _pool_options(), callback, 10.0)
    assert entered == [], "invalid callback must not be entered"


@both_apis
async def test_unconnected_socket_from_callback_is_rejected(api):
    # An unconnected socket would fail later as a transient error and be retried.
    with socket.socket() as bare:
        with pytest.raises(ConfigurationError, match="already connected"):
            await api.connect(_KMS_ADDRESS, _pool_options(), api.callback_returning(bare), 10.0)


@both_apis
async def test_datagram_socket_from_callback_is_rejected(api):
    # TLS on a connected UDP socket raises NotImplementedError, which would be retried.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as left:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as right:
            right.bind(("127.0.0.1", 0))
            left.connect(right.getsockname())

            with pytest.raises(ConfigurationError, match="stream socket"):
                await api.connect(_KMS_ADDRESS, _pool_options(), api.callback_returning(left), 10.0)


@both_apis
async def test_kms_request_does_not_retry_a_contract_violation(api):
    # _connect_kms has no retry loop; the no-retry guarantee is in
    # kms_request, so exercise that instead.
    calls = []

    def action(context):
        calls.append(context)
        return "not-a-socket"

    opts = AutoEncryptionOpts({}, "k.d", kms_connect_callback=api.callback(action))
    io = api.encryption()._EncryptionIO(None, mock.MagicMock(), None, opts)

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
        await api.maybe_await(io.kms_request(StubKmsContext()))
    assert len(calls) == 1


@both_apis
async def test_contract_violation_surfaces_as_encryption_error(api):
    # Callers see EncryptionError with ConfigurationError as its cause.
    with pytest.raises(EncryptionError) as exc_info:
        with api.encryption()._wrap_encryption_errors():
            raise ConfigurationError("kms_connect_callback must return ...")
    assert isinstance(exc_info.value.__cause__, ConfigurationError)


@both_apis
async def test_network_error_from_callback_propagates(api):
    def action(context):
        raise OSError("proxy unreachable")

    # Not a ConfigurationError, so kms_request retries it.
    with pytest.raises(OSError):
        await api.connect(_KMS_ADDRESS, _pool_options(), api.callback(action), 10.0)


@async_only
async def test_csot_deadline_stops_a_hung_callback(api):
    # A callback that ignores the timeout cannot block past the CSOT
    # deadline, and a socket it yields later must be closed.
    with _socketpair() as (left, _right):

        async def hung_callback(context):
            await asyncio.sleep(0.5)
            return left

        with pytest.raises(NetworkTimeout):
            with pymongo.timeout(0.1):
                await api.connect(_KMS_ADDRESS, _pool_options(), hung_callback, 10.0)
        assert left.fileno() != -1
        # Let the shielded callback finish. The driver closes the late result.
        await asyncio.sleep(0.75)
        assert left.fileno() == -1


@async_only
async def test_cancelling_kms_connect_closes_the_callback_socket(api):
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
            # handshake: peek for a ClientHello without consuming it, or for
            # EOF if the driver closed the socket, before wrap_socket
            # detaches conn.
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
                # A discarded connection may reset instead of reaching clean
                # EOF; either proves the driver closed it.
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

        options = _pool_options(api.client_tls_context())
        socks = []

        def connect():
            sock = socket.create_connection(listener.getsockname(), timeout=10)
            socks.append(sock)
            return sock

        # Schedule the connect now and cancel it once the callback socket
        # exists, so the cancellation lands mid-handshake.
        task = asyncio.ensure_future(
            api.connect(
                listener.getsockname(),
                options,
                api.blocking_callback(lambda context: connect()),
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
@both_apis
async def test_client_encryption_accepts_callback(api):
    def action(context):
        raise AssertionError("not called")

    callback = api.callback(action)
    client = api.simple_client()
    encryption = api.client_encryption(
        {"local": {"key": b"\x00" * 96}}, "keyvault.datakeys", client, callback
    )
    try:
        assert encryption._io_callbacks.opts._kms_connect_callback is callback
    finally:
        await api.maybe_await(encryption.close())
        await api.maybe_await(client.close())


@pytest.mark.skipif(not _HAVE_PYMONGOCRYPT, reason="pymongocrypt is not installed")
@both_apis
async def test_client_encryption_rejects_non_callable(api):
    client = api.simple_client()
    with pytest.raises(TypeError, match="kms_connect_callback must be callable"):
        api.client_encryption(
            {"local": {"key": b"\x00" * 96}},
            "keyvault.datakeys",
            client,
            "not-callable",
        )
    await api.maybe_await(client.close())
