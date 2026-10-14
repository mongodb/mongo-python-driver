# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""KMS connection support shared by the synchronous and asynchronous APIs.

The per-API connection logic lives in ``pymongo.asynchronous._kms_connect``
and its generated synchronous mirror.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import functools
import re
import socket
import ssl
import threading
import time
import urllib.parse
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from typing import Any, Callable, Optional

from pymongo.errors import ConfigurationError


@dataclass(frozen=True)
class KMSConnectContext:
    """Information about a pending KMS connection.

    Passed to ``kms_connect_callback``, which must return a plain, unwrapped
    :class:`socket.socket`. The driver performs the KMS TLS handshake over it,
    verifying against ``host`` rather than the peer actually reached.

    Prefer :class:`HTTPProxyKMSConnect` or :class:`AsyncHTTPProxyKMSConnect`
    over writing a callback.

    :param host: Hostname of the KMS server, and the TLS verification target.
    :param port: Port of the KMS server.
    :param timeout: Seconds allowed for the connection: the default KMS
        connect timeout, capped by the remaining ``timeoutMS`` budget.

    .. note:: ``timeoutMS`` does not constrain KMS requests for explicit
       encryption, so ``timeout`` is always the default there. Automatic
       encryption passes the remaining budget. This deviates from the Client
       Side Operations Timeout specification; see PYTHON-6037.

    .. versionadded:: 4.19
    """

    host: str
    port: int
    timeout: float


# A callback that opens a connection to a KMS host.
AsyncKMSConnectCallback = Callable[[KMSConnectContext], Awaitable[socket.socket]]
KMSConnectCallback = Callable[[KMSConnectContext], socket.socket]


def _close_rejected_kms_socket(obj: Any) -> None:
    """Close a callback return value that failed validation, best effort.

    ``_connect_kms`` raises on a contract violation, so no caller takes
    ownership. Tolerates non-socket values and ``close()`` failures.
    """
    close = getattr(obj, "close", None)
    if callable(close):
        with contextlib.suppress(Exception):
            close()


# Cap the CONNECT response header so a silent proxy cannot grow the buffer without bound.
_MAX_CONNECT_HEADER = 8192

# An RFC 7230 token: the grammar for a header field name.
_TOKEN_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


def _remaining(deadline: float) -> float:
    """Seconds left before ``deadline``."""
    left = deadline - time.monotonic()
    if left <= 0:
        raise socket.timeout("timed out connecting through the proxy")
    return left


def _redact_proxy_url(proxy_url: str) -> str:
    """Redact userinfo from a proxy URL for safe display in errors.

    Everything between the scheme and the last ``@`` goes: a malformed
    URL may place credentials anywhere before that ``@``. URLs without
    an ``@`` pass through unscathed.
    """
    scheme, sep, tail = proxy_url.partition("://")
    if sep:
        prefix = f"{scheme}://"
    elif ":" in proxy_url:
        # A scheme without ://, e.g. "user:secret@host:8080".
        scheme, _, tail = proxy_url.partition(":")
        prefix = f"{scheme}:"
    else:
        prefix, tail = "", proxy_url
    if "@" not in tail:
        return proxy_url
    return f"{prefix}***@{tail.rpartition('@')[2]}"


def _close_late_bridge_sockets(
    late_sockets: list[socket.socket], future: asyncio.Future[Any]
) -> None:
    """Close a late proxy-connect result: the driver end and the bridge.

    The bridge returns one end of a socketpair while its relay threads hold
    the proxy connection; closing only the driver end leaves those threads
    blocked in recv until the proxy closes the tunnel.
    """
    if not future.cancelled() and future.exception() is None:
        # The task may have succeeded after the awaiter gave up.
        late_sockets.append(future.result())
    for sock in late_sockets:
        # shutdown can raise the ValueError an SSLSocket shows in the
        # teardown race; tolerate it here exactly as relay does.
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except (OSError, ValueError):
            pass
        sock.close()


class HTTPProxyKMSConnect:
    """Route KMS connections through an HTTP proxy, for the synchronous API.

    Pass an instance as ``kms_connect_callback`` to reach KMS hosts through a
    forward proxy that speaks HTTP ``CONNECT``::

      from pymongo.encryption_options import AutoEncryptionOpts, HTTPProxyKMSConnect

      opts = AutoEncryptionOpts(
          kms_providers={"aws": aws_creds},
          key_vault_namespace="keyvault.datakeys",
          kms_connect_callback=HTTPProxyKMSConnect("http://proxy.example.com:8080"),
      )

    To reach the proxy over TLS, use an ``https`` proxy URL. Pass an
    :class:`ssl.SSLContext` to customize trust; the default context is used
    otherwise. It applies only to the proxy connection; KMS TLS is still
    negotiated end to end::

      import ssl

      proxy_tls = ssl.create_default_context(cafile="proxy-ca.pem")
      callback = HTTPProxyKMSConnect("https://proxy.example.com:8443", proxy_tls)

    Use :class:`AsyncHTTPProxyKMSConnect` with the asynchronous API.

    :param proxy_url: URL of the proxy, in the same form as a proxy configured
        for :mod:`urllib.request`, e.g. ``http://proxy.example.com:8080``. The
        scheme must be ``http`` or ``https``, and the port defaults to 80 and
        443, respectively. Userinfo, e.g.
        ``http://user:pass@proxy.example.com:8080``, is sent as a
        ``Proxy-Authorization`` basic auth header. When sending credentials,
        use an ``https`` proxy URL so they are not sent in cleartext.
    :param ssl_context: Optional :class:`ssl.SSLContext` for connecting to the
        proxy over TLS. Defaults to ``None``, meaning the default context for
        an ``https`` proxy URL, or a plain connection for ``http``.
    :param headers: Optional mapping of extra ``CONNECT`` request headers,
        e.g. ``{"X-Trace-Id": "abc123"}``. ``Host`` is always set from the KMS
        address, and ``Proxy-Authorization`` from any proxy URL userinfo.

    .. versionadded:: 4.19
    """

    def __init__(
        self,
        proxy_url: str,
        ssl_context: Optional[ssl.SSLContext] = None,
        headers: Optional[Mapping[str, str]] = None,
    ):
        if not isinstance(proxy_url, str):
            raise TypeError(f"proxy_url must be a string, not {type(proxy_url)}")
        try:
            split = urllib.parse.urlsplit(proxy_url)
        except ValueError as exc:
            # Malformed URLs, e.g. an unmatched IPv6 bracket, raise here
            # rather than surfacing lazily from the parsed parts below.
            raise ConfigurationError(
                f"invalid proxy_url: {_redact_proxy_url(proxy_url)!r}"
            ) from exc
        if split.scheme not in ("http", "https"):
            raise ConfigurationError(
                f"proxy_url must have an http or https scheme, not {_redact_proxy_url(proxy_url)!r}"
            )
        if not split.hostname:
            raise ConfigurationError(
                f"proxy_url must include a host, not {_redact_proxy_url(proxy_url)!r}"
            )
        if split.path not in ("", "/") or split.query or split.fragment:
            raise ConfigurationError(
                f"proxy_url must not include a path, query, or fragment: {_redact_proxy_url(proxy_url)!r}"
            )
        try:
            port = split.port
        except ValueError as exc:
            raise ConfigurationError(
                f"invalid proxy_url: {_redact_proxy_url(proxy_url)!r}"
            ) from exc
        self.host = split.hostname
        if port is None:
            port = 443 if split.scheme == "https" else 80
        elif port == 0:
            # Port zero is never a valid proxy destination.
            raise ConfigurationError(f"invalid proxy_url: {_redact_proxy_url(proxy_url)!r}")
        self.port = port
        if split.scheme == "https":
            self.ssl_context: Optional[ssl.SSLContext] = (
                ssl.create_default_context() if ssl_context is None else ssl_context
            )
        else:
            if ssl_context is not None:
                raise ConfigurationError("ssl_context requires an https proxy_url")
            self.ssl_context = None
        self.headers = dict(headers) if headers else {}
        self._validate_headers()
        if split.username is not None:
            if any(name.lower() == "proxy-authorization" for name in self.headers):
                raise ConfigurationError(
                    "proxy_url must not include userinfo when headers includes Proxy-Authorization"
                )
            password = urllib.parse.unquote(split.password) if split.password else ""
            creds = f"{urllib.parse.unquote(split.username)}:{password}".encode()
            token = base64.b64encode(creds).decode("ascii")
            self.headers["Proxy-Authorization"] = f"Basic {token}"

    def _validate_headers(self) -> None:
        """Validate the headers mapping: it is public and mutable."""
        for name, value in self.headers.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("proxy header names and values must be strings")
            # Header fields become CONNECT request lines; a name outside the
            # RFC 7230 token grammar, or CR/LF in a value, would corrupt or
            # inject request lines. Use fullmatch: a match() of an anchored
            # pattern accepts a trailing newline ($ matches just before it).
            if not _TOKEN_RE.fullmatch(name):
                raise ConfigurationError(f"invalid proxy header name: {name!r}")
            if name.lower() == "host":
                raise ConfigurationError("the Host CONNECT header is set from the KMS address")
            if "\r" in value or "\n" in value:
                raise ConfigurationError(f"invalid proxy header value for {name!r}")

    def _tunnel(self, sock: socket.socket, context: KMSConnectContext, deadline: float) -> None:
        # An IPv6 literal needs brackets to be a valid HTTP authority.
        host = f"[{context.host}]" if ":" in context.host else context.host
        target = f"{host}:{context.port}"
        lines = [f"CONNECT {target} HTTP/1.1", f"Host: {target}"]
        lines.extend(f"{name}: {value}" for name, value in self.headers.items())
        sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
        # Read a byte at a time: a bulk read could consume tunneled bytes from
        # this same socket. Reapply the budget before each read so a trickling
        # proxy cannot outlive the deadline.
        response = bytearray()
        while not response.endswith(b"\r\n\r\n"):
            sock.settimeout(_remaining(deadline))
            chunk = sock.recv(1)
            if not chunk:
                raise OSError(f"proxy closed the connection while tunneling to {target}")
            response += chunk
            if len(response) > _MAX_CONNECT_HEADER:
                raise OSError(f"proxy sent an oversized CONNECT response for {target}")
        status = bytes(response).split(b"\r\n", 1)[0]
        # A CONNECT is successful for any 2xx status, e.g. "HTTP/1.0 200" or
        # "HTTP/1.1 201"; require a three-digit code and reject malformed lines.
        parts = status.split(b" ", 2)
        valid = (
            len(parts) >= 2
            and parts[0].startswith(b"HTTP/")
            and len(parts[1]) == 3
            and parts[1].isdigit()
        )
        if not valid or not 200 <= int(parts[1]) < 300:
            raise OSError(f"proxy refused CONNECT to {target}: {status!r}")

    def _bridge(
        self, proxy: socket.socket, late_sockets: Optional[list[socket.socket]] = None
    ) -> socket.socket:
        """Relay a TLS proxy connection through a socketpair.

        Python cannot layer TLS over an :class:`ssl.SSLSocket`, so return the
        plain end of a pair, using threads rather than tasks even in
        :class:`AsyncHTTPProxyKMSConnect`: the event loop cannot read an
        :class:`ssl.SSLSocket`.
        """
        # Clear the CONNECT-phase timeout; the tunneled KMS request is governed
        # by the driver's own timeout, not the elapsed connect budget.
        proxy.settimeout(None)
        driver_side, relay_side = socket.socketpair()
        # Each socket is read by one relay and written by the other, and it
        # closes only when both of those have ended: EOF propagates as a
        # write-half shutdown, and the reverse direction may still carry
        # a reply.
        finished: dict[socket.socket, set[str]] = {relay_side: set(), proxy: set()}
        phases_lock = threading.Lock()

        def relay(src: socket.socket, dst: socket.socket) -> None:
            # Daemon threads: any error, including the ValueError an
            # SSLSocket.shutdown can raise in the teardown race, ends the relay.
            try:
                while True:
                    buf = src.recv(16384)
                    if not buf:
                        break
                    dst.sendall(buf)
            except (OSError, ValueError):
                pass
            finally:
                with phases_lock:
                    finished[src].add("read")
                    finished[dst].add("write")
                    done = [s for s, marks in finished.items() if len(marks) == 2]
                    for s in done:
                        del finished[s]
                for sock in done:
                    # Both directions over this socket have ended.
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except (OSError, ValueError):
                        pass
                    sock.close()
                if dst not in done:
                    # Send EOF downstream without cutting the reverse
                    # direction, which may still deliver a reply.
                    try:
                        dst.shutdown(socket.SHUT_WR)
                    except (OSError, ValueError):
                        pass

        try:
            for pair in ((relay_side, proxy), (proxy, relay_side)):
                threading.Thread(target=relay, args=pair, daemon=True).start()
        except BaseException:
            # Unblock any thread that did start, then drop every socket.
            # shutdown can raise the ValueError an SSLSocket shows in the
            # teardown race; tolerate it here exactly as relay does.
            for sock in (proxy, relay_side, driver_side):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except (OSError, ValueError):
                    pass
                sock.close()
            raise
        if late_sockets is not None:
            # Late cleanup must stop the relay threads, which hold proxy and
            # relay_side even after the driver end is closed.
            late_sockets.extend((proxy, relay_side))
        return driver_side

    def __call__(
        self, context: KMSConnectContext, *, deadline: Optional[float] = None
    ) -> socket.socket:
        # One deadline for all three phases; per-phase timeouts would multiply
        # the caller's budget.
        if deadline is None:
            deadline = time.monotonic() + context.timeout
        return self._connect_via(context, deadline)

    def _connect_via(
        self,
        context: KMSConnectContext,
        deadline: float,
        late_sockets: Optional[list[socket.socket]] = None,
    ) -> socket.socket:
        # A configurable KMS host could inject CR/LF into, or split the
        # request line of, the CONNECT request.
        if any(not c.isprintable() or c.isspace() for c in context.host):
            raise ConfigurationError(
                f"KMS host must not contain control characters or whitespace: {context.host!r}"
            )
        # The headers mapping is public: validate whatever it holds now.
        self._validate_headers()
        sock = self._connect_proxy(deadline)
        try:
            if self.ssl_context is not None:
                sock.settimeout(_remaining(deadline))
                sock = self.ssl_context.wrap_socket(sock, server_hostname=self.host)
            sock.settimeout(_remaining(deadline))
            self._tunnel(sock, context, deadline)
        except BaseException:
            sock.close()
            raise
        if self.ssl_context is None:
            return sock
        try:
            return self._bridge(sock, late_sockets)
        except BaseException:
            sock.close()
            raise

    def _connect_proxy(self, deadline: float) -> socket.socket:
        # Recompute the budget per address, rather than socket.create_connection,
        # which applies the timeout to every address.
        #
        # DNS resolution is not bounded by the deadline; a slow lookup can
        # exceed the budget, as in the driver's own connect path.
        last_error: Optional[OSError] = None
        for family, socktype, proto, _, sockaddr in socket.getaddrinfo(
            self.host, self.port, type=socket.SOCK_STREAM
        ):
            sock = socket.socket(family, socktype, proto)
            try:
                # Propagate the timeout from _remaining rather than report a
                # connect error.
                sock.settimeout(_remaining(deadline))
            except socket.timeout:
                sock.close()
                raise
            try:
                sock.connect(sockaddr)
            except socket.timeout:
                # Preserve the timeout type rather than report a generic
                # connect error.
                sock.close()
                raise
            except OSError as exc:
                last_error = exc
                sock.close()
                continue
            return sock
        if last_error is None:
            # getaddrinfo returned no usable addresses.
            raise OSError(f"could not connect to proxy {self.host}:{self.port}")
        raise OSError(
            f"could not connect to proxy {self.host}:{self.port}: {last_error}"
        ) from last_error


class AsyncHTTPProxyKMSConnect(HTTPProxyKMSConnect):
    """Route KMS connections through an HTTP proxy, for the asynchronous API.

    Behaves exactly like :class:`HTTPProxyKMSConnect`, but is a coroutine
    callable and runs the blocking connect in a thread so the event loop stays
    free.

    .. versionadded:: 4.19
    """

    async def __call__(self, context: KMSConnectContext) -> socket.socket:  # type: ignore[override]
        # Executor queue time counts against the budget.
        deadline = time.monotonic() + context.timeout
        late_sockets: list[socket.socket] = []
        connect = functools.partial(super()._connect_via, context, deadline, late_sockets)
        future = asyncio.get_running_loop().run_in_executor(None, connect)
        try:
            return await asyncio.wait_for(asyncio.shield(future), _remaining(deadline))
        except asyncio.CancelledError:
            # The thread runs on regardless; close its late sockets.
            future.add_done_callback(functools.partial(_close_late_bridge_sockets, late_sockets))
            raise
        except TimeoutError:
            if future.done() and not future.cancelled():
                exc = future.exception()
                if exc is not None:
                    # Report the executor's own error.
                    raise exc from None
                # The task succeeded right at the deadline.
                _close_late_bridge_sockets(late_sockets, future)
                raise socket.timeout("timed out connecting through the proxy") from None
            if not future.done():
                # The thread runs on regardless; close its late sockets.
                future.add_done_callback(
                    functools.partial(_close_late_bridge_sockets, late_sockets)
                )
            raise socket.timeout("timed out connecting through the proxy") from None


# Sphinx documents these classes under pymongo.encryption_options, the public
# import path, so the definitions must claim that module name.
KMSConnectContext.__module__ = "pymongo.encryption_options"
HTTPProxyKMSConnect.__module__ = "pymongo.encryption_options"
AsyncHTTPProxyKMSConnect.__module__ = "pymongo.encryption_options"
