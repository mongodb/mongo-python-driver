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
from pymongo.pool_shared import _close_late_socket


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
            raise ConfigurationError(f"invalid proxy_url: {proxy_url!r}") from exc
        if split.scheme not in ("http", "https"):
            raise ConfigurationError(
                f"proxy_url must have an http or https scheme, not {proxy_url!r}"
            )
        if not split.hostname:
            raise ConfigurationError(f"proxy_url must include a host, not {proxy_url!r}")
        if split.path not in ("", "/") or split.query or split.fragment:
            raise ConfigurationError(
                f"proxy_url must not include a path, query, or fragment: {proxy_url!r}"
            )
        try:
            port = split.port
        except ValueError as exc:
            raise ConfigurationError(f"invalid proxy_url: {proxy_url!r}") from exc
        self.host = split.hostname
        self.port = port or (443 if split.scheme == "https" else 80)
        if split.scheme == "https":
            self.ssl_context: Optional[ssl.SSLContext] = (
                ssl.create_default_context() if ssl_context is None else ssl_context
            )
        else:
            if ssl_context is not None:
                raise ConfigurationError("ssl_context requires an https proxy_url")
            self.ssl_context = None
        self.headers = dict(headers) if headers else {}
        for name, value in self.headers.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("proxy header names and values must be strings")
            # Header fields become CONNECT request lines; a name outside the
            # RFC 7230 token grammar, or CR/LF in a value, would corrupt or
            # inject request lines.
            if not _TOKEN_RE.match(name):
                raise ConfigurationError(f"invalid proxy header name: {name!r}")
            if name.lower() == "host":
                raise ConfigurationError("the Host CONNECT header is set from the KMS address")
            if "\r" in value or "\n" in value:
                raise ConfigurationError(f"invalid proxy header value for {name!r}")
        if split.username is not None:
            if any(name.lower() == "proxy-authorization" for name in self.headers):
                raise ConfigurationError(
                    "proxy_url must not include userinfo when headers includes Proxy-Authorization"
                )
            password = urllib.parse.unquote(split.password) if split.password else ""
            creds = f"{urllib.parse.unquote(split.username)}:{password}".encode()
            token = base64.b64encode(creds).decode("ascii")
            self.headers["Proxy-Authorization"] = f"Basic {token}"

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

    def _bridge(self, proxy: socket.socket) -> socket.socket:
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
                # Send EOF to the peer instead of closing a socket it may be reading.
                try:
                    dst.shutdown(socket.SHUT_RDWR)
                except (OSError, ValueError):
                    pass
                src.close()

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
        return driver_side

    def __call__(
        self, context: KMSConnectContext, *, deadline: Optional[float] = None
    ) -> socket.socket:
        # A configurable KMS host could inject CR/LF into, or split the
        # request line of, the CONNECT request.
        if any(not c.isprintable() or c.isspace() for c in context.host):
            raise ConfigurationError(
                f"KMS host must not contain control characters or whitespace: {context.host!r}"
            )
        # One deadline for all three phases; per-phase timeouts would multiply
        # the caller's budget.
        if deadline is None:
            deadline = time.monotonic() + context.timeout
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
            return self._bridge(sock)
        except BaseException:
            sock.close()
            raise

    def _connect_proxy(self, deadline: float) -> socket.socket:
        # Recompute the budget per address, rather than socket.create_connection,
        # which applies the timeout to every address.
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
        # Capture the deadline before scheduling so time spent queued behind
        # other executor work counts against the KMS budget.
        deadline = time.monotonic() + context.timeout
        connect = functools.partial(super().__call__, context, deadline=deadline)
        future = asyncio.get_running_loop().run_in_executor(None, connect)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # The thread runs on regardless, so close the socket it returns.
            future.add_done_callback(_close_late_socket)
            raise


# Sphinx documents these classes under pymongo.encryption_options, the public
# import path, so the definitions must claim that module name.
KMSConnectContext.__module__ = "pymongo.encryption_options"
HTTPProxyKMSConnect.__module__ = "pymongo.encryption_options"
AsyncHTTPProxyKMSConnect.__module__ = "pymongo.encryption_options"
