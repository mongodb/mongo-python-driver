# Copyright 2025-present MongoDB, Inc.
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

"""Pool utilities and shared helper methods."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import os
import socket
import ssl
import sys
import time
import weakref
from collections.abc import MutableMapping
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    NoReturn,
    Optional,
    Protocol,
    TypeVar,
    Union,
)

from pymongo import _csot
from pymongo._telemetry import _CmapTelemetry
from pymongo.asynchronous.helpers import _getaddrinfo
from pymongo.auth_shared import _AuthContext
from pymongo.common import MAX_BSON_SIZE, MAX_MESSAGE_SIZE, MAX_WIRE_VERSION, MAX_WRITE_BATCH_SIZE
from pymongo.errors import (  # type:ignore[attr-defined]
    AutoReconnect,
    ConnectionFailure,
    ExecutionTimeout,
    InvalidOperation,
    NetworkTimeout,
    NotPrimaryError,
    PyMongoError,
    WaitQueueTimeoutError,
    _CertificateError,
)
from pymongo.hello import HelloCompat
from pymongo.helpers_shared import _get_timeout_details, format_timeout_details
from pymongo.monitoring import ConnectionCheckOutFailedReason, ConnectionClosedReason
from pymongo.network_layer import (
    AsyncNetworkingInterface,
    NetworkingInterface,
    NetworkingInterfaceBase,
    PyMongoProtocol,
)
from pymongo.pool_options import PoolOptions
from pymongo.server_api import _add_to_command
from pymongo.socket_checker import SocketChecker
from pymongo.ssl_support import SSL_EOF_ERRORS, PYSSLError, SSLError, _has_sni

SSLErrors = (PYSSLError, SSLError)
if TYPE_CHECKING:
    from bson.objectid import ObjectId
    from pymongo.compression_support import SnappyContext, ZlibContext, ZstdContext
    from pymongo.pyopenssl_context import _sslConn
    from pymongo.typings import _Address, _AgnosticClientSession, _AgnosticMongoClient

# Flags recording the counters a checkout has incremented, so a failed
# checkout can restore them exactly once (PYTHON-6136).
_UNDO_OPERATION_COUNT = 1
_UNDO_REQUESTS = 2
_UNDO_SOCKETS = 4
_UNDO_PENDING = 8

# The connection type stored by a pool and the pool type owning a connection,
# so shared methods see the concrete subclasses.
_ConnT = TypeVar("_ConnT", bound="_AgnosticBaseConnection[Any]")
_PoolT = TypeVar("_PoolT", bound="_AgnosticBasePool[Any]")


class _ConnectionTelemetryInfo(Protocol):
    """Protocol for connection fields consumed by :class:`~pymongo._telemetry._CommandTelemetry`."""

    id: int
    server_connection_id: Optional[int]
    address: tuple[str, int]
    service_id: Optional[ObjectId]


class _AgnosticBaseConnection(_ConnectionTelemetryInfo, Generic[_PoolT]):
    """Shared base class for AsyncConnection and Connection. Subclasses contain all methods that perform I/O."""

    def __init__(
        self,
        conn: NetworkingInterfaceBase,
        pool: _PoolT,
        address: tuple[str, int],
        id: int,
        is_sdam: bool,
    ):
        self.conn = conn
        self.pool_ref: weakref.ref[_PoolT] = weakref.ref(pool)
        self.address = address
        self.id = id
        self.is_sdam = is_sdam
        self.closed = False
        self.creation_time = time.monotonic()
        self.last_checkin_time = time.monotonic()
        self.performed_handshake = False
        self.is_writable: bool = False
        self.max_wire_version = MAX_WIRE_VERSION
        self.max_bson_size = MAX_BSON_SIZE
        self.max_message_size = MAX_MESSAGE_SIZE
        self.max_write_batch_size = MAX_WRITE_BATCH_SIZE
        self.supports_sessions = False
        self.hello_ok: bool = False
        self.is_mongos = False
        self.listeners = pool.opts._event_listeners
        self._telemetry = pool._telemetry
        self.compression_settings = pool.opts._compression_settings
        self.compression_context: Union[SnappyContext, ZlibContext, ZstdContext, None] = None
        self.socket_checker: SocketChecker = SocketChecker()
        self.oidc_token_gen_id: Optional[int] = None
        # Support for mechanism negotiation on the initial handshake.
        self.negotiated_mechs: Optional[list[str]] = None
        self.auth_ctx: Optional[_AuthContext] = None

        # The pool's generation changes with each reset() so we can close
        # sockets created before the last reset.
        self.pool_gen = pool.gen
        self.generation = self.pool_gen.get_overall()
        self.ready = False
        self.cancel_context: _CancellationContext = _CancellationContext()
        self.opts = pool.opts
        self.more_to_come: bool = False
        # For load balancer support.
        self.service_id: Optional[ObjectId] = None
        self.server_connection_id: Optional[int] = None
        # When executing a transaction in load balancing mode, this flag is
        # set to true to indicate that the session now owns the connection.
        self.pinned_txn = False
        self.pinned_cursor = False
        self.active = False
        self.last_timeout = self.opts.socket_timeout
        self.connect_rtt = 0.0
        # For gossiping $clusterTime from the connection handshake to the client.
        self._cluster_time = None

    def set_conn_timeout(self, timeout: Optional[float]) -> None:
        """Cache last timeout to avoid duplicate calls to conn.settimeout."""
        if timeout == self.last_timeout:
            return
        self.last_timeout = timeout
        self.conn.get_conn.settimeout(timeout)

    def apply_timeout(self, cmd: Optional[MutableMapping[str, Any]]) -> Optional[float]:
        # CSOT: use remaining timeout when set.
        timeout = _csot.remaining()
        if timeout is None:
            # Reset the socket timeout unless we're performing a streaming monitor check.
            if not self.more_to_come:
                self.set_conn_timeout(self.opts.socket_timeout)
            return None
        # RTT validation.
        rtt = _csot.get_rtt()
        if rtt is None:
            rtt = self.connect_rtt
        max_time_ms = timeout - rtt
        if max_time_ms < 0:
            timeout_details = _get_timeout_details(self.opts)
            formatted = format_timeout_details(timeout_details)
            # CSOT: raise an error without running the command since we know it will time out.
            errmsg = f"operation would exceed time limit, remaining timeout:{timeout:.5f} <= network round trip time:{rtt:.5f} {formatted}"
            raise ExecutionTimeout(
                errmsg,
                50,
                {"ok": 0, "errmsg": errmsg, "code": 50},
                self.max_wire_version,
            )
        if cmd is not None:
            cmd["maxTimeMS"] = int(max_time_ms * 1000)
        self.set_conn_timeout(timeout)
        return timeout

    def pin_txn(self) -> None:
        self.pinned_txn = True
        assert not self.pinned_cursor

    def pin_cursor(self) -> None:
        self.pinned_cursor = True
        assert not self.pinned_txn

    def hello_cmd(self) -> dict[str, Any]:
        # As of PYTHON-5713, always use OP_MSG for the handshake since all
        # supported servers (MongoDB 4.4+, wire version >= 9) support it.
        if self.opts.server_api or self.hello_ok or self.opts.load_balanced:
            return {HelloCompat.CMD: 1}
        return {HelloCompat.LEGACY_CMD: 1, "helloOk": True}

    def _raise_if_not_writable(self) -> None:
        """Raise NotPrimaryError if this connection is not writable."""
        if not self.is_writable:
            raise NotPrimaryError("not primary", {"ok": 0, "errmsg": "not primary", "code": 10107})

    def validate_session(
        self, client: Optional[_AgnosticMongoClient], session: Optional[_AgnosticClientSession]
    ) -> None:
        """Validate this session before use with client.

        Raises error if the client is not the one that created the session.
        """
        if session:
            if session._client is not client:
                raise InvalidOperation(
                    f"Can only use session with the {type(session._client).__name__} that started it"
                )

    def send_cluster_time(
        self,
        command: MutableMapping[str, Any],
        session: Optional[_AgnosticClientSession],
        client: Optional[_AgnosticMongoClient],
    ) -> None:
        """Add $clusterTime."""
        if client:
            client._send_cluster_time(command, session)  # type: ignore[arg-type]

    def add_server_api(self, command: MutableMapping[str, Any]) -> None:
        """Add server_api parameters."""
        if self.opts.server_api:
            _add_to_command(command, self.opts.server_api)

    def update_last_checkin_time(self) -> None:
        self.last_checkin_time = time.monotonic()

    def update_is_writable(self, is_writable: bool) -> None:
        self.is_writable = is_writable

    def idle_time_seconds(self) -> float:
        """Seconds since this socket was last checked into its pool."""
        return time.monotonic() - self.last_checkin_time

    def __eq__(self, other: Any) -> bool:
        return self.conn == other.conn

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self.conn)


class _AgnosticBasePool(Generic[_ConnT]):
    """Shared base class for async and sync Pool. Subclasses contain all methods that perform I/O."""

    def __init__(
        self,
        address: _Address,
        options: PoolOptions,
        is_sdam: bool = False,
        client_id: Optional[ObjectId] = None,
    ):
        """
        :param address: a (hostname, port) tuple
        :param options: a PoolOptions instance
        :param is_sdam: whether to call hello for each new connection
        """
        if options.pause_enabled:
            self.state = PoolState.PAUSED
        else:
            self.state = PoolState.READY
        # Check a socket's health with socket_closed() every once in a while.
        # Can override for testing: 0 to always check, None to never check.
        self._check_interval_seconds = 1
        # LIFO pool. Sockets are ordered on idle time. Sockets claimed
        # and returned to pool from the left side. Stale sockets removed
        # from the right side.
        self.conns: collections.deque[_ConnT] = collections.deque()
        self.active_contexts: set[_CancellationContext] = set()
        self.active_sockets = 0
        # Monotonically increasing connection ID required for CMAP Events.
        self.next_connection_id = 1
        # Track whether the sockets in this pool are writeable or not.
        self.is_writable: Optional[bool] = None

        # Keep track of resets, so we notice sockets created before the most
        # recent reset and close them.
        self.gen = _PoolGeneration()
        self.pid = os.getpid()
        self.address = address
        self.opts = options
        self.is_sdam = is_sdam

        self.requests = 0
        self.max_pool_size = self.opts.max_pool_size
        if not self.max_pool_size:
            self.max_pool_size = float("inf")
        self._pending = 0
        self._max_connecting = self.opts.max_connecting
        self._ssl_session_cache: Optional[list[Any]] = (
            [None] if self.opts._ssl_context is not None else None
        )
        self._telemetry = _CmapTelemetry(
            client_id, address, options._event_listeners, publish=not is_sdam, log=not is_sdam
        )
        # Similar to active_sockets but includes threads in the wait queue.
        self.operation_count: int = 0
        # Retain references to pinned connections to prevent the CPython GC
        # from thinking that a cursor's pinned connection can be GC'd when the
        # cursor is GC'd (see PYTHON-2751).
        self._pinned_sockets: set[_ConnT] = set()
        self.ncursors = 0
        self.ntxns = 0
        self._telemetry.pool_created(self.opts.non_default_options)

    @property
    def closed(self) -> bool:
        return self.state == PoolState.CLOSED

    def stale_generation(self, gen: int, service_id: Optional[ObjectId]) -> bool:
        return self.gen.stale(gen, service_id)

    def _handle_connection_error(self, error: BaseException) -> None:
        # Handle system overload condition for non-sdam pools.
        # Look for errors of type AutoReconnect and add error labels if appropriate.
        if self.is_sdam or type(error) not in (AutoReconnect, NetworkTimeout):
            return
        assert isinstance(error, AutoReconnect)  # Appease type checker.
        # If the original error was a DNS, certificate, or SSL error, ignore it.
        if isinstance(error.__cause__, (_CertificateError, SSLErrors, socket.gaierror)):
            # End of file errors are excluded, because the server may have disconnected
            # during the handshake.
            if not isinstance(error.__cause__, SSL_EOF_ERRORS):
                return
        error._add_error_label("SystemOverloadedError")
        error._add_error_label("RetryableError")

    def _raise_if_not_ready(self, checkout_started_time: float, emit_event: bool) -> None:
        if self.state != PoolState.READY:
            if emit_event:
                self._telemetry.checkout_failed(
                    "An error occurred while trying to establish a new connection",
                    ConnectionCheckOutFailedReason.CONN_ERROR,
                    checkout_started_time,
                )

            details = _get_timeout_details(self.opts)
            _raise_connection_failure(
                self.address, AutoReconnect("connection pool paused"), timeout_details=details
            )

    def _restore_applied(self, applied: int) -> None:
        """Restore the counters flagged in ``applied``. Caller holds ``size_cond``."""
        if applied & _UNDO_OPERATION_COUNT:
            self.operation_count -= 1
        if applied & _UNDO_REQUESTS:
            self.requests -= 1
        if applied & _UNDO_SOCKETS:
            self.active_sockets -= 1
        if applied & _UNDO_PENDING:
            self._pending -= 1

    def _checkin_apply(
        self, conn: _ConnT, txn: bool, cursor: bool, forked: bool
    ) -> tuple[Optional[str], bool, bool]:
        """Apply checkin accounting; caller holds ``size_cond``.

        No cooperative I/O, so safe while a gevent greenlet unwinds. Returns
        ``(close_conn_reason, emit_closed, appended)`` for outside the lock.
        """
        self.active_contexts.discard(conn.cancel_context)
        if txn:
            self.ntxns -= 1
        elif cursor:
            self.ncursors -= 1
        self.requests -= 1
        self.active_sockets -= 1
        self.operation_count -= 1
        close_conn_reason: Optional[str] = None
        emit_closed = False
        appended = False
        if not forked:
            if self.closed:
                close_conn_reason = ConnectionClosedReason.POOL_CLOSED
            elif conn.closed:
                # CMAP requires the closed event be emitted after the check in.
                emit_closed = True
            elif self.stale_generation(conn.generation, conn.service_id):
                close_conn_reason = ConnectionClosedReason.STALE
            else:
                conn.update_last_checkin_time()
                conn.update_is_writable(bool(self.is_writable))
                self.conns.appendleft(conn)
                appended = True
        return close_conn_reason, emit_closed, appended

    def _raise_wait_queue_timeout(self, checkout_started_time: float) -> NoReturn:
        self._telemetry.checkout_failed(
            "Wait queue timeout elapsed without a connection becoming available",
            ConnectionCheckOutFailedReason.TIMEOUT,
            checkout_started_time,
        )
        timeout = _csot.get_timeout() or self.opts.wait_queue_timeout
        if self.opts.load_balanced:
            other_ops = self.active_sockets - self.ncursors - self.ntxns
            raise WaitQueueTimeoutError(
                "Timeout waiting for connection from the connection pool. "
                f"maxPoolSize: {self.opts.max_pool_size}, connections in use by cursors: {self.ncursors}, "
                f"connections in use by transactions: {self.ntxns}, connections in use "
                f"by other operations: {other_ops}, timeout: {timeout}"
            )
        raise WaitQueueTimeoutError(
            "Timed out while checking out a connection from connection pool. "
            f"maxPoolSize: {self.opts.max_pool_size}, timeout: {timeout}"
        )


def _get_ssl_session(ssl_sock: Any) -> Optional[Any]:
    """Return the TLS session from an SSL socket, handling both PyOpenSSL and stdlib ssl."""
    if hasattr(ssl_sock, "get_session"):
        return ssl_sock.get_session()
    return getattr(ssl_sock, "session", None)


try:
    from fcntl import F_GETFD, F_SETFD, FD_CLOEXEC, fcntl

    def _set_non_inheritable_non_atomic(fd: int) -> None:
        """Set the close-on-exec flag on the given file descriptor."""
        flags = fcntl(fd, F_GETFD)
        fcntl(fd, F_SETFD, flags | FD_CLOEXEC)

except ImportError:
    # Windows, various platforms we don't claim to support
    # (Jython, IronPython, ..), systems that don't provide
    # everything we need from fcntl, etc.
    def _set_non_inheritable_non_atomic(fd: int) -> None:
        """Dummy function for platforms that don't provide fcntl."""


_MAX_TCP_KEEPIDLE = 120
_MAX_TCP_KEEPINTVL = 10
_MAX_TCP_KEEPCNT = 9

if sys.platform == "win32":
    try:
        import _winreg as winreg
    except ImportError:
        import winreg

    def _query(key, name, default):
        try:
            value, _ = winreg.QueryValueEx(key, name)
            # Ensure the value is a number or raise ValueError.
            return int(value)
        except (OSError, ValueError):
            # QueryValueEx raises OSError when the key does not exist (i.e.
            # the system is using the Windows default value).
            return default

    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"
        ) as key:
            _WINDOWS_TCP_IDLE_MS = _query(key, "KeepAliveTime", 7200000)
            _WINDOWS_TCP_INTERVAL_MS = _query(key, "KeepAliveInterval", 1000)
    except OSError:
        # We could not check the default values because winreg.OpenKey failed.
        # Assume the system is using the default values.
        _WINDOWS_TCP_IDLE_MS = 7200000
        _WINDOWS_TCP_INTERVAL_MS = 1000

    def _set_keepalive_times(sock):
        idle_ms = min(_WINDOWS_TCP_IDLE_MS, _MAX_TCP_KEEPIDLE * 1000)
        interval_ms = min(_WINDOWS_TCP_INTERVAL_MS, _MAX_TCP_KEEPINTVL * 1000)
        if idle_ms < _WINDOWS_TCP_IDLE_MS or interval_ms < _WINDOWS_TCP_INTERVAL_MS:
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, idle_ms, interval_ms))

else:

    def _set_tcp_option(sock: socket.socket, tcp_option: str, max_value: int) -> None:
        if hasattr(socket, tcp_option):
            sockopt = getattr(socket, tcp_option)
            try:
                # PYTHON-1350 - NetBSD doesn't implement getsockopt for
                # TCP_KEEPIDLE and friends. Don't attempt to set the
                # values there.
                default = sock.getsockopt(socket.IPPROTO_TCP, sockopt)
                if default > max_value:
                    sock.setsockopt(socket.IPPROTO_TCP, sockopt, max_value)
            except OSError:
                pass

    def _set_keepalive_times(sock: socket.socket) -> None:
        _set_tcp_option(sock, "TCP_KEEPIDLE", _MAX_TCP_KEEPIDLE)
        _set_tcp_option(sock, "TCP_KEEPINTVL", _MAX_TCP_KEEPINTVL)
        _set_tcp_option(sock, "TCP_KEEPCNT", _MAX_TCP_KEEPCNT)


def _raise_connection_failure(
    address: Any,
    error: Exception,
    msg_prefix: Optional[str] = None,
    timeout_details: Optional[dict[str, float]] = None,
) -> NoReturn:
    """Convert a socket.error to ConnectionFailure and raise it."""
    host, port = address
    # If connecting to a Unix socket, port will be None.
    if port is not None:
        msg = f"{host}:{port}: {error}"
    else:
        msg = f"{host}: {error}"
    if msg_prefix:
        msg = msg_prefix + msg
    if "configured timeouts" not in msg:
        msg += format_timeout_details(timeout_details)
    if isinstance(error, socket.timeout) or (
        isinstance(error, SSLErrors) and "timed out" in str(error)
    ):
        raise NetworkTimeout(msg) from error
    else:
        raise AutoReconnect(msg) from error


class _CancellationContext:
    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        """Cancel this context."""
        self._cancelled = True

    @property
    def cancelled(self) -> bool:
        """Was cancel called?"""
        return self._cancelled


class _PoolClosedError(PyMongoError):
    """Internal error raised when a thread tries to get a connection from a
    closed pool.
    """


class _PoolGeneration:
    def __init__(self) -> None:
        # Maps service_id to generation.
        self._generations: dict[ObjectId, int] = collections.defaultdict(int)
        # Overall pool generation.
        self._generation = 0

    def get(self, service_id: Optional[ObjectId]) -> int:
        """Get the generation for the given service_id."""
        if service_id is None:
            return self._generation
        return self._generations[service_id]

    def get_overall(self) -> int:
        """Get the Pool's overall generation."""
        return self._generation

    def inc(self, service_id: Optional[ObjectId]) -> None:
        """Increment the generation for the given service_id."""
        self._generation += 1
        if service_id is None:
            for service_id in self._generations:
                self._generations[service_id] += 1
        else:
            self._generations[service_id] += 1

    def stale(self, gen: int, service_id: Optional[ObjectId]) -> bool:
        """Return if the given generation for a given service_id is stale."""
        return gen != self.get(service_id)


class PoolState:
    PAUSED = 1
    READY = 2
    CLOSED = 3


async def _async_create_connection(address: _Address, options: PoolOptions) -> socket.socket:
    """Given (host, port) and PoolOptions, connect and return a raw socket object.

    Can raise socket.error.

    This is a modified version of create_connection from CPython >= 2.7.
    """
    host, port = address

    # Check if dealing with a unix domain socket
    if host.endswith(".sock"):
        if not hasattr(socket, "AF_UNIX"):
            raise ConnectionFailure("UNIX-sockets are not supported on this system")
        sock = socket.socket(socket.AF_UNIX)
        # SOCK_CLOEXEC not supported for Unix sockets.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.setblocking(False)
            await asyncio.get_running_loop().sock_connect(sock, host)
            return sock
        except OSError:
            sock.close()
            raise

    # Don't try IPv6 if we don't support it. Also skip it if host
    # is 'localhost' (::1 is fine). Avoids slow connect issues
    # like PYTHON-356.
    family = socket.AF_INET
    if socket.has_ipv6 and host != "localhost":
        family = socket.AF_UNSPEC

    err = None
    for res in await _getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM):
        af, socktype, proto, dummy, sa = res
        # SOCK_CLOEXEC was new in CPython 3.2, and only available on a limited
        # number of platforms (newer Linux and *BSD). Starting with CPython 3.4
        # all file descriptors are created non-inheritable. See PEP 446.
        try:
            sock = socket.socket(af, socktype | getattr(socket, "SOCK_CLOEXEC", 0), proto)
        except OSError:
            # Can SOCK_CLOEXEC be defined even if the kernel doesn't support
            # it?
            sock = socket.socket(af, socktype, proto)
        # Fallback when SOCK_CLOEXEC isn't available.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # CSOT: apply timeout to socket connect.
            timeout = _csot.remaining()
            if timeout is None:
                timeout = options.connect_timeout
            elif timeout <= 0:
                raise socket.timeout("timed out")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            _set_keepalive_times(sock)
            # Socket needs to be non-blocking during connection to not block the event loop
            sock.setblocking(False)
            await asyncio.wait_for(
                asyncio.get_running_loop().sock_connect(sock, sa), timeout=timeout
            )
            sock.settimeout(timeout)
            return sock
        except asyncio.TimeoutError as e:
            sock.close()
            err = socket.timeout("timed out")
            err.__cause__ = e
        except OSError as e:
            sock.close()
            err = e  # type: ignore[assignment]

    if err is not None:
        raise err
    else:
        # This likely means we tried to connect to an IPv6 only
        # host with an OS/kernel or Python interpreter that doesn't
        # support IPv6.
        raise OSError("getaddrinfo failed")


def _close_late_socket(future: asyncio.Future[Any]) -> None:
    """Close a socket produced after its awaiting task was cancelled."""
    if not future.cancelled() and future.exception() is None:
        # The callback may have returned a non-socket; close best effort.
        close = getattr(future.result(), "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()


async def _async_wrap_socket_tls(
    sock: socket.socket, address: _Address, options: PoolOptions
) -> Union[socket.socket, _sslConn]:
    """Given a connected socket, (host, port), and PoolOptions, apply TLS.

    The handshake, SNI, and certificate/hostname verification all target
    ``address``, which may differ from the peer ``sock`` is connected to, e.g.
    when ``sock`` tunnels through an HTTP proxy.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets the socket's SSL and timeout options.
    """
    ssl_context = options._ssl_context

    if ssl_context is None:
        sock.settimeout(options.socket_timeout)
        return sock

    host = address[0]
    try:
        # We have to pass hostname / ip address to wrap_socket
        # to use SSLContext.check_hostname.
        if _has_sni(False):
            loop = asyncio.get_running_loop()
            wrap = functools.partial(ssl_context.wrap_socket, sock, server_hostname=host)  # type: ignore[assignment, misc, unused-ignore]
        else:
            loop = asyncio.get_running_loop()
            wrap = functools.partial(ssl_context.wrap_socket, sock)  # type: ignore[assignment, misc, unused-ignore]
        # Shield the executor future: wrap_socket hands the fd to a new
        # SSLSocket, so cancellation must not orphan the result it produces.
        future = loop.run_in_executor(None, wrap)
        try:
            ssl_sock = await asyncio.shield(future)
        except asyncio.CancelledError:
            future.add_done_callback(_close_late_socket)
            sock.close()
            raise
    except _CertificateError:
        sock.close()
        # Raise _CertificateError directly like we do after match_hostname
        # below.
        raise
    except (OSError, *SSLErrors) as exc:
        sock.close()
        # We raise AutoReconnect for transient and permanent SSL handshake
        # failures alike. Permanent handshake failures, like protocol
        # mismatch, will be turned into ServerSelectionTimeoutErrors later.
        details = _get_timeout_details(options)
        _raise_connection_failure(address, exc, "SSL handshake failed: ", timeout_details=details)
    if (
        ssl_context.verify_mode
        and not ssl_context.check_hostname
        and not options.tls_allow_invalid_hostnames
    ):
        try:
            ssl.match_hostname(ssl_sock.getpeercert(), hostname=host)  # type:ignore[attr-defined, unused-ignore]
        except _CertificateError:
            ssl_sock.close()
            raise

    ssl_sock.settimeout(options.socket_timeout)
    return ssl_sock


async def _async_configured_socket(
    address: _Address, options: PoolOptions
) -> Union[socket.socket, _sslConn]:
    """Given (host, port) and PoolOptions, return a raw configured socket.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets socket's SSL and timeout options.
    """
    sock = await _async_create_connection(address, options)
    return await _async_wrap_socket_tls(sock, address, options)


async def _configured_protocol_interface(
    address: _Address,
    options: PoolOptions,
    ssl_session_cache: Optional[list[Any]] = None,
) -> AsyncNetworkingInterface:
    """Given (host, port) and PoolOptions, return a configured AsyncNetworkingInterface.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets protocol's SSL and timeout options.
    """
    sock = await _async_create_connection(address, options)
    ssl_context = options._ssl_context
    timeout = options.socket_timeout

    if ssl_context is None:
        return AsyncNetworkingInterface(
            await asyncio.get_running_loop().create_connection(
                lambda: PyMongoProtocol(timeout=timeout), sock=sock
            )
        )

    host = address[0]
    # asyncio does not support TLS session resumption natively (cpython#79152,
    # closed without a fix).  SSLProtocol.__init__ calls wrap_bio()
    # synchronously before the first event-loop yield, so setting
    # sslobject_class is race-free.  (The async path always uses stdlib ssl,
    # never PyOpenSSL.)
    if ssl_session_cache is not None:
        session = ssl_session_cache[0]
        if session is not None:
            _session = session

            class _SessionSSLObject(ssl.SSLObject):
                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.session = _session

            ssl_context.sslobject_class = _SessionSSLObject  # type: ignore[attr-defined]
    try:
        # We have to pass hostname / ip address to wrap_socket
        # to use SSLContext.check_hostname.
        transport, protocol = await asyncio.get_running_loop().create_connection(  # type: ignore[call-overload]
            lambda: PyMongoProtocol(timeout=timeout),
            sock=sock,
            server_hostname=host,
            ssl=ssl_context,
        )
    except _CertificateError:
        # Raise _CertificateError directly like we do after match_hostname
        # below.
        raise
    except (OSError, *SSLErrors) as exc:
        # We raise AutoReconnect for transient and permanent SSL handshake
        # failures alike. Permanent handshake failures, like protocol
        # mismatch, will be turned into ServerSelectionTimeoutErrors later.
        details = _get_timeout_details(options)
        _raise_connection_failure(address, exc, "SSL handshake failed: ", timeout_details=details)

    if (
        ssl_context.verify_mode
        and not ssl_context.check_hostname
        and not options.tls_allow_invalid_hostnames
    ):
        try:
            ssl.match_hostname(transport.get_extra_info("peercert"), hostname=host)  # type:ignore[attr-defined,unused-ignore]
        except _CertificateError:
            transport.abort()
            raise

    if ssl_session_cache is not None:
        ssl_obj = transport.get_extra_info("ssl_object")
        if ssl_obj is not None:
            new_session = ssl_obj.session
            if new_session is not None:
                ssl_session_cache[0] = new_session

    return AsyncNetworkingInterface((transport, protocol))


def _create_connection(address: _Address, options: PoolOptions) -> socket.socket:
    """Given (host, port) and PoolOptions, connect and return a raw socket object.

    Can raise socket.error.

    This is a modified version of create_connection from CPython >= 2.7.
    """
    host, port = address

    # Check if dealing with a unix domain socket
    if host.endswith(".sock"):
        if not hasattr(socket, "AF_UNIX"):
            raise ConnectionFailure("UNIX-sockets are not supported on this system")
        sock = socket.socket(socket.AF_UNIX)
        # SOCK_CLOEXEC not supported for Unix sockets.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.connect(host)
            return sock
        except OSError:
            sock.close()
            raise

    # Don't try IPv6 if we don't support it. Also skip it if host
    # is 'localhost' (::1 is fine). Avoids slow connect issues
    # like PYTHON-356.
    family = socket.AF_INET
    if socket.has_ipv6 and host != "localhost":
        family = socket.AF_UNSPEC

    err = None
    for res in socket.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM):  # type: ignore[attr-defined, unused-ignore]
        af, socktype, proto, dummy, sa = res
        # SOCK_CLOEXEC was new in CPython 3.2, and only available on a limited
        # number of platforms (newer Linux and *BSD). Starting with CPython 3.4
        # all file descriptors are created non-inheritable. See PEP 446.
        try:
            sock = socket.socket(af, socktype | getattr(socket, "SOCK_CLOEXEC", 0), proto)
        except OSError:
            # Can SOCK_CLOEXEC be defined even if the kernel doesn't support
            # it?
            sock = socket.socket(af, socktype, proto)
        # Fallback when SOCK_CLOEXEC isn't available.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # CSOT: apply timeout to socket connect.
            timeout = _csot.remaining()
            if timeout is None:
                timeout = options.connect_timeout
            elif timeout <= 0:
                raise socket.timeout("timed out")
            sock.settimeout(timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            _set_keepalive_times(sock)
            sock.connect(sa)
            return sock
        except OSError as e:
            err = e
            sock.close()

    if err is not None:
        raise err
    else:
        # This likely means we tried to connect to an IPv6 only
        # host with an OS/kernel or Python interpreter that doesn't
        # support IPv6.
        raise OSError("getaddrinfo failed")


def _wrap_socket_tls(
    sock: socket.socket, address: _Address, options: PoolOptions
) -> Union[socket.socket, _sslConn]:
    """Given a connected socket, (host, port), and PoolOptions, apply TLS.

    The handshake, SNI, and certificate/hostname verification all target
    ``address``, which may differ from the peer ``sock`` is connected to, e.g.
    when ``sock`` tunnels through an HTTP proxy.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets the socket's SSL and timeout options.
    """
    ssl_context = options._ssl_context

    if ssl_context is None:
        sock.settimeout(options.socket_timeout)
        return sock

    host = address[0]
    try:
        # We have to pass hostname / ip address to wrap_socket
        # to use SSLContext.check_hostname.
        if _has_sni(True):
            ssl_sock = ssl_context.wrap_socket(sock, server_hostname=host)  # type: ignore[assignment, misc, unused-ignore]
        else:
            ssl_sock = ssl_context.wrap_socket(sock)  # type: ignore[assignment, misc, unused-ignore]
    except _CertificateError:
        sock.close()
        # Raise _CertificateError directly like we do after match_hostname
        # below.
        raise
    except (OSError, *SSLErrors) as exc:
        sock.close()
        # We raise AutoReconnect for transient and permanent SSL handshake
        # failures alike. Permanent handshake failures, like protocol
        # mismatch, will be turned into ServerSelectionTimeoutErrors later.
        details = _get_timeout_details(options)
        _raise_connection_failure(address, exc, "SSL handshake failed: ", timeout_details=details)
    if (
        ssl_context.verify_mode
        and not ssl_context.check_hostname
        and not options.tls_allow_invalid_hostnames
    ):
        try:
            ssl.match_hostname(ssl_sock.getpeercert(), hostname=host)  # type:ignore[attr-defined, unused-ignore]
        except _CertificateError:
            ssl_sock.close()
            raise

    ssl_sock.settimeout(options.socket_timeout)
    return ssl_sock


def _configured_socket(address: _Address, options: PoolOptions) -> Union[socket.socket, _sslConn]:
    """Given (host, port) and PoolOptions, return a raw configured socket.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets socket's SSL and timeout options.
    """
    sock = _create_connection(address, options)
    return _wrap_socket_tls(sock, address, options)


def _configured_socket_interface(
    address: _Address,
    options: PoolOptions,
    ssl_session_cache: Optional[list[Any]] = None,
) -> NetworkingInterface:
    """Given (host, port) and PoolOptions, return a NetworkingInterface wrapping a configured socket.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets socket's SSL and timeout options.
    """
    sock = _create_connection(address, options)
    ssl_context = options._ssl_context

    if ssl_context is None:
        sock.settimeout(options.socket_timeout)
        return NetworkingInterface(sock)

    host = address[0]
    session = ssl_session_cache[0] if ssl_session_cache is not None else None
    try:
        # We have to pass hostname / ip address to wrap_socket
        # to use SSLContext.check_hostname.
        if _has_sni(True):
            ssl_sock = ssl_context.wrap_socket(sock, server_hostname=host, session=session)
        else:
            ssl_sock = ssl_context.wrap_socket(sock, session=session)
    except _CertificateError:
        sock.close()
        # Raise _CertificateError directly like we do after match_hostname
        # below.
        raise
    except (OSError, *SSLErrors) as exc:
        sock.close()
        # We raise AutoReconnect for transient and permanent SSL handshake
        # failures alike. Permanent handshake failures, like protocol
        # mismatch, will be turned into ServerSelectionTimeoutErrors later.
        details = _get_timeout_details(options)
        _raise_connection_failure(address, exc, "SSL handshake failed: ", timeout_details=details)
    if (
        ssl_context.verify_mode
        and not ssl_context.check_hostname
        and not options.tls_allow_invalid_hostnames
    ):
        try:
            ssl.match_hostname(ssl_sock.getpeercert(), hostname=host)  # type:ignore[attr-defined,unused-ignore]
        except _CertificateError:
            ssl_sock.close()
            raise

    if ssl_session_cache is not None:
        new_session = _get_ssl_session(ssl_sock)
        if new_session is not None:
            ssl_session_cache[0] = new_session

    ssl_sock.settimeout(options.socket_timeout)
    return NetworkingInterface(ssl_sock)
