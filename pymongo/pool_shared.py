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

import socket
import sys
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
    Optional,
)

from pymongo import _csot
from pymongo.errors import (  # type:ignore[attr-defined]
    AutoReconnect,
    NetworkTimeout,
)
from pymongo.pool_options import PoolOptions
from pymongo.ssl_support import SSLError

if TYPE_CHECKING:
    pass

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
    def _set_non_inheritable_non_atomic(fd: int) -> None:  # noqa: ARG001
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
        msg = "%s:%d: %s" % (host, port, error)
    else:
        msg = f"{host}: {error}"
    if msg_prefix:
        msg = msg_prefix + msg
    if "configured timeouts" not in msg:
        msg += format_timeout_details(timeout_details)
    if isinstance(error, socket.timeout):
        raise NetworkTimeout(msg) from error
    elif isinstance(error, SSLError) and "timed out" in str(error):
        # Eventlet does not distinguish TLS network timeouts from other
        # SSLErrors (https://github.com/eventlet/eventlet/issues/692).
        # Luckily, we can work around this limitation because the phrase
        # 'timed out' appears in all the timeout related SSLErrors raised.
        raise NetworkTimeout(msg) from error
    else:
        raise AutoReconnect(msg) from error


def _get_timeout_details(options: PoolOptions) -> dict[str, float]:
    details = {}
    timeout = _csot.get_timeout()
    socket_timeout = options.socket_timeout
    connect_timeout = options.connect_timeout
    if timeout:
        details["timeoutMS"] = timeout * 1000
    if socket_timeout and not timeout:
        details["socketTimeoutMS"] = socket_timeout * 1000
    if connect_timeout:
        details["connectTimeoutMS"] = connect_timeout * 1000
    return details


def format_timeout_details(details: Optional[dict[str, float]]) -> str:
    result = ""
    if details:
        result += " (configured timeouts:"
        for timeout in ["socketTimeoutMS", "timeoutMS", "connectTimeoutMS"]:
            if timeout in details:
                result += f" {timeout}: {details[timeout]}ms,"
        result = result[:-1]
        result += ")"
    return result


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
