# Copyright 2024-present MongoDB, Inc.
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

"""Shared constants and helper methods for pymongo, bson, and gridfs test suites."""
from __future__ import annotations

import asyncio
import base64
import gc
import multiprocessing
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import unittest
import warnings
from asyncio import iscoroutinefunction

from pymongo._asyncio_task import create_task

try:
    import ipaddress

    HAVE_IPADDRESS = True
except ImportError:
    HAVE_IPADDRESS = False
from functools import wraps
from typing import Any, Callable, Dict, Generator, Optional, no_type_check
from unittest import SkipTest

from bson.son import SON
from pymongo import common, message
from pymongo.read_preferences import ReadPreference
from pymongo.ssl_support import HAVE_SSL, _ssl  # type:ignore[attr-defined]
from pymongo.synchronous.uri_parser import parse_uri

if HAVE_SSL:
    import ssl

_IS_SYNC = False

# Enable debug output for uncollectable objects. PyPy does not have set_debug.
if hasattr(gc, "set_debug"):
    gc.set_debug(
        gc.DEBUG_UNCOLLECTABLE | getattr(gc, "DEBUG_OBJECTS", 0) | getattr(gc, "DEBUG_INSTANCES", 0)
    )

# The host and port of a single mongod or mongos, or the seed host
# for a replica set.
host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))
IS_SRV = "mongodb+srv" in host

db_user = os.environ.get("DB_USER", "user")
db_pwd = os.environ.get("DB_PASSWORD", "password")

CERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "certificates")
CLIENT_PEM = os.environ.get("CLIENT_PEM", os.path.join(CERT_PATH, "client.pem"))
CA_PEM = os.environ.get("CA_PEM", os.path.join(CERT_PATH, "ca.pem"))

TLS_OPTIONS: Dict = {"tls": True}
if CLIENT_PEM:
    TLS_OPTIONS["tlsCertificateKeyFile"] = CLIENT_PEM
if CA_PEM:
    TLS_OPTIONS["tlsCAFile"] = CA_PEM

COMPRESSORS = os.environ.get("COMPRESSORS")
MONGODB_API_VERSION = os.environ.get("MONGODB_API_VERSION")
TEST_LOADBALANCER = bool(os.environ.get("TEST_LOAD_BALANCER"))
TEST_SERVERLESS = bool(os.environ.get("TEST_SERVERLESS"))
SINGLE_MONGOS_LB_URI = os.environ.get("SINGLE_MONGOS_LB_URI")
MULTI_MONGOS_LB_URI = os.environ.get("MULTI_MONGOS_LB_URI")

if TEST_LOADBALANCER:
    res = parse_uri(SINGLE_MONGOS_LB_URI or "")
    host, port = res["nodelist"][0]
    db_user = res["username"] or db_user
    db_pwd = res["password"] or db_pwd
elif TEST_SERVERLESS:
    TEST_LOADBALANCER = True
    res = parse_uri(SINGLE_MONGOS_LB_URI or "")
    host, port = res["nodelist"][0]
    db_user = res["username"] or db_user
    db_pwd = res["password"] or db_pwd
    TLS_OPTIONS = {"tls": True}
    # Spec says serverless tests must be run with compression.
    COMPRESSORS = COMPRESSORS or "zlib"


# Shared KMS data.
LOCAL_MASTER_KEY = base64.b64decode(
    b"Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ"
    b"5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"
)
AWS_CREDS = {
    "accessKeyId": os.environ.get("FLE_AWS_KEY", ""),
    "secretAccessKey": os.environ.get("FLE_AWS_SECRET", ""),
}
AWS_CREDS_2 = {
    "accessKeyId": os.environ.get("FLE_AWS_KEY2", ""),
    "secretAccessKey": os.environ.get("FLE_AWS_SECRET2", ""),
}
AZURE_CREDS = {
    "tenantId": os.environ.get("FLE_AZURE_TENANTID", ""),
    "clientId": os.environ.get("FLE_AZURE_CLIENTID", ""),
    "clientSecret": os.environ.get("FLE_AZURE_CLIENTSECRET", ""),
}
GCP_CREDS = {
    "email": os.environ.get("FLE_GCP_EMAIL", ""),
    "privateKey": os.environ.get("FLE_GCP_PRIVATEKEY", ""),
}
KMIP_CREDS = {"endpoint": os.environ.get("FLE_KMIP_ENDPOINT", "localhost:5698")}

# Ensure Evergreen metadata doesn't result in truncation
os.environ.setdefault("MONGOB_LOG_MAX_DOCUMENT_LENGTH", "2000")


def is_server_resolvable():
    """Returns True if 'server' is resolvable."""
    socket_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(1)
    try:
        try:
            socket.gethostbyname("server")
            return True
        except OSError:
            return False
    finally:
        socket.setdefaulttimeout(socket_timeout)


def _create_user(authdb, user, pwd=None, roles=None, **kwargs):
    cmd = SON([("createUser", user)])
    # X509 doesn't use a password
    if pwd:
        cmd["pwd"] = pwd
    cmd["roles"] = roles or ["root"]
    cmd.update(**kwargs)
    return authdb.command(cmd)


async def async_repl_set_step_down(client, **kwargs):
    """Run replSetStepDown, first unfreezing a secondary with replSetFreeze."""
    cmd = SON([("replSetStepDown", 1)])
    cmd.update(kwargs)

    # Unfreeze a secondary to ensure a speedy election.
    await client.admin.command("replSetFreeze", 0, read_preference=ReadPreference.SECONDARY)
    await client.admin.command(cmd)


class client_knobs:
    def __init__(
        self,
        heartbeat_frequency=None,
        min_heartbeat_interval=None,
        kill_cursor_frequency=None,
        events_queue_frequency=None,
    ):
        self.heartbeat_frequency = heartbeat_frequency
        self.min_heartbeat_interval = min_heartbeat_interval
        self.kill_cursor_frequency = kill_cursor_frequency
        self.events_queue_frequency = events_queue_frequency

        self.old_heartbeat_frequency = None
        self.old_min_heartbeat_interval = None
        self.old_kill_cursor_frequency = None
        self.old_events_queue_frequency = None
        self._enabled = False
        self._stack = None

    def enable(self):
        self.old_heartbeat_frequency = common.HEARTBEAT_FREQUENCY
        self.old_min_heartbeat_interval = common.MIN_HEARTBEAT_INTERVAL
        self.old_kill_cursor_frequency = common.KILL_CURSOR_FREQUENCY
        self.old_events_queue_frequency = common.EVENTS_QUEUE_FREQUENCY

        if self.heartbeat_frequency is not None:
            common.HEARTBEAT_FREQUENCY = self.heartbeat_frequency

        if self.min_heartbeat_interval is not None:
            common.MIN_HEARTBEAT_INTERVAL = self.min_heartbeat_interval

        if self.kill_cursor_frequency is not None:
            common.KILL_CURSOR_FREQUENCY = self.kill_cursor_frequency

        if self.events_queue_frequency is not None:
            common.EVENTS_QUEUE_FREQUENCY = self.events_queue_frequency
        self._enabled = True
        # Store the allocation traceback to catch non-disabled client_knobs.
        self._stack = "".join(traceback.format_stack())

    def __enter__(self):
        self.enable()

    @no_type_check
    def disable(self):
        common.HEARTBEAT_FREQUENCY = self.old_heartbeat_frequency
        common.MIN_HEARTBEAT_INTERVAL = self.old_min_heartbeat_interval
        common.KILL_CURSOR_FREQUENCY = self.old_kill_cursor_frequency
        common.EVENTS_QUEUE_FREQUENCY = self.old_events_queue_frequency
        self._enabled = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()

    def __call__(self, func):
        def make_wrapper(f):
            @wraps(f)
            async def wrap(*args, **kwargs):
                with self:
                    return await f(*args, **kwargs)

            return wrap

        return make_wrapper(func)

    def __del__(self):
        if self._enabled:
            msg = (
                "ERROR: client_knobs still enabled! HEARTBEAT_FREQUENCY={}, "
                "MIN_HEARTBEAT_INTERVAL={}, KILL_CURSOR_FREQUENCY={}, "
                "EVENTS_QUEUE_FREQUENCY={}, stack:\n{}".format(
                    common.HEARTBEAT_FREQUENCY,
                    common.MIN_HEARTBEAT_INTERVAL,
                    common.KILL_CURSOR_FREQUENCY,
                    common.EVENTS_QUEUE_FREQUENCY,
                    self._stack,
                )
            )
            self.disable()
            raise Exception(msg)


def _all_users(db):
    return {u["user"] for u in db.command("usersInfo").get("users", [])}


def sanitize_cmd(cmd):
    cp = cmd.copy()
    cp.pop("$clusterTime", None)
    cp.pop("$db", None)
    cp.pop("$readPreference", None)
    cp.pop("lsid", None)
    if MONGODB_API_VERSION:
        # Stable API parameters
        cp.pop("apiVersion", None)
    # OP_MSG encoding may move the payload type one field to the
    # end of the command. Do the same here.
    name = next(iter(cp))
    try:
        identifier = message._FIELD_MAP[name]
        docs = cp.pop(identifier)
        cp[identifier] = docs
    except KeyError:
        pass
    return cp


def sanitize_reply(reply):
    cp = reply.copy()
    cp.pop("$clusterTime", None)
    cp.pop("operationTime", None)
    return cp


def print_thread_tracebacks() -> None:
    """Print all Python thread tracebacks."""
    for thread_id, frame in sys._current_frames().items():
        sys.stderr.write(f"\n--- Traceback for thread {thread_id} ---\n")
        traceback.print_stack(frame, file=sys.stderr)


def print_thread_stacks(pid: int) -> None:
    """Print all C-level thread stacks for a given process id."""
    if sys.platform == "darwin":
        cmd = ["lldb", "--attach-pid", f"{pid}", "--batch", "--one-line", '"thread backtrace all"']
    else:
        cmd = ["gdb", f"--pid={pid}", "--batch", '--eval-command="thread apply all bt"']

    try:
        res = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8"
        )
    except Exception as exc:
        sys.stderr.write(f"Could not print C-level thread stacks because {cmd[0]} failed: {exc}")
    else:
        sys.stderr.write(res.stdout)


# Global knobs to speed up the test suite.
global_knobs = client_knobs(events_queue_frequency=0.05)


def _get_executors(topology):
    executors = []
    for server in topology._servers.values():
        # Some MockMonitor do not have an _executor.
        if hasattr(server._monitor, "_executor"):
            executors.append(server._monitor._executor)
        if hasattr(server._monitor, "_rtt_monitor"):
            executors.append(server._monitor._rtt_monitor._executor)
    executors.append(topology._Topology__events_executor)
    if topology._srv_monitor:
        executors.append(topology._srv_monitor._executor)

    return [e for e in executors if e is not None]


def print_running_topology(topology):
    running = [e for e in _get_executors(topology) if not e._stopped]
    if running:
        print(
            "WARNING: found Topology with running threads:\n"
            f"  Threads: {running}\n"
            f"  Topology: {topology}\n"
            f"  Creation traceback:\n{topology._settings._stack}"
        )


def test_cases(suite):
    """Iterator over all TestCases within a TestSuite."""
    for suite_or_case in suite._tests:
        if isinstance(suite_or_case, unittest.TestCase):
            # unittest.TestCase
            yield suite_or_case
        else:
            # unittest.TestSuite
            yield from test_cases(suite_or_case)


# Helper method to workaround https://bugs.python.org/issue21724
def clear_warning_registry():
    """Clear the __warningregistry__ for all modules."""
    for _, module in list(sys.modules.items()):
        if hasattr(module, "__warningregistry__"):
            module.__warningregistry__ = {}  # type:ignore[attr-defined]


class SystemCertsPatcher:
    def __init__(self, ca_certs):
        if (
            ssl.OPENSSL_VERSION.lower().startswith("libressl")
            and sys.platform == "darwin"
            and not _ssl.IS_PYOPENSSL
        ):
            raise SkipTest(
                "LibreSSL on OSX doesn't support setting CA certificates "
                "using SSL_CERT_FILE environment variable."
            )
        self.original_certs = os.environ.get("SSL_CERT_FILE")
        # Tell OpenSSL where CA certificates live.
        os.environ["SSL_CERT_FILE"] = ca_certs

    def disable(self):
        if self.original_certs is None:
            os.environ.pop("SSL_CERT_FILE")
        else:
            os.environ["SSL_CERT_FILE"] = self.original_certs


if _IS_SYNC:
    PARENT = threading.Thread
else:
    PARENT = object


class ConcurrentRunner(PARENT):
    def __init__(self, **kwargs):
        if _IS_SYNC:
            super().__init__(**kwargs)
        self.name = kwargs.get("name", "ConcurrentRunner")
        self.stopped = False
        self.task = None
        self.target = kwargs.get("target", None)
        self.args = kwargs.get("args", [])

    if not _IS_SYNC:

        async def start(self):
            self.task = create_task(self.run(), name=self.name)

        async def join(self, timeout: Optional[float] = None):  # type: ignore[override]
            if self.task is not None:
                await asyncio.wait([self.task], timeout=timeout)

        def is_alive(self):
            return not self.stopped

    async def run(self):
        try:
            await self.target(*self.args)
        finally:
            self.stopped = True


class ExceptionCatchingTask(ConcurrentRunner):
    """A Task that stores any exception encountered while running."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.exc = None

    async def run(self):
        try:
            await super().run()
        except BaseException as exc:
            self.exc = exc
            raise
