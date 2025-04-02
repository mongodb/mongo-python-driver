# Copyright 2012-present MongoDB, Inc.
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

"""Utilities for testing pymongo that require synchronization."""
from __future__ import annotations

import asyncio
import contextlib
import random
import threading  # Used in the synchronized version of this file
import time
from asyncio import iscoroutinefunction

from bson.son import SON
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from pymongo.hello import HelloCompat
from pymongo.lock import _create_lock
from pymongo.operations import _Op
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import any_server_selector, writable_server_selector
from pymongo.synchronous.pool import _CancellationContext, _PoolGeneration

_IS_SYNC = True


def get_pool(client):
    """Get the standalone, primary, or mongos pool."""
    topology = client._get_topology()
    server = topology._select_server(writable_server_selector, _Op.TEST)
    return server.pool


def get_pools(client):
    """Get all pools."""
    return [
        server.pool
        for server in (client._get_topology()).select_servers(any_server_selector, _Op.TEST)
    ]


def wait_until(predicate, success_description, timeout=10):
    """Wait up to 10 seconds (by default) for predicate to be true.

    E.g.:

        wait_until(lambda: client.primary == ('a', 1),
                   'connect to the primary')

    If the lambda-expression isn't true after 10 seconds, we raise
    AssertionError("Didn't ever connect to the primary").

    Returns the predicate's first true value.
    """
    start = time.time()
    interval = min(float(timeout) / 100, 0.1)
    while True:
        if iscoroutinefunction(predicate):
            retval = predicate()
        else:
            retval = predicate()
        if retval:
            return retval

        if time.time() - start > timeout:
            raise AssertionError("Didn't ever %s" % success_description)

        time.sleep(interval)


def is_mongos(client):
    res = client.admin.command(HelloCompat.LEGACY_CMD)
    return res.get("msg", "") == "isdbgrid"


def ensure_all_connected(client: MongoClient) -> None:
    """Ensure that the client's connection pool has socket connections to all
    members of a replica set. Raises ConfigurationError when called with a
    non-replica set client.

    Depending on the use-case, the caller may need to clear any event listeners
    that are configured on the client.
    """
    hello: dict = client.admin.command(HelloCompat.LEGACY_CMD)
    if "setName" not in hello:
        raise ConfigurationError("cluster is not a replica set")

    target_host_list = set(hello["hosts"] + hello.get("passives", []))
    connected_host_list = {hello["me"]}

    # Run hello until we have connected to each host at least once.
    def discover():
        i = 0
        while i < 100 and connected_host_list != target_host_list:
            hello: dict = client.admin.command(
                HelloCompat.LEGACY_CMD, read_preference=ReadPreference.SECONDARY
            )
            connected_host_list.update([hello["me"]])
            i += 1
        return connected_host_list

    try:

        def predicate():
            return target_host_list == discover()

        wait_until(predicate, "connected to all hosts")
    except AssertionError as exc:
        raise AssertionError(
            f"{exc}, {connected_host_list} != {target_host_list}, {client.topology_description}"
        )


def assertRaisesExactly(cls, fn, *args, **kwargs):
    """
    Unlike the standard assertRaises, this checks that a function raises a
    specific class of exception, and not a subclass. E.g., check that
    MongoClient() raises ConnectionFailure but not its subclass, AutoReconnect.
    """
    try:
        fn(*args, **kwargs)
    except Exception as e:
        assert e.__class__ == cls, f"got {e.__class__.__name__}, expected {cls.__name__}"
    else:
        raise AssertionError("%s not raised" % cls)


def set_fail_point(client, command_args):
    cmd = SON([("configureFailPoint", "failCommand")])
    cmd.update(command_args)
    client.admin.command(cmd)


def joinall(tasks):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    if _IS_SYNC:
        for t in tasks:
            t.join(300)
            assert not t.is_alive(), "Thread %s hung" % t
    else:
        asyncio.wait([t.task for t in tasks if t is not None], timeout=300)


class MockConnection:
    def __init__(self):
        self.cancel_context = _CancellationContext()
        self.more_to_come = False
        self.id = random.randint(0, 100)
        self.server_connection_id = random.randint(0, 100)

    def close_conn(self, reason):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockPool:
    def __init__(self, address, options, handshake=True, client_id=None):
        self.gen = _PoolGeneration()
        self._lock = _create_lock()
        self.opts = options
        self.operation_count = 0
        self.conns = []

    def stale_generation(self, gen, service_id):
        return self.gen.stale(gen, service_id)

    @contextlib.contextmanager
    def checkout(self, handler=None):
        yield MockConnection()

    def checkin(self, *args, **kwargs):
        pass

    def _reset(self, service_id=None):
        with self._lock:
            self.gen.inc(service_id)

    def ready(self):
        pass

    def reset(self, service_id=None, interrupt_connections=False):
        self._reset()

    def reset_without_pause(self):
        self._reset()

    def close(self):
        self._reset()

    def update_is_writable(self, is_writable):
        pass

    def remove_stale_sockets(self, *args, **kwargs):
        pass
