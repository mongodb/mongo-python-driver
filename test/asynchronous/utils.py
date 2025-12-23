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
import os
import random
import sys
import threading  # Used in the synchronized version of this file
import time
import traceback
from functools import wraps
from inspect import iscoroutinefunction

from bson.son import SON
from pymongo import AsyncMongoClient
from pymongo.asynchronous.pool import Pool, _CancellationContext, _PoolGeneration
from pymongo.errors import ConfigurationError
from pymongo.hello import HelloCompat
from pymongo.lock import _async_create_lock
from pymongo.operations import _Op
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import any_server_selector, writable_server_selector

_IS_SYNC = False


async def async_get_pool(client: AsyncMongoClient) -> Pool:
    """Get the standalone, primary, or mongos pool."""
    topology = await client._get_topology()
    server = await topology._select_server(writable_server_selector, _Op.TEST)
    return server.pool


async def async_get_pools(client: AsyncMongoClient) -> list[Pool]:
    """Get all pools."""
    return [
        server.pool
        for server in await (await client._get_topology()).select_servers(
            any_server_selector, _Op.TEST
        )
    ]


async def async_wait_until(predicate, success_description, timeout=10):
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
            retval = await predicate()
        else:
            retval = predicate()
        if retval:
            return retval

        if time.time() - start > timeout:
            raise AssertionError("Didn't ever %s" % success_description)

        await asyncio.sleep(interval)


async def async_is_mongos(client):
    res = await client.admin.command(HelloCompat.LEGACY_CMD)
    return res.get("msg", "") == "isdbgrid"


async def async_ensure_all_connected(client: AsyncMongoClient) -> None:
    """Ensure that the client's connection pool has socket connections to all
    members of a replica set. Raises ConfigurationError when called with a
    non-replica set client.

    Depending on the use-case, the caller may need to clear any event listeners
    that are configured on the client.
    """
    hello: dict = await client.admin.command(HelloCompat.LEGACY_CMD)
    if "setName" not in hello:
        raise ConfigurationError("cluster is not a replica set")

    target_host_list = set(hello["hosts"] + hello.get("passives", []))
    connected_host_list = {hello["me"]}

    # Run hello until we have connected to each host at least once.
    async def discover():
        i = 0
        while i < 100 and connected_host_list != target_host_list:
            hello: dict = await client.admin.command(
                HelloCompat.LEGACY_CMD, read_preference=ReadPreference.SECONDARY
            )
            connected_host_list.update([hello["me"]])
            i += 1
        return connected_host_list

    try:

        async def predicate():
            return target_host_list == await discover()

        await async_wait_until(predicate, "connected to all hosts")
    except AssertionError as exc:
        raise AssertionError(
            f"{exc}, {connected_host_list} != {target_host_list}, {client.topology_description}"
        )


async def asyncAssertRaisesExactly(cls, fn, *args, **kwargs):
    """
    Unlike the standard assertRaises, this checks that a function raises a
    specific class of exception, and not a subclass. E.g., check that
    MongoClient() raises ConnectionFailure but not its subclass, AutoReconnect.
    """
    try:
        await fn(*args, **kwargs)
    except Exception as e:
        assert e.__class__ == cls, f"got {e.__class__.__name__}, expected {cls.__name__}"
    else:
        raise AssertionError("%s not raised" % cls)


async def async_set_fail_point(client, command_args):
    cmd = SON([("configureFailPoint", "failCommand")])
    cmd.update(command_args)
    await client.admin.command(cmd)


async def async_joinall(tasks):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    if _IS_SYNC:
        for t in tasks:
            t.join(300)
            assert not t.is_alive(), "Thread %s hung" % t
    else:
        await asyncio.wait([t.task for t in tasks if t is not None], timeout=300)


def flaky(
    *,
    reason=None,
    max_runs=2,
    min_passes=1,
    delay=1,
    affects_cpython_linux=False,
    func_name=None,
    reset_func=None,
):
    """Decorate a test as flaky.

    :param reason: the reason why the test is flaky
    :param max_runs: the maximum number of runs before raising an error
    :param min_passes: the minimum number of passing runs
    :param delay: the delay in seconds between retries
    :param affects_cpython_links: whether the test is flaky on CPython on Linux
    :param func_name: the name of the function, used for the rety message
    :param reset_func: a function to call before retrying

    """
    if reason is None:
        raise ValueError("flaky requires a reason input")
    is_cpython_linux = sys.platform == "linux" and sys.implementation.name == "cpython"
    disable_flaky = "DISABLE_FLAKY" in os.environ
    if "CI" not in os.environ and "ENABLE_FLAKY" not in os.environ:
        disable_flaky = True

    if disable_flaky or (is_cpython_linux and not affects_cpython_linux):
        max_runs = 1
        min_passes = 1

    def decorator(target_func):
        @wraps(target_func)
        async def wrapper(*args, **kwargs):
            passes = 0
            for i in range(max_runs):
                try:
                    result = await target_func(*args, **kwargs)
                    passes += 1
                    if passes == min_passes:
                        return result
                except Exception as e:
                    if i == max_runs - 1:
                        raise e
                    print(
                        f"Retrying after attempt {i+1} of {func_name or target_func.__name__} failed with ({reason})):\n"
                        f"{traceback.format_exc()}",
                        file=sys.stderr,
                    )
                    await asyncio.sleep(delay)
                    if reset_func:
                        await reset_func()

        return wrapper

    return decorator


class AsyncMockConnection:
    def __init__(self):
        self.cancel_context = _CancellationContext()
        self.more_to_come = False
        self.id = random.randint(0, 100)
        self.is_sdam = False
        self.server_connection_id = random.randint(0, 100)

    def close_conn(self, reason):
        pass

    def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class AsyncMockPool:
    def __init__(self, address, options, is_sdam=False, client_id=None):
        self.gen = _PoolGeneration()
        self._lock = _async_create_lock()
        self.opts = options
        self.operation_count = 0
        self.conns = []

    def stale_generation(self, gen, service_id):
        return self.gen.stale(gen, service_id)

    @contextlib.asynccontextmanager
    async def checkout(self, handler=None):
        yield AsyncMockConnection()

    async def checkin(self, *args, **kwargs):
        pass

    async def _reset(self, service_id=None):
        async with self._lock:
            self.gen.inc(service_id)

    async def ready(self):
        pass

    async def reset(self, service_id=None, interrupt_connections=False):
        await self._reset()

    async def reset_without_pause(self):
        await self._reset()

    async def close(self):
        await self._reset()

    async def update_is_writable(self, is_writable):
        pass

    async def remove_stale_sockets(self, *args, **kwargs):
        pass
