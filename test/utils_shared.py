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

"""Shared utilities for testing pymongo"""
from __future__ import annotations

import asyncio
import contextlib
import copy
import functools
import random
import re
import shutil
import sys
import threading
import unittest
import warnings
from collections import abc, defaultdict
from functools import partial
from inspect import iscoroutinefunction
from test import client_context
from test.asynchronous.utils import async_wait_until
from test.utils import wait_until
from typing import List

from bson.objectid import ObjectId
from pymongo import monitoring, operations, read_preferences
from pymongo.cursor_shared import CursorType
from pymongo.errors import OperationFailure
from pymongo.helpers_shared import _SENSITIVE_COMMANDS
from pymongo.lock import _async_create_lock, _create_lock
from pymongo.monitoring import (
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckOutStartedEvent,
    ConnectionClosedEvent,
    ConnectionCreatedEvent,
    ConnectionReadyEvent,
    PoolClearedEvent,
    PoolClosedEvent,
    PoolCreatedEvent,
    PoolReadyEvent,
)
from pymongo.read_concern import ReadConcern
from pymongo.server_type import SERVER_TYPE
from pymongo.synchronous.collection import ReturnDocument
from pymongo.synchronous.pool import _CancellationContext, _PoolGeneration
from pymongo.write_concern import WriteConcern

IMPOSSIBLE_WRITE_CONCERN = WriteConcern(w=50)


class BaseListener:
    def __init__(self):
        self.events = []

    def reset(self):
        self.events = []

    def add_event(self, event):
        self.events.append(event)

    def event_count(self, event_type):
        return len(self.events_by_type(event_type))

    def events_by_type(self, event_type):
        """Return the matching events by event class.

        event_type can be a single class or a tuple of classes.
        """
        return self.matching(lambda e: isinstance(e, event_type))

    def matching(self, matcher):
        """Return the matching events."""
        return [event for event in self.events[:] if matcher(event)]

    def wait_for_event(self, event, count):
        """Wait for a number of events to be published, or fail."""
        wait_until(lambda: self.event_count(event) >= count, f"find {count} {event} event(s)")

    async def async_wait_for_event(self, event, count):
        """Wait for a number of events to be published, or fail."""
        await async_wait_until(
            lambda: self.event_count(event) >= count, f"find {count} {event} event(s)"
        )


class CMAPListener(BaseListener, monitoring.ConnectionPoolListener):
    def connection_created(self, event):
        assert isinstance(event, ConnectionCreatedEvent)
        self.add_event(event)

    def connection_ready(self, event):
        assert isinstance(event, ConnectionReadyEvent)
        self.add_event(event)

    def connection_closed(self, event):
        assert isinstance(event, ConnectionClosedEvent)
        self.add_event(event)

    def connection_check_out_started(self, event):
        assert isinstance(event, ConnectionCheckOutStartedEvent)
        self.add_event(event)

    def connection_check_out_failed(self, event):
        assert isinstance(event, ConnectionCheckOutFailedEvent)
        self.add_event(event)

    def connection_checked_out(self, event):
        assert isinstance(event, ConnectionCheckedOutEvent)
        self.add_event(event)

    def connection_checked_in(self, event):
        assert isinstance(event, ConnectionCheckedInEvent)
        self.add_event(event)

    def pool_created(self, event):
        assert isinstance(event, PoolCreatedEvent)
        self.add_event(event)

    def pool_ready(self, event):
        assert isinstance(event, PoolReadyEvent)
        self.add_event(event)

    def pool_cleared(self, event):
        assert isinstance(event, PoolClearedEvent)
        self.add_event(event)

    def pool_closed(self, event):
        assert isinstance(event, PoolClosedEvent)
        self.add_event(event)


class EventListener(BaseListener, monitoring.CommandListener):
    def __init__(self):
        super().__init__()
        self.results = defaultdict(list)

    @property
    def started_events(self) -> List[monitoring.CommandStartedEvent]:
        return self.results["started"]

    @property
    def succeeded_events(self) -> List[monitoring.CommandSucceededEvent]:
        return self.results["succeeded"]

    @property
    def failed_events(self) -> List[monitoring.CommandFailedEvent]:
        return self.results["failed"]

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        self.started_events.append(event)
        self.add_event(event)

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        self.succeeded_events.append(event)
        self.add_event(event)

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        self.failed_events.append(event)
        self.add_event(event)

    def started_command_names(self) -> List[str]:
        """Return list of command names started."""
        return [event.command_name for event in self.started_events]

    def reset(self) -> None:
        """Reset the state of this listener."""
        self.results.clear()
        super().reset()


class TopologyEventListener(monitoring.TopologyListener):
    def __init__(self):
        self.results = defaultdict(list)

    def closed(self, event):
        self.results["closed"].append(event)

    def description_changed(self, event):
        self.results["description_changed"].append(event)

    def opened(self, event):
        self.results["opened"].append(event)

    def reset(self):
        """Reset the state of this listener."""
        self.results.clear()


class AllowListEventListener(EventListener):
    def __init__(self, *commands):
        self.commands = set(commands)
        super().__init__()

    def started(self, event):
        if event.command_name in self.commands:
            super().started(event)

    def succeeded(self, event):
        if event.command_name in self.commands:
            super().succeeded(event)

    def failed(self, event):
        if event.command_name in self.commands:
            super().failed(event)


class OvertCommandListener(EventListener):
    """A CommandListener that ignores sensitive commands."""

    ignore_list_collections = False

    def started(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super().started(event)

    def succeeded(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super().succeeded(event)

    def failed(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super().failed(event)


class _ServerEventListener:
    """Listens to all events."""

    def __init__(self):
        self.results = []

    def opened(self, event):
        self.results.append(event)

    def description_changed(self, event):
        self.results.append(event)

    def closed(self, event):
        self.results.append(event)

    def matching(self, matcher):
        """Return the matching events."""
        results = self.results[:]
        return [event for event in results if matcher(event)]

    def reset(self):
        self.results = []


class ServerEventListener(_ServerEventListener, monitoring.ServerListener):
    """Listens to Server events."""


class ServerAndTopologyEventListener(  # type: ignore[misc]
    ServerEventListener, monitoring.TopologyListener
):
    """Listens to Server and Topology events."""


class HeartbeatEventListener(BaseListener, monitoring.ServerHeartbeatListener):
    """Listens to only server heartbeat events."""

    def started(self, event):
        self.add_event(event)

    def succeeded(self, event):
        self.add_event(event)

    def failed(self, event):
        self.add_event(event)


class HeartbeatEventsListListener(HeartbeatEventListener):
    """Listens to only server heartbeat events and publishes them to a provided list."""

    def __init__(self, events):
        super().__init__()
        self.event_list = events

    def started(self, event):
        self.add_event(event)
        self.event_list.append("serverHeartbeatStartedEvent")

    def succeeded(self, event):
        self.add_event(event)
        self.event_list.append("serverHeartbeatSucceededEvent")

    def failed(self, event):
        self.add_event(event)
        self.event_list.append("serverHeartbeatFailedEvent")


class ScenarioDict(dict):
    """Dict that returns {} for any unknown key, recursively."""

    def __init__(self, data):
        def convert(v):
            if isinstance(v, abc.Mapping):
                return ScenarioDict(v)
            if isinstance(v, (str, bytes)):
                return v
            if isinstance(v, abc.Sequence):
                return [convert(item) for item in v]
            return v

        dict.__init__(self, [(k, convert(v)) for k, v in data.items()])

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            # Unlike a defaultdict, don't set the key, just return a dict.
            return ScenarioDict({})


class CompareType:
    """Class that compares equal to any object of the given type(s)."""

    def __init__(self, types):
        self.types = types

    def __eq__(self, other):
        return isinstance(other, self.types)


class FunctionCallRecorder:
    """Utility class to wrap a callable and record its invocations."""

    def __init__(self, function):
        self._function = function
        self._call_list = []

    def __call__(self, *args, **kwargs):
        self._call_list.append((args, kwargs))
        if iscoroutinefunction(self._function):
            return self._function(*args, **kwargs)
        else:
            return self._function(*args, **kwargs)

    def reset(self):
        """Wipes the call list."""
        self._call_list = []

    def call_list(self):
        """Returns a copy of the call list."""
        return self._call_list[:]

    @property
    def call_count(self):
        """Returns the number of times the function has been called."""
        return len(self._call_list)


def one(s):
    """Get one element of a set"""
    return next(iter(s))


def oid_generated_on_process(oid):
    """Makes a determination as to whether the given ObjectId was generated
    by the current process, based on the 5-byte random number in the ObjectId.
    """
    return ObjectId._random() == oid.binary[4:9]


def delay(sec):
    """Along with a ``$where`` operator, this triggers an arbitrarily long-running
    operation on the server.

    This can be useful in time-sensitive tests (e.g., timeouts, signals).
    Note that you must have at least one document in the collection or the
    server may decide not to sleep at all.

    Example
    -------

    .. code-block:: python

        db.coll.insert_one({"x": 1})
        db.test.find_one({"x": 1})
        # {'x': 1, '_id': ObjectId('54f4e12bfba5220aa4d6dee8')}

        # The following will wait 2.5 seconds before returning.
        db.test.find_one({"$where": delay(2.5)})
        # {'x': 1, '_id': ObjectId('54f4e12bfba5220aa4d6dee8')}

    Using ``delay`` to provoke a KeyboardInterrupt
    ----------------------------------------------

    .. code-block:: python

        import signal

        # Raise KeyboardInterrupt in 1 second
        def sigalarm(num, frame):
            raise KeyboardInterrupt


        signal.signal(signal.SIGALRM, sigalarm)
        signal.alarm(1)

        raised = False
        try:
            clxn.find_one({"$where": delay(1.5)})
        except KeyboardInterrupt:
            raised = True

        assert raised
    """
    return "function() { sleep(%f * 1000); return true; }" % sec


def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case.
    snake = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake).lower()


def camel_to_upper_camel(camel):
    return camel[0].upper() + camel[1:]


def camel_to_snake_args(arguments):
    for arg_name in list(arguments):
        c2s = camel_to_snake(arg_name)
        arguments[c2s] = arguments.pop(arg_name)
    return arguments


def snake_to_camel(snake):
    # Regex to convert snake_case to lowerCamelCase.
    return re.sub(r"_([a-z])", lambda m: m.group(1).upper(), snake)


def parse_collection_options(opts):
    if "readPreference" in opts:
        opts["read_preference"] = parse_read_preference(opts.pop("readPreference"))

    if "writeConcern" in opts:
        opts["write_concern"] = WriteConcern(**dict(opts.pop("writeConcern")))

    if "readConcern" in opts:
        opts["read_concern"] = ReadConcern(**dict(opts.pop("readConcern")))

    if "timeoutMS" in opts:
        opts["timeout"] = int(opts.pop("timeoutMS")) / 1000.0
    return opts


@contextlib.contextmanager
def _ignore_deprecations():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        yield


def ignore_deprecations(wrapped=None):
    """A context manager or a decorator."""
    if wrapped:
        if iscoroutinefunction(wrapped):

            @functools.wraps(wrapped)
            async def wrapper(*args, **kwargs):
                with _ignore_deprecations():
                    return await wrapped(*args, **kwargs)
        else:

            @functools.wraps(wrapped)
            def wrapper(*args, **kwargs):
                with _ignore_deprecations():
                    return wrapped(*args, **kwargs)

        return wrapper

    else:
        return _ignore_deprecations()


class DeprecationFilter:
    def __init__(self, action="ignore"):
        """Start filtering deprecations."""
        self.warn_context = warnings.catch_warnings()
        self.warn_context.__enter__()
        warnings.simplefilter(action, DeprecationWarning)

    def stop(self):
        """Stop filtering deprecations."""
        self.warn_context.__exit__()  # type: ignore
        self.warn_context = None  # type: ignore


# Constants for run_threads and lazy_client_trial.
NTRIALS = 5
NTHREADS = 10


def run_threads(collection, target):
    """Run a target function in many threads.

    target is a function taking a Collection and an integer.
    """
    threads = []
    for i in range(NTHREADS):
        bound_target = partial(target, collection, i)
        threads.append(threading.Thread(target=bound_target))

    for t in threads:
        t.start()

    for t in threads:
        t.join(60)
        assert not t.is_alive()


@contextlib.contextmanager
def frequent_thread_switches():
    """Make concurrency bugs more likely to manifest."""
    interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)

    try:
        yield
    finally:
        sys.setswitchinterval(interval)


def lazy_client_trial(reset, target, test, get_client):
    """Test concurrent operations on a lazily-connecting client.

    `reset` takes a collection and resets it for the next trial.

    `target` takes a lazily-connecting collection and an index from
    0 to NTHREADS, and performs some operation, e.g. an insert.

    `test` takes the lazily-connecting collection and asserts a
    post-condition to prove `target` succeeded.
    """
    collection = client_context.client.pymongo_test.test

    with frequent_thread_switches():
        for _i in range(NTRIALS):
            reset(collection)
            lazy_client = get_client()
            lazy_collection = lazy_client.pymongo_test.test
            run_threads(lazy_collection, target)
            test(lazy_collection)


def gevent_monkey_patched():
    """Check if gevent's monkey patching is active."""
    try:
        import socket

        import gevent.socket  # type:ignore[import]

        return socket.socket is gevent.socket.socket
    except ImportError:
        return False


def is_greenthread_patched():
    return gevent_monkey_patched()


def parse_read_preference(pref):
    # Make first letter lowercase to match read_pref's modes.
    mode_string = pref.get("mode", "primary")
    mode_string = mode_string[:1].lower() + mode_string[1:]
    mode = read_preferences.read_pref_mode_from_name(mode_string)
    max_staleness = pref.get("maxStalenessSeconds", -1)
    tag_sets = pref.get("tagSets") or pref.get("tag_sets")
    return read_preferences.make_read_preference(
        mode, tag_sets=tag_sets, max_staleness=max_staleness
    )


def server_name_to_type(name):
    """Convert a ServerType name to the corresponding value. For SDAM tests."""
    # Special case, some tests in the spec include the PossiblePrimary
    # type, but only single-threaded drivers need that type. We call
    # possible primaries Unknown.
    if name == "PossiblePrimary":
        return SERVER_TYPE.Unknown
    return getattr(SERVER_TYPE, name)


def cat_files(dest, *sources):
    """Cat multiple files into dest."""
    with open(dest, "wb") as fdst:
        for src in sources:
            with open(src, "rb") as fsrc:
                shutil.copyfileobj(fsrc, fdst)


@contextlib.contextmanager
def assertion_context(msg):
    """A context manager that adds info to an assertion failure."""
    try:
        yield
    except AssertionError as exc:
        raise AssertionError(f"{msg}: {exc}")


def parse_spec_options(opts):
    if "readPreference" in opts:
        opts["read_preference"] = parse_read_preference(opts.pop("readPreference"))

    if "writeConcern" in opts:
        w_opts = opts.pop("writeConcern")
        if "journal" in w_opts:
            w_opts["j"] = w_opts.pop("journal")
        if "wtimeoutMS" in w_opts:
            w_opts["wtimeout"] = w_opts.pop("wtimeoutMS")
        opts["write_concern"] = WriteConcern(**dict(w_opts))

    if "readConcern" in opts:
        opts["read_concern"] = ReadConcern(**dict(opts.pop("readConcern")))

    if "timeoutMS" in opts:
        assert isinstance(opts["timeoutMS"], int)
        opts["timeout"] = int(opts.pop("timeoutMS")) / 1000.0

    if "maxTimeMS" in opts:
        opts["max_time_ms"] = opts.pop("maxTimeMS")

    if "maxCommitTimeMS" in opts:
        opts["max_commit_time_ms"] = opts.pop("maxCommitTimeMS")

    return dict(opts)


def prepare_spec_arguments(spec, arguments, opname, entity_map, with_txn_callback):
    for arg_name in list(arguments):
        c2s = camel_to_snake(arg_name)
        # Named "key" instead not fieldName.
        if arg_name == "fieldName":
            arguments["key"] = arguments.pop(arg_name)
        # Aggregate uses "batchSize", while find uses batch_size.
        elif (arg_name == "batchSize" or arg_name == "allowDiskUse") and opname == "aggregate":
            continue
        elif arg_name == "bypassDocumentValidation" and (
            opname == "aggregate" or "find_one_and" in opname
        ):
            continue
        elif arg_name == "timeoutMode":
            raise unittest.SkipTest("PyMongo does not support timeoutMode")
        # Requires boolean returnDocument.
        elif arg_name == "returnDocument":
            arguments[c2s] = getattr(ReturnDocument, arguments.pop(arg_name).upper())
        elif "bulk_write" in opname and (c2s == "requests" or c2s == "models"):
            # Parse each request into a bulk write model.
            requests = []
            for request in arguments[c2s]:
                if "name" in request:
                    # CRUD v2 format
                    bulk_model = camel_to_upper_camel(request["name"])
                    bulk_class = getattr(operations, bulk_model)
                    bulk_arguments = camel_to_snake_args(request["arguments"])
                else:
                    # Unified test format
                    bulk_model, spec = next(iter(request.items()))
                    bulk_class = getattr(operations, camel_to_upper_camel(bulk_model))
                    bulk_arguments = camel_to_snake_args(spec)
                requests.append(bulk_class(**dict(bulk_arguments)))
            arguments[c2s] = requests
        elif arg_name == "session":
            arguments["session"] = entity_map[arguments["session"]]
        elif opname == "open_download_stream" and arg_name == "id":
            arguments["file_id"] = arguments.pop(arg_name)
        elif opname not in ("find", "find_one") and c2s == "max_time_ms":
            # find is the only method that accepts snake_case max_time_ms.
            # All other methods take kwargs which must use the server's
            # camelCase maxTimeMS. See PYTHON-1855.
            arguments["maxTimeMS"] = arguments.pop("max_time_ms")
        elif opname == "with_transaction" and arg_name == "callback":
            if "operations" in arguments[arg_name]:
                # CRUD v2 format
                callback_ops = arguments[arg_name]["operations"]
            else:
                # Unified test format
                callback_ops = arguments[arg_name]
            arguments["callback"] = lambda _: with_txn_callback(copy.deepcopy(callback_ops))
        elif opname == "drop_collection" and arg_name == "collection":
            arguments["name_or_collection"] = arguments.pop(arg_name)
        elif opname == "create_collection":
            if arg_name == "collection":
                arguments["name"] = arguments.pop(arg_name)
            arguments["check_exists"] = False
            # Any other arguments to create_collection are passed through
            # **kwargs.
        elif opname == "create_index" and arg_name == "keys":
            arguments["keys"] = list(arguments.pop(arg_name).items())
        elif opname == "drop_index" and arg_name == "name":
            arguments["index_or_name"] = arguments.pop(arg_name)
        elif opname == "rename" and arg_name == "to":
            arguments["new_name"] = arguments.pop(arg_name)
        elif opname == "rename" and arg_name == "dropTarget":
            arguments["dropTarget"] = arguments.pop(arg_name)
        elif arg_name == "cursorType":
            cursor_type = arguments.pop(arg_name)
            if cursor_type == "tailable":
                arguments["cursor_type"] = CursorType.TAILABLE
            elif cursor_type == "tailableAwait":
                arguments["cursor_type"] = CursorType.TAILABLE
            else:
                raise AssertionError(f"Unsupported cursorType: {cursor_type}")
        else:
            arguments[c2s] = arguments.pop(arg_name)


def create_async_event():
    return asyncio.Event()


def create_event():
    return threading.Event()


def async_create_barrier(n_tasks: int):
    return asyncio.Barrier(n_tasks)


def create_barrier(n_tasks: int, timeout: float | None = None):
    return threading.Barrier(n_tasks, timeout=timeout)


async def async_barrier_wait(barrier, timeout: float | None = None):
    await asyncio.wait_for(barrier.wait(), timeout=timeout)


def barrier_wait(barrier, timeout: float | None = None):
    barrier.wait(timeout=timeout)
