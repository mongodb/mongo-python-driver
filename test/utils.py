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

"""Utilities for testing pymongo
"""

import contextlib
import copy
import functools
import os
import re
import shutil
import sys
import threading
import time
import unittest
import warnings
from collections import abc, defaultdict
from functools import partial
from test import client_context, db_pwd, db_user

from bson import json_util
from bson.objectid import ObjectId
from bson.son import SON
from pymongo import MongoClient, monitoring, operations, read_preferences
from pymongo.collection import ReturnDocument
from pymongo.cursor import CursorType
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.hello import HelloCompat
from pymongo.monitoring import (
    _SENSITIVE_COMMANDS,
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
from pymongo.pool import _CancellationContext, _PoolGeneration
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import any_server_selector, writable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.uri_parser import parse_uri
from pymongo.write_concern import WriteConcern

IMPOSSIBLE_WRITE_CONCERN = WriteConcern(w=50)


class BaseListener(object):
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
        wait_until(lambda: self.event_count(event) >= count, "find %s %s event(s)" % (count, event))


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


class EventListener(monitoring.CommandListener):
    def __init__(self):
        self.results = defaultdict(list)

    def started(self, event):
        self.results["started"].append(event)

    def succeeded(self, event):
        self.results["succeeded"].append(event)

    def failed(self, event):
        self.results["failed"].append(event)

    def started_command_names(self):
        """Return list of command names started."""
        return [event.command_name for event in self.results["started"]]

    def reset(self):
        """Reset the state of this listener."""
        self.results.clear()


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
        super(AllowListEventListener, self).__init__()

    def started(self, event):
        if event.command_name in self.commands:
            super(AllowListEventListener, self).started(event)

    def succeeded(self, event):
        if event.command_name in self.commands:
            super(AllowListEventListener, self).succeeded(event)

    def failed(self, event):
        if event.command_name in self.commands:
            super(AllowListEventListener, self).failed(event)


class OvertCommandListener(EventListener):
    """A CommandListener that ignores sensitive commands."""

    ignore_list_collections = False

    def started(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super(OvertCommandListener, self).started(event)

    def succeeded(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super(OvertCommandListener, self).succeeded(event)

    def failed(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super(OvertCommandListener, self).failed(event)


class _ServerEventListener(object):
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


class MockSocketInfo(object):
    def __init__(self):
        self.cancel_context = _CancellationContext()
        self.more_to_come = False

    def close_socket(self, reason):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockPool(object):
    def __init__(self, address, options, handshake=True):
        self.gen = _PoolGeneration()
        self._lock = threading.Lock()
        self.opts = options
        self.operation_count = 0

    def stale_generation(self, gen, service_id):
        return self.gen.stale(gen, service_id)

    def get_socket(self, handler=None):
        return MockSocketInfo()

    def return_socket(self, *args, **kwargs):
        pass

    def _reset(self, service_id=None):
        with self._lock:
            self.gen.inc(service_id)

    def ready(self):
        pass

    def reset(self, service_id=None):
        self._reset()

    def reset_without_pause(self):
        self._reset()

    def close(self):
        self._reset()

    def update_is_writable(self, is_writable):
        pass

    def remove_stale_sockets(self, *args, **kwargs):
        pass


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


class CompareType(object):
    """Class that compares equal to any object of the given type."""

    def __init__(self, type):
        self.type = type

    def __eq__(self, other):
        return isinstance(other, self.type)

    def __ne__(self, other):
        """Needed for Python 2."""
        return not self.__eq__(other)


class FunctionCallRecorder(object):
    """Utility class to wrap a callable and record its invocations."""

    def __init__(self, function):
        self._function = function
        self._call_list = []

    def __call__(self, *args, **kwargs):
        self._call_list.append((args, kwargs))
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


class TestCreator(object):
    """Class to create test cases from specifications."""

    def __init__(self, create_test, test_class, test_path):
        """Create a TestCreator object.

        :Parameters:
          - `create_test`: callback that returns a test case. The callback
            must accept the following arguments - a dictionary containing the
            entire test specification (the `scenario_def`), a dictionary
            containing the specification for which the test case will be
            generated (the `test_def`).
          - `test_class`: the unittest.TestCase class in which to create the
            test case.
          - `test_path`: path to the directory containing the JSON files with
            the test specifications.
        """
        self._create_test = create_test
        self._test_class = test_class
        self.test_path = test_path

    def _ensure_min_max_server_version(self, scenario_def, method):
        """Test modifier that enforces a version range for the server on a
        test case."""
        if "minServerVersion" in scenario_def:
            min_ver = tuple(int(elt) for elt in scenario_def["minServerVersion"].split("."))
            if min_ver is not None:
                method = client_context.require_version_min(*min_ver)(method)

        if "maxServerVersion" in scenario_def:
            max_ver = tuple(int(elt) for elt in scenario_def["maxServerVersion"].split("."))
            if max_ver is not None:
                method = client_context.require_version_max(*max_ver)(method)

        if "serverless" in scenario_def:
            serverless = scenario_def["serverless"]
            if serverless == "require":
                serverless_satisfied = client_context.serverless
            elif serverless == "forbid":
                serverless_satisfied = not client_context.serverless
            else:  # unset or "allow"
                serverless_satisfied = True
            method = unittest.skipUnless(
                serverless_satisfied, "Serverless requirement not satisfied"
            )(method)

        return method

    @staticmethod
    def valid_topology(run_on_req):
        return client_context.is_topology_type(
            run_on_req.get("topology", ["single", "replicaset", "sharded", "load-balanced"])
        )

    @staticmethod
    def min_server_version(run_on_req):
        version = run_on_req.get("minServerVersion")
        if version:
            min_ver = tuple(int(elt) for elt in version.split("."))
            return client_context.version >= min_ver
        return True

    @staticmethod
    def max_server_version(run_on_req):
        version = run_on_req.get("maxServerVersion")
        if version:
            max_ver = tuple(int(elt) for elt in version.split("."))
            return client_context.version <= max_ver
        return True

    @staticmethod
    def valid_auth_enabled(run_on_req):
        if "authEnabled" in run_on_req:
            if run_on_req["authEnabled"]:
                return client_context.auth_enabled
            return not client_context.auth_enabled
        return True

    @staticmethod
    def serverless_ok(run_on_req):
        serverless = run_on_req["serverless"]
        if serverless == "require":
            return client_context.serverless
        elif serverless == "forbid":
            return not client_context.serverless
        else:  # unset or "allow"
            return True

    def should_run_on(self, scenario_def):
        run_on = scenario_def.get("runOn", [])
        if not run_on:
            # Always run these tests.
            return True

        for req in run_on:
            if (
                self.valid_topology(req)
                and self.min_server_version(req)
                and self.max_server_version(req)
                and self.valid_auth_enabled(req)
                and self.serverless_ok(req)
            ):
                return True
        return False

    def ensure_run_on(self, scenario_def, method):
        """Test modifier that enforces a 'runOn' on a test case."""
        return client_context._require(
            lambda: self.should_run_on(scenario_def), "runOn not satisfied", method
        )

    def tests(self, scenario_def):
        """Allow CMAP spec test to override the location of test."""
        return scenario_def["tests"]

    def create_tests(self):
        for dirpath, _, filenames in os.walk(self.test_path):
            dirname = os.path.split(dirpath)[-1]

            for filename in filenames:
                with open(os.path.join(dirpath, filename)) as scenario_stream:
                    # Use tz_aware=False to match how CodecOptions decodes
                    # dates.
                    opts = json_util.JSONOptions(tz_aware=False)
                    scenario_def = ScenarioDict(
                        json_util.loads(scenario_stream.read(), json_options=opts)
                    )

                test_type = os.path.splitext(filename)[0]

                # Construct test from scenario.
                for test_def in self.tests(scenario_def):
                    test_name = "test_%s_%s_%s" % (
                        dirname,
                        test_type.replace("-", "_").replace(".", "_"),
                        str(test_def["description"].replace(" ", "_").replace(".", "_")),
                    )

                    new_test = self._create_test(scenario_def, test_def, test_name)
                    new_test = self._ensure_min_max_server_version(scenario_def, new_test)
                    new_test = self.ensure_run_on(scenario_def, new_test)

                    new_test.__name__ = test_name
                    setattr(self._test_class, new_test.__name__, new_test)


def _connection_string(h):
    if h.startswith("mongodb://") or h.startswith("mongodb+srv://"):
        return h
    return "mongodb://%s" % (str(h),)


def _mongo_client(host, port, authenticate=True, directConnection=None, **kwargs):
    """Create a new client over SSL/TLS if necessary."""
    host = host or client_context.host
    port = port or client_context.port
    client_options: dict = client_context.default_client_options.copy()
    if client_context.replica_set_name and not directConnection:
        client_options["replicaSet"] = client_context.replica_set_name
    if directConnection is not None:
        client_options["directConnection"] = directConnection
    client_options.update(kwargs)

    uri = _connection_string(host)
    if client_context.auth_enabled and authenticate:
        # Only add the default username or password if one is not provided.
        res = parse_uri(uri)
        if (
            not res["username"]
            and not res["password"]
            and "username" not in client_options
            and "password" not in client_options
        ):
            client_options["username"] = db_user
            client_options["password"] = db_pwd

    return MongoClient(uri, port, **client_options)


def single_client_noauth(h=None, p=None, **kwargs):
    """Make a direct connection. Don't authenticate."""
    return _mongo_client(h, p, authenticate=False, directConnection=True, **kwargs)


def single_client(h=None, p=None, **kwargs):
    """Make a direct connection, and authenticate if necessary."""
    return _mongo_client(h, p, directConnection=True, **kwargs)


def rs_client_noauth(h=None, p=None, **kwargs):
    """Connect to the replica set. Don't authenticate."""
    return _mongo_client(h, p, authenticate=False, **kwargs)


def rs_client(h=None, p=None, **kwargs):
    """Connect to the replica set and authenticate if necessary."""
    return _mongo_client(h, p, **kwargs)


def rs_or_single_client_noauth(h=None, p=None, **kwargs):
    """Connect to the replica set if there is one, otherwise the standalone.

    Like rs_or_single_client, but does not authenticate.
    """
    return _mongo_client(h, p, authenticate=False, **kwargs)


def rs_or_single_client(h=None, p=None, **kwargs):
    """Connect to the replica set if there is one, otherwise the standalone.

    Authenticates if necessary.
    """
    return _mongo_client(h, p, **kwargs)


def ensure_all_connected(client):
    """Ensure that the client's connection pool has socket connections to all
    members of a replica set. Raises ConfigurationError when called with a
    non-replica set client.

    Depending on the use-case, the caller may need to clear any event listeners
    that are configured on the client.
    """
    hello = client.admin.command(HelloCompat.LEGACY_CMD)
    if "setName" not in hello:
        raise ConfigurationError("cluster is not a replica set")

    target_host_list = set(hello["hosts"])
    connected_host_list = set([hello["me"]])
    admindb = client.get_database("admin")

    # Run hello until we have connected to each host at least once.
    while connected_host_list != target_host_list:
        hello = admindb.command(HelloCompat.LEGACY_CMD, read_preference=ReadPreference.SECONDARY)
        connected_host_list.update([hello["me"]])


def one(s):
    """Get one element of a set"""
    return next(iter(s))


def oid_generated_on_process(oid):
    """Makes a determination as to whether the given ObjectId was generated
    by the current process, based on the 5-byte random number in the ObjectId.
    """
    return ObjectId._random() == oid.binary[4:9]


def delay(sec):
    return """function() { sleep(%f * 1000); return true; }""" % sec


def get_command_line(client):
    command_line = client.admin.command("getCmdLineOpts")
    assert command_line["ok"] == 1, "getCmdLineOpts() failed"
    return command_line


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


def server_started_with_option(client, cmdline_opt, config_opt):
    """Check if the server was started with a particular option.

    :Parameters:
      - `cmdline_opt`: The command line option (i.e. --nojournal)
      - `config_opt`: The config file option (i.e. nojournal)
    """
    command_line = get_command_line(client)
    if "parsed" in command_line:
        parsed = command_line["parsed"]
        if config_opt in parsed:
            return parsed[config_opt]
    argv = command_line["argv"]
    return cmdline_opt in argv


def server_started_with_auth(client):
    try:
        command_line = get_command_line(client)
    except OperationFailure as e:
        msg = e.details.get("errmsg", "")  # type: ignore
        if e.code == 13 or "unauthorized" in msg or "login" in msg:
            # Unauthorized.
            return True
        raise

    # MongoDB >= 2.0
    if "parsed" in command_line:
        parsed = command_line["parsed"]
        # MongoDB >= 2.6
        if "security" in parsed:
            security = parsed["security"]
            # >= rc3
            if "authorization" in security:
                return security["authorization"] == "enabled"
            # < rc3
            return security.get("auth", False) or bool(security.get("keyFile"))
        return parsed.get("auth", False) or bool(parsed.get("keyFile"))
    # Legacy
    argv = command_line["argv"]
    return "--auth" in argv or "--keyFile" in argv


def drop_collections(db):
    # Drop all non-system collections in this database.
    for coll in db.list_collection_names(filter={"name": {"$regex": r"^(?!system\.)"}}):
        db.drop_collection(coll)


def remove_all_users(db):
    db.command("dropAllUsersFromDatabase", 1, writeConcern={"w": client_context.w})


def joinall(threads):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    for t in threads:
        t.join(300)
        assert not t.is_alive(), "Thread %s hung" % t


def connected(client):
    """Convenience to wait for a newly-constructed client to connect."""
    with warnings.catch_warnings():
        # Ignore warning that ping is always routed to primary even
        # if client's read preference isn't PRIMARY.
        warnings.simplefilter("ignore", UserWarning)
        client.admin.command("ping")  # Force connection.

    return client


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
        retval = predicate()
        if retval:
            return retval

        if time.time() - start > timeout:
            raise AssertionError("Didn't ever %s" % success_description)

        time.sleep(interval)


def repl_set_step_down(client, **kwargs):
    """Run replSetStepDown, first unfreezing a secondary with replSetFreeze."""
    cmd = SON([("replSetStepDown", 1)])
    cmd.update(kwargs)

    # Unfreeze a secondary to ensure a speedy election.
    client.admin.command("replSetFreeze", 0, read_preference=ReadPreference.SECONDARY)
    client.admin.command(cmd)


def is_mongos(client):
    res = client.admin.command(HelloCompat.LEGACY_CMD)
    return res.get("msg", "") == "isdbgrid"


def assertRaisesExactly(cls, fn, *args, **kwargs):
    """
    Unlike the standard assertRaises, this checks that a function raises a
    specific class of exception, and not a subclass. E.g., check that
    MongoClient() raises ConnectionFailure but not its subclass, AutoReconnect.
    """
    try:
        fn(*args, **kwargs)
    except Exception as e:
        assert e.__class__ == cls, "got %s, expected %s" % (e.__class__.__name__, cls.__name__)
    else:
        raise AssertionError("%s not raised" % cls)


@contextlib.contextmanager
def _ignore_deprecations():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        yield


def ignore_deprecations(wrapped=None):
    """A context manager or a decorator."""
    if wrapped:

        @functools.wraps(wrapped)
        def wrapper(*args, **kwargs):
            with _ignore_deprecations():
                return wrapped(*args, **kwargs)

        return wrapper

    else:
        return _ignore_deprecations()


class DeprecationFilter(object):
    def __init__(self, action="ignore"):
        """Start filtering deprecations."""
        self.warn_context = warnings.catch_warnings()
        self.warn_context.__enter__()
        warnings.simplefilter(action, DeprecationWarning)

    def stop(self):
        """Stop filtering deprecations."""
        self.warn_context.__exit__()  # type: ignore
        self.warn_context = None  # type: ignore


def get_pool(client):
    """Get the standalone, primary, or mongos pool."""
    topology = client._get_topology()
    server = topology.select_server(writable_server_selector)
    return server.pool


def get_pools(client):
    """Get all pools."""
    return [server.pool for server in client._get_topology().select_servers(any_server_selector)]


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
        for i in range(NTRIALS):
            reset(collection)
            lazy_client = get_client()
            lazy_collection = lazy_client.pymongo_test.test
            run_threads(lazy_collection, target)
            test(lazy_collection)


def gevent_monkey_patched():
    """Check if gevent's monkey patching is active."""
    try:
        import socket

        import gevent.socket

        return socket.socket is gevent.socket.socket
    except ImportError:
        return False


def eventlet_monkey_patched():
    """Check if eventlet's monkey patching is active."""
    import threading

    return threading.current_thread.__module__ == "eventlet.green.threading"


def is_greenthread_patched():
    return gevent_monkey_patched() or eventlet_monkey_patched()


def disable_replication(client):
    """Disable replication on all secondaries."""
    for host, port in client.secondaries:
        secondary = single_client(host, port)
        secondary.admin.command("configureFailPoint", "stopReplProducer", mode="alwaysOn")


def enable_replication(client):
    """Enable replication on all secondaries."""
    for host, port in client.secondaries:
        secondary = single_client(host, port)
        secondary.admin.command("configureFailPoint", "stopReplProducer", mode="off")


class ExceptionCatchingThread(threading.Thread):
    """A thread that stores any exception encountered from run()."""

    def __init__(self, *args, **kwargs):
        self.exc = None
        super(ExceptionCatchingThread, self).__init__(*args, **kwargs)

    def run(self):
        try:
            super(ExceptionCatchingThread, self).run()
        except BaseException as exc:
            self.exc = exc
            raise


def parse_read_preference(pref):
    # Make first letter lowercase to match read_pref's modes.
    mode_string = pref.get("mode", "primary")
    mode_string = mode_string[:1].lower() + mode_string[1:]
    mode = read_preferences.read_pref_mode_from_name(mode_string)
    max_staleness = pref.get("maxStalenessSeconds", -1)
    tag_sets = pref.get("tag_sets")
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
        msg = "%s (%s)" % (exc, msg)
        exc_type, exc_val, exc_tb = sys.exc_info()
        assert exc_type is not None
        raise exc_type(exc_val).with_traceback(exc_tb)


def parse_spec_options(opts):
    if "readPreference" in opts:
        opts["read_preference"] = parse_read_preference(opts.pop("readPreference"))

    if "writeConcern" in opts:
        opts["write_concern"] = WriteConcern(**dict(opts.pop("writeConcern")))

    if "readConcern" in opts:
        opts["read_concern"] = ReadConcern(**dict(opts.pop("readConcern")))

    if "timeoutMS" in opts:
        assert isinstance(opts["timeoutMS"], int)
        opts["timeout"] = int(opts.pop("timeoutMS")) / 1000.0

    if "maxTimeMS" in opts:
        opts["max_time_ms"] = opts.pop("maxTimeMS")

    if "maxCommitTimeMS" in opts:
        opts["max_commit_time_ms"] = opts.pop("maxCommitTimeMS")

    if "hint" in opts:
        hint = opts.pop("hint")
        if not isinstance(hint, str):
            hint = list(hint.items())
        opts["hint"] = hint

    # Properly format 'hint' arguments for the Bulk API tests.
    if "requests" in opts:
        reqs = opts.pop("requests")
        for req in reqs:
            if "name" in req:
                # CRUD v2 format
                args = req.pop("arguments", {})
                if "hint" in args:
                    hint = args.pop("hint")
                    if not isinstance(hint, str):
                        hint = list(hint.items())
                    args["hint"] = hint
                req["arguments"] = args
            else:
                # Unified test format
                bulk_model, spec = next(iter(req.items()))
                if "hint" in spec:
                    hint = spec.pop("hint")
                    if not isinstance(hint, str):
                        hint = list(hint.items())
                    spec["hint"] = hint
        opts["requests"] = reqs

    return dict(opts)


def prepare_spec_arguments(spec, arguments, opname, entity_map, with_txn_callback):
    for arg_name in list(arguments):
        c2s = camel_to_snake(arg_name)
        # PyMongo accepts sort as list of tuples.
        if arg_name == "sort":
            sort_dict = arguments[arg_name]
            arguments[arg_name] = list(sort_dict.items())
        # Named "key" instead not fieldName.
        if arg_name == "fieldName":
            arguments["key"] = arguments.pop(arg_name)
        # Aggregate uses "batchSize", while find uses batch_size.
        elif (arg_name == "batchSize" or arg_name == "allowDiskUse") and opname == "aggregate":
            continue
        elif arg_name == "timeoutMode":
            raise unittest.SkipTest("PyMongo does not support timeoutMode")
        # Requires boolean returnDocument.
        elif arg_name == "returnDocument":
            arguments[c2s] = getattr(ReturnDocument, arguments.pop(arg_name).upper())
        elif c2s == "requests":
            # Parse each request into a bulk write model.
            requests = []
            for request in arguments["requests"]:
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
            arguments["requests"] = requests
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
        elif arg_name == "cursorType":
            cursor_type = arguments.pop(arg_name)
            if cursor_type == "tailable":
                arguments["cursor_type"] = CursorType.TAILABLE
            elif cursor_type == "tailableAwait":
                arguments["cursor_type"] = CursorType.TAILABLE
            else:
                assert False, f"Unsupported cursorType: {cursor_type}"
        else:
            arguments[c2s] = arguments.pop(arg_name)
