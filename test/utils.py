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
import functools
import sys
import threading
import time
import warnings

from collections import defaultdict
from functools import partial

from bson.objectid import ObjectId
from pymongo import MongoClient, monitoring
from pymongo.errors import OperationFailure
from pymongo.monitoring import _SENSITIVE_COMMANDS
from pymongo.server_selectors import (any_server_selector,
                                      writable_server_selector)
from pymongo.write_concern import WriteConcern
from test import (client_context, db_user, db_pwd)


IMPOSSIBLE_WRITE_CONCERN = WriteConcern(w=1000)


class WhiteListEventListener(monitoring.CommandListener):

    def __init__(self, *commands):
        self.commands = set(commands)
        self.results = defaultdict(list)

    def started(self, event):
        if event.command_name in self.commands:
            self.results['started'].append(event)

    def succeeded(self, event):
        if event.command_name in self.commands:
            self.results['succeeded'].append(event)

    def failed(self, event):
        if event.command_name in self.commands:
            self.results['failed'].append(event)


class EventListener(monitoring.CommandListener):

    def __init__(self):
        self.results = defaultdict(list)

    def started(self, event):
        self.results['started'].append(event)

    def succeeded(self, event):
        self.results['succeeded'].append(event)

    def failed(self, event):
        self.results['failed'].append(event)


class OvertCommandListener(EventListener):
    """A CommandListener that ignores sensitive commands."""
    def started(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super(OvertCommandListener, self).started(event)

    def succeeded(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super(OvertCommandListener, self).succeeded(event)

    def failed(self, event):
        if event.command_name.lower() not in _SENSITIVE_COMMANDS:
            super(OvertCommandListener, self).failed(event)


class ServerAndTopologyEventListener(monitoring.ServerListener,
                                     monitoring.TopologyListener):
    """Listens to all events."""

    def __init__(self):
        self.results = []

    def opened(self, event):
        self.results.append(event)

    def description_changed(self, event):
        self.results.append(event)

    def closed(self, event):
        self.results.append(event)


class HeartbeatEventListener(monitoring.ServerHeartbeatListener):
    """Listens to only server heartbeat events."""

    def __init__(self):
        self.results = []

    def started(self, event):
        self.results.append(event)

    def succeeded(self, event):
        self.results.append(event)

    def failed(self, event):
        self.results.append(event)


def _connection_string(h, authenticate):
    if h.startswith("mongodb://"):
        return h
    elif client_context.auth_enabled and authenticate:
        return "mongodb://%s:%s@%s" % (db_user, db_pwd, str(h))
    else:
        return "mongodb://%s" % (str(h),)


def _mongo_client(host, port, authenticate=True, direct=False, **kwargs):
    """Create a new client over SSL/TLS if necessary."""
    host = host or client_context.host
    port = port or client_context.port
    client_options = client_context.default_client_options.copy()
    if client_context.replica_set_name and not direct:
        client_options['replicaSet'] = client_context.replica_set_name
    client_options.update(kwargs)

    client = MongoClient(_connection_string(host, authenticate), port,
                         **client_options)

    return client


def single_client_noauth(h=None, p=None, **kwargs):
    """Make a direct connection. Don't authenticate."""
    return _mongo_client(h, p, authenticate=False, direct=True, **kwargs)


def single_client(h=None, p=None, **kwargs):
    """Make a direct connection, and authenticate if necessary."""
    return _mongo_client(h, p, direct=True, **kwargs)


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


def one(s):
    """Get one element of a set"""
    return next(iter(s))


def oid_generated_on_process(oid):
    """Makes a determination as to whether the given ObjectId was generated
    by the current process, based on the 5-byte random number in the ObjectId.
    """
    return ObjectId._random() == oid.binary[4:9]


def delay(sec):
    return '''function() { sleep(%f * 1000); return true; }''' % sec


def get_command_line(client):
    command_line = client.admin.command('getCmdLineOpts')
    assert command_line['ok'] == 1, "getCmdLineOpts() failed"
    return command_line


def server_started_with_option(client, cmdline_opt, config_opt):
    """Check if the server was started with a particular option.

    :Parameters:
      - `cmdline_opt`: The command line option (i.e. --nojournal)
      - `config_opt`: The config file option (i.e. nojournal)
    """
    command_line = get_command_line(client)
    if 'parsed' in command_line:
        parsed = command_line['parsed']
        if config_opt in parsed:
            return parsed[config_opt]
    argv = command_line['argv']
    return cmdline_opt in argv


def server_started_with_auth(client):
    try:
        command_line = get_command_line(client)
    except OperationFailure as e:
        msg = e.details.get('errmsg', '')
        if e.code == 13 or 'unauthorized' in msg or 'login' in msg:
            # Unauthorized.
            return True
        raise

    # MongoDB >= 2.0
    if 'parsed' in command_line:
        parsed = command_line['parsed']
        # MongoDB >= 2.6
        if 'security' in parsed:
            security = parsed['security']
            # >= rc3
            if 'authorization' in security:
                return security['authorization'] == 'enabled'
            # < rc3
            return security.get('auth', False) or bool(security.get('keyFile'))
        return parsed.get('auth', False) or bool(parsed.get('keyFile'))
    # Legacy
    argv = command_line['argv']
    return '--auth' in argv or '--keyFile' in argv


def server_started_with_nojournal(client):
    command_line = get_command_line(client)

    # MongoDB 2.6.
    if 'parsed' in command_line:
        parsed = command_line['parsed']
        if 'storage' in parsed:
            storage = parsed['storage']
            if 'journal' in storage:
                return not storage['journal']['enabled']

    return server_started_with_option(client, '--nojournal', 'nojournal')


def server_is_master_with_slave(client):
    command_line = get_command_line(client)
    if 'parsed' in command_line:
        return command_line['parsed'].get('master', False)
    return '--master' in command_line['argv']


def drop_collections(db):
    for coll in db.list_collection_names():
        if not coll.startswith('system'):
            db.drop_collection(coll)


def remove_all_users(db):
    db.command("dropAllUsersFromDatabase", 1,
               writeConcern={"w": client_context.w})


def joinall(threads):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    for t in threads:
        t.join(300)
        assert not t.isAlive(), "Thread %s hung" % t


def connected(client):
    """Convenience to wait for a newly-constructed client to connect."""
    with warnings.catch_warnings():
        # Ignore warning that "ismaster" is always routed to primary even
        # if client's read preference isn't PRIMARY.
        warnings.simplefilter("ignore", UserWarning)
        client.admin.command('ismaster')  # Force connection.

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
    while True:
        retval = predicate()
        if retval:
            return retval

        if time.time() - start > timeout:
            raise AssertionError("Didn't ever %s" % success_description)

        time.sleep(0.1)


def is_mongos(client):
    res = client.admin.command('ismaster')
    return res.get('msg', '') == 'isdbgrid'


def assertRaisesExactly(cls, fn, *args, **kwargs):
    """
    Unlike the standard assertRaises, this checks that a function raises a
    specific class of exception, and not a subclass. E.g., check that
    MongoClient() raises ConnectionFailure but not its subclass, AutoReconnect.
    """
    try:
        fn(*args, **kwargs)
    except Exception as e:
        assert e.__class__ == cls, "got %s, expected %s" % (
            e.__class__.__name__, cls.__name__)
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
        self.warn_context.__exit__()
        self.warn_context = None


def get_pool(client):
    """Get the standalone, primary, or mongos pool."""
    topology = client._get_topology()
    server = topology.select_server(writable_server_selector)
    return server.pool


def get_pools(client):
    """Get all pools."""
    return [
        server.pool for server in
        client._get_topology().select_servers(any_server_selector)]


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
        assert not t.isAlive()


@contextlib.contextmanager
def frequent_thread_switches():
    """Make concurrency bugs more likely to manifest."""
    interval = None
    if not sys.platform.startswith('java'):
        if hasattr(sys, 'getswitchinterval'):
            interval = sys.getswitchinterval()
            sys.setswitchinterval(1e-6)
        else:
            interval = sys.getcheckinterval()
            sys.setcheckinterval(1)

    try:
        yield
    finally:
        if not sys.platform.startswith('java'):
            if hasattr(sys, 'setswitchinterval'):
                sys.setswitchinterval(interval)
            else:
                sys.setcheckinterval(interval)


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
    # In Python 3.6 importing gevent.socket raises an ImportWarning.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ImportWarning)
        try:
            import socket
            import gevent.socket
            return socket.socket is gevent.socket.socket
        except ImportError:
            return False


def eventlet_monkey_patched():
    """Check if eventlet's monkey patching is active."""
    try:
        import threading
        import eventlet
        return (threading.current_thread.__module__ ==
                'eventlet.green.threading')
    except ImportError:
        return False


def is_greenthread_patched():
    return gevent_monkey_patched() or eventlet_monkey_patched()


def disable_replication(client):
    """Disable replication on all secondaries, requires MongoDB 3.2."""
    for host, port in client.secondaries:
        secondary = single_client(host, port)
        secondary.admin.command('configureFailPoint', 'stopReplProducer',
                                mode='alwaysOn')


def enable_replication(client):
    """Enable replication on all secondaries, requires MongoDB 3.2."""
    for host, port in client.secondaries:
        secondary = single_client(host, port)
        secondary.admin.command('configureFailPoint', 'stopReplProducer',
                                mode='off')
