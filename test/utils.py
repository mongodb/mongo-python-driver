# Copyright 2012-2015 MongoDB, Inc.
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
import os
import struct
import sys
import threading
import time
import warnings
from functools import partial

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, OperationFailure
from pymongo.server_selectors import (any_server_selector,
                                      writable_server_selector)
from pymongo.write_concern import WriteConcern
from test import (client_context,
                  db_user,
                  db_pwd,
                  host,
                  port)
from test.version import Version


def _connection_string_noauth(h, p):
    if h.startswith("mongodb://"):
        return h
    return "mongodb://%s:%d" % (h, p)


def _connection_string(h, p):
    if h.startswith("mongodb://"):
        return h
    elif client_context.auth_enabled:
        return "mongodb://%s:%s@%s:%d" % (db_user, db_pwd, h, p)
    else:
        return _connection_string_noauth(h, p)


def single_client_noauth(h=host, p=port, **kwargs):
    """Make a direct connection. Don't authenticate."""
    return MongoClient(_connection_string_noauth(h, p), **kwargs)


def single_client(h=host, p=port, **kwargs):
    """Make a direct connection, and authenticate if necessary."""
    return MongoClient(_connection_string(h, p), **kwargs)


def rs_client_noauth(h=host, p=port, **kwargs):
    """Connect to the replica set. Don't authenticate."""
    return MongoClient(_connection_string_noauth(h, p),
                       replicaSet=client_context.replica_set_name, **kwargs)


def rs_client(h=host, p=port, **kwargs):
    """Connect to the replica set and authenticate if necessary."""
    return MongoClient(_connection_string(h, p),
                       replicaSet=client_context.replica_set_name, **kwargs)


def rs_or_single_client_noauth(h=host, p=port, **kwargs):
    """Connect to the replica set if there is one, otherwise the standalone.

    Like rs_or_single_client, but does not authenticate.
    """
    if client_context.replica_set_name:
        return rs_client_noauth(h, p, **kwargs)
    else:
        return single_client_noauth(h, p, **kwargs)


def rs_or_single_client(h=host, p=port, **kwargs):
    """Connect to the replica set if there is one, otherwise the standalone.

    Authenticates if necessary.
    """
    if client_context.replica_set_name:
        return rs_client(h, p, **kwargs)
    else:
        return single_client(h, p, **kwargs)


def one(s):
    """Get one element of a set"""
    return next(iter(s))


def oid_generated_on_client(oid):
    """Is this process's PID in this ObjectId?"""
    pid_from_doc = struct.unpack(">H", oid.binary[7:9])[0]
    return (os.getpid() % 0xFFFF) == pid_from_doc


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
    for coll in db.collection_names():
        if not coll.startswith('system'):
            db.drop_collection(coll)


def remove_all_users(db):
    if Version.from_client(db.client).at_least(2, 5, 3, -1):
        db.command("dropAllUsersFromDatabase", 1,
                   writeConcern={"w": client_context.w})
    else:
        db = db.client.get_database(
            db.name, write_concern=WriteConcern(w=client_context.w))
        db.system.users.delete_many({})


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


def enable_text_search(client):
    client.admin.command(
        'setParameter', textSearchEnabled=True)

    for host, port in client.secondaries:
        client = MongoClient(host, port)
        if client_context.auth_enabled:
            client.admin.authenticate(db_user, db_pwd)
        client.admin.command('setParameter', textSearchEnabled=True)


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
def ignore_deprecations():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        yield


def read_from_which_host(
        client,
        pref,
        tag_sets=None,
):
    """Read from a client with the given Read Preference.

    Return the 'host:port' which was read from.

    :Parameters:
      - `client`: A MongoClient
      - `mode`: A ReadPreference
      - `tag_sets`: List of dicts of tags for data-center-aware reads
    """
    db = client.pymongo_test

    if isinstance(tag_sets, dict):
        tag_sets = [tag_sets]
    if tag_sets:
        tags = tag_sets or pref.tag_sets
        pref = pref.__class__(tags)

    db.read_preference = pref

    cursor = db.test.find()
    try:
        try:
            next(cursor)
        except StopIteration:
            # No documents in collection, that's fine
            pass

        return cursor.address
    except AutoReconnect:
        return None


def assertReadFrom(testcase, client, member, *args, **kwargs):
    """Check that a query with the given mode and tag_sets reads from
    the expected replica-set member.

    :Parameters:
      - `testcase`: A unittest.TestCase
      - `client`: A MongoClient
      - `member`: A host:port expected to be used
      - `mode`: A ReadPreference
      - `tag_sets` (optional): List of dicts of tags for data-center-aware reads
    """
    for _ in range(10):
        testcase.assertEqual(member,
                             read_from_which_host(client, *args, **kwargs))


def assertReadFromAll(testcase, client, members, *args, **kwargs):
    """Check that a query with the given mode and tag_sets reads from all
    members in a set, and only members in that set.

    :Parameters:
      - `testcase`: A unittest.TestCase
      - `client`: A MongoClient
      - `members`: Sequence of host:port expected to be used
      - `mode`: A ReadPreference
      - `tag_sets` (optional): List of dicts of tags for data-center-aware reads
    """
    members = set(members)
    used = set()
    for _ in range(100):
        used.add(read_from_which_host(client, *args, **kwargs))

    testcase.assertEqual(members, used)


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
        t.join(30)
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
