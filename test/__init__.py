# Copyright 2010-present MongoDB, Inc.
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

"""Test suite for pymongo, bson, and gridfs.
"""

import os
import socket
import sys
import threading
import time
import warnings

try:
    import ipaddress
    HAVE_IPADDRESS = True
except ImportError:
    HAVE_IPADDRESS = False

if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
    from unittest2 import SkipTest
else:
    import unittest
    from unittest import SkipTest

from functools import wraps

import pymongo
import pymongo.errors

from bson.son import SON
from bson.py3compat import _unicode
from pymongo import common, message
from pymongo.common import partition_node
from pymongo.ssl_support import HAVE_SSL, validate_cert_reqs
from test.version import Version

if HAVE_SSL:
    import ssl

# The host and port of a single mongod or mongos, or the seed host
# for a replica set.
host = os.environ.get("DB_IP", 'localhost')
port = int(os.environ.get("DB_PORT", 27017))

db_user = os.environ.get("DB_USER", "user")
db_pwd = os.environ.get("DB_PASSWORD", "password")

CERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         'certificates')
CLIENT_PEM = os.environ.get('CLIENT_PEM',
                            os.path.join(CERT_PATH, 'client.pem'))
CA_PEM = os.environ.get('CA_PEM', os.path.join(CERT_PATH, 'ca.pem'))
CERT_REQS = validate_cert_reqs('CERT_REQS', os.environ.get('CERT_REQS'))

_SSL_OPTIONS = dict(ssl=True)
if CLIENT_PEM:
    _SSL_OPTIONS['ssl_certfile'] = CLIENT_PEM
if CA_PEM:
    _SSL_OPTIONS['ssl_ca_certs'] = CA_PEM
if CERT_REQS is not None:
    _SSL_OPTIONS['ssl_cert_reqs'] = CERT_REQS

COMPRESSORS = os.environ.get("COMPRESSORS")

def is_server_resolvable():
    """Returns True if 'server' is resolvable."""
    socket_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(1)
    try:
        try:
            socket.gethostbyname('server')
            return True
        except socket.error:
            return False
    finally:
        socket.setdefaulttimeout(socket_timeout)


def _create_user(authdb, user, pwd=None, roles=None, **kwargs):
    cmd = SON([('createUser', user)])
    # X509 doesn't use a password
    if pwd:
        cmd['pwd'] = pwd
    cmd['roles'] = roles or ['root']
    cmd.update(**kwargs)
    return authdb.command(cmd)


class client_knobs(object):
    def __init__(
            self,
            heartbeat_frequency=None,
            min_heartbeat_interval=None,
            kill_cursor_frequency=None,
            events_queue_frequency=None):
        self.heartbeat_frequency = heartbeat_frequency
        self.min_heartbeat_interval = min_heartbeat_interval
        self.kill_cursor_frequency = kill_cursor_frequency
        self.events_queue_frequency = events_queue_frequency

        self.old_heartbeat_frequency = None
        self.old_min_heartbeat_interval = None
        self.old_kill_cursor_frequency = None
        self.old_events_queue_frequency = None

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

    def __enter__(self):
        self.enable()

    def disable(self):
        common.HEARTBEAT_FREQUENCY = self.old_heartbeat_frequency
        common.MIN_HEARTBEAT_INTERVAL = self.old_min_heartbeat_interval
        common.KILL_CURSOR_FREQUENCY = self.old_kill_cursor_frequency
        common.EVENTS_QUEUE_FREQUENCY = self.old_events_queue_frequency

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()


def _all_users(db):
    return set(u['user'] for u in db.command('usersInfo').get('users', []))


class ClientContext(object):

    def __init__(self):
        """Create a client and grab essential information from the server."""
        self.connection_attempts = []
        self.connected = False
        self.ismaster = {}
        self.w = None
        self.nodes = set()
        self.replica_set_name = None
        self.cmd_line = None
        self.server_status = None
        self.version = Version(-1)  # Needs to be comparable with Version
        self.auth_enabled = False
        self.test_commands_enabled = False
        self.is_mongos = False
        self.is_rs = False
        self.has_ipv6 = False
        self.ssl = False
        self.ssl_cert_none = False
        self.ssl_certfile = False
        self.server_is_resolvable = is_server_resolvable()
        self.default_client_options = {}
        self.sessions_enabled = False
        self.client = None
        self.conn_lock = threading.Lock()

        if COMPRESSORS:
            self.default_client_options["compressors"] = COMPRESSORS

    def _connect(self, host, port, **kwargs):
        # Jython takes a long time to connect.
        if sys.platform.startswith('java'):
            timeout_ms = 10000
        else:
            timeout_ms = 5000
        if COMPRESSORS:
            kwargs["compressors"] = COMPRESSORS
        client = pymongo.MongoClient(
            host, port, serverSelectionTimeoutMS=timeout_ms, **kwargs)
        try:
            try:
                client.admin.command('isMaster')  # Can we connect?
            except pymongo.errors.OperationFailure as exc:
                # SERVER-32063
                self.connection_attempts.append(
                    'connected client %r, but isMaster failed: %s' % (
                        client, exc))
            else:
                self.connection_attempts.append(
                    'successfully connected client %r' % (client,))
            # If connected, then return client with default timeout
            return pymongo.MongoClient(host, port, **kwargs)
        except pymongo.errors.ConnectionFailure as exc:
            self.connection_attempts.append(
                'failed to connect client %r: %s' % (client, exc))
            return None

    def _init_client(self):
        self.client = self._connect(host, port)
        if HAVE_SSL and not self.client:
            # Is MongoDB configured for SSL?
            self.client = self._connect(host, port, **_SSL_OPTIONS)
            if self.client:
                self.ssl = True
                self.default_client_options.update(_SSL_OPTIONS)
                self.ssl_certfile = True
                if _SSL_OPTIONS.get('ssl_cert_reqs') == ssl.CERT_NONE:
                    self.ssl_cert_none = True

        if self.client:
            self.connected = True

            try:
                self.cmd_line = self.client.admin.command('getCmdLineOpts')
            except pymongo.errors.OperationFailure as e:
                msg = e.details.get('errmsg', '')
                if e.code == 13 or 'unauthorized' in msg or 'login' in msg:
                    # Unauthorized.
                    self.auth_enabled = True
                else:
                    raise
            else:
                self.auth_enabled = self._server_started_with_auth()

            if self.auth_enabled:
                # See if db_user already exists.
                if not self._check_user_provided():
                    _create_user(self.client.admin, db_user, db_pwd)

                self.client = self._connect(
                    host, port, username=db_user, password=db_pwd,
                    replicaSet=self.replica_set_name,
                    **self.default_client_options)

                # May not have this if OperationFailure was raised earlier.
                self.cmd_line = self.client.admin.command('getCmdLineOpts')

            self.server_status = self.client.admin.command('serverStatus')
            self.ismaster = ismaster = self.client.admin.command('isMaster')
            self.sessions_enabled = 'logicalSessionTimeoutMinutes' in ismaster

            if 'setName' in ismaster:
                self.replica_set_name = str(ismaster['setName'])
                self.is_rs = True
                if self.auth_enabled:
                    # It doesn't matter which member we use as the seed here.
                    self.client = pymongo.MongoClient(
                        host,
                        port,
                        username=db_user,
                        password=db_pwd,
                        replicaSet=self.replica_set_name,
                        **self.default_client_options)
                else:
                    self.client = pymongo.MongoClient(
                        host,
                        port,
                        replicaSet=self.replica_set_name,
                        **self.default_client_options)

                # Get the authoritative ismaster result from the primary.
                self.ismaster = self.client.admin.command('ismaster')
                nodes = [partition_node(node.lower())
                         for node in self.ismaster.get('hosts', [])]
                nodes.extend([partition_node(node.lower())
                              for node in self.ismaster.get('passives', [])])
                nodes.extend([partition_node(node.lower())
                              for node in self.ismaster.get('arbiters', [])])
                self.nodes = set(nodes)
            else:
                self.ismaster = ismaster
                self.nodes = set([(host, port)])
            self.w = len(self.ismaster.get("hosts", [])) or 1
            self.version = Version.from_client(self.client)

            if 'enableTestCommands=1' in self.cmd_line['argv']:
                self.test_commands_enabled = True
            elif 'parsed' in self.cmd_line:
                params = self.cmd_line['parsed'].get('setParameter', [])
                if 'enableTestCommands=1' in params:
                    self.test_commands_enabled = True
                else:
                    params = self.cmd_line['parsed'].get('setParameter', {})
                    if params.get('enableTestCommands') == '1':
                        self.test_commands_enabled = True

            self.is_mongos = (self.ismaster.get('msg') == 'isdbgrid')
            self.has_ipv6 = self._server_started_with_ipv6()

    def init(self):
        with self.conn_lock:
            if not self.client and not self.connection_attempts:
                self._init_client()

    def connection_attempt_info(self):
        return '\n'.join(self.connection_attempts)

    @property
    def host(self):
        if self.is_rs:
            primary = self.client.primary
            return str(primary[0]) if primary is not None else host
        return host

    @property
    def port(self):
        if self.is_rs:
            primary = self.client.primary
            return primary[1] if primary is not None else port
        return port

    @property
    def pair(self):
        return "%s:%d" % (self.host, self.port)

    @property
    def has_secondaries(self):
        if not self.client:
            return False
        return bool(len(self.client.secondaries))

    def _check_user_provided(self):
        """Return True if db_user/db_password is already an admin user."""
        client = pymongo.MongoClient(
            host, port,
            username=db_user,
            password=db_pwd,
            serverSelectionTimeoutMS=100,
            **self.default_client_options)

        try:
            return db_user in _all_users(client.admin)
        except pymongo.errors.OperationFailure as e:
            msg = e.details.get('errmsg', '')
            if e.code == 18 or 'auth fails' in msg:
                # Auth failed.
                return False
            else:
                raise

    def _server_started_with_auth(self):
        # MongoDB >= 2.0
        if 'parsed' in self.cmd_line:
            parsed = self.cmd_line['parsed']
            # MongoDB >= 2.6
            if 'security' in parsed:
                security = parsed['security']
                # >= rc3
                if 'authorization' in security:
                    return security['authorization'] == 'enabled'
                # < rc3
                return (security.get('auth', False) or
                        bool(security.get('keyFile')))
            return parsed.get('auth', False) or bool(parsed.get('keyFile'))
        # Legacy
        argv = self.cmd_line['argv']
        return '--auth' in argv or '--keyFile' in argv

    def _server_started_with_ipv6(self):
        if not socket.has_ipv6:
            return False

        if 'parsed' in self.cmd_line:
            if not self.cmd_line['parsed'].get('net', {}).get('ipv6'):
                return False
        else:
            if '--ipv6' not in self.cmd_line['argv']:
                return False

        # The server was started with --ipv6. Is there an IPv6 route to it?
        try:
            for info in socket.getaddrinfo(self.host, self.port):
                if info[0] == socket.AF_INET6:
                    return True
        except socket.error:
            pass

        return False

    def _require(self, condition, msg, func=None):
        def make_wrapper(f):
            @wraps(f)
            def wrap(*args, **kwargs):
                self.init()
                # Always raise SkipTest if we can't connect to MongoDB
                if not self.connected:
                    raise SkipTest(
                        "Cannot connect to MongoDB on %s" % (self.pair,))
                if condition():
                    return f(*args, **kwargs)
                raise SkipTest(msg)
            return wrap

        if func is None:
            def decorate(f):
                return make_wrapper(f)
            return decorate
        return make_wrapper(func)

    def create_user(self, dbname, user, pwd=None, roles=None, **kwargs):
        kwargs['writeConcern'] = {'w': self.w}
        return _create_user(self.client[dbname], user, pwd, roles, **kwargs)

    def drop_user(self, dbname, user):
        self.client[dbname].command(
            'dropUser', user, writeConcern={'w': self.w})

    def require_connection(self, func):
        """Run a test only if we can connect to MongoDB."""
        return self._require(
            lambda: True,  # _require checks if we're connected
            "Cannot connect to MongoDB on %s" % (self.pair,),
            func=func)

    def require_no_mmap(self, func):
        """Run a test only if the server is not using the MMAPv1 storage
        engine. Only works for standalone and replica sets; tests are
        run regardless of storage engine on sharded clusters. """
        def is_not_mmap():
            if self.is_mongos:
                return True
            try:
                storage_engine = self.server_status.get(
                    'storageEngine').get('name')
            except AttributeError:
                # Raised if the storageEngine key does not exist or if
                # self.server_status is None.
                return False
            return storage_engine != 'mmapv1'

        return self._require(
            is_not_mmap, "Storage engine must not be MMAPv1", func=func)

    def require_version_min(self, *ver):
        """Run a test only if the server version is at least ``version``."""
        other_version = Version(*ver)
        return self._require(lambda: self.version >= other_version,
                             "Server version must be at least %s"
                             % str(other_version))

    def require_version_max(self, *ver):
        """Run a test only if the server version is at most ``version``."""
        other_version = Version(*ver)
        return self._require(lambda: self.version <= other_version,
                             "Server version must be at most %s"
                             % str(other_version))

    def require_auth(self, func):
        """Run a test only if the server is running with auth enabled."""
        return self.check_auth_with_sharding(
            self._require(lambda: self.auth_enabled,
                          "Authentication is not enabled on the server",
                          func=func))

    def require_no_auth(self, func):
        """Run a test only if the server is running without auth enabled."""
        return self._require(lambda: not self.auth_enabled,
                             "Authentication must not be enabled on the server",
                             func=func)

    def require_replica_set(self, func):
        """Run a test only if the client is connected to a replica set."""
        return self._require(lambda: self.is_rs,
                             "Not connected to a replica set",
                             func=func)

    def require_secondaries_count(self, count):
        """Run a test only if the client is connected to a replica set that has
        `count` secondaries.
        """
        def sec_count():
            return 0 if not self.client else len(self.client.secondaries)
        return self._require(lambda: sec_count() >= count,
                             "Not enough secondaries available")

    def require_no_replica_set(self, func):
        """Run a test if the client is *not* connected to a replica set."""
        return self._require(
            lambda: not self.is_rs,
            "Connected to a replica set, not a standalone mongod",
            func=func)

    def require_ipv6(self, func):
        """Run a test only if the client can connect to a server via IPv6."""
        return self._require(lambda: self.has_ipv6,
                             "No IPv6",
                             func=func)

    def require_no_mongos(self, func):
        """Run a test only if the client is not connected to a mongos."""
        return self._require(lambda: not self.is_mongos,
                             "Must be connected to a mongod, not a mongos",
                             func=func)

    def require_mongos(self, func):
        """Run a test only if the client is connected to a mongos."""
        return self._require(lambda: self.is_mongos,
                             "Must be connected to a mongos",
                             func=func)

    def require_standalone(self, func):
        """Run a test only if the client is connected to a standalone."""
        return self._require(lambda: not (self.is_mongos or self.is_rs),
                             "Must be connected to a standalone",
                             func=func)

    def require_no_standalone(self, func):
        """Run a test only if the client is not connected to a standalone."""
        return self._require(lambda: self.is_mongos or self.is_rs,
                             "Must be connected to a replica set or mongos",
                             func=func)

    def check_auth_with_sharding(self, func):
        """Skip a test when connected to mongos < 2.0 and running with auth."""
        condition = lambda: not (self.auth_enabled and
                         self.is_mongos and self.version < (2,))
        return self._require(condition,
                             "Auth with sharding requires MongoDB >= 2.0.0",
                             func=func)

    def require_cluster_type(self, topologies=[]):
        """Run a test only if the client is connected to a cluster that
        conforms to one of the specified topologies. Acceptable topologies
        are 'single', 'replicaset', and 'sharded'."""
        def _is_valid_topology():
            if 'single' in topologies and not (self.is_mongos or self.is_rs):
                return True
            if 'replicaset' in topologies and self.is_rs:
                return True
            if 'sharded' in topologies and self.is_mongos:
                return True
            return False
        return self._require(
            _is_valid_topology, 
            "Cluster type not in %s" % (topologies))

    def require_test_commands(self, func):
        """Run a test only if the server has test commands enabled."""
        return self._require(lambda: self.test_commands_enabled,
                             "Test commands must be enabled",
                             func=func)

    def require_ssl(self, func):
        """Run a test only if the client can connect over SSL."""
        return self._require(lambda: self.ssl,
                             "Must be able to connect via SSL",
                             func=func)

    def require_no_ssl(self, func):
        """Run a test only if the client can connect over SSL."""
        return self._require(lambda: not self.ssl,
                             "Must be able to connect without SSL",
                             func=func)

    def require_ssl_cert_none(self, func):
        """Run a test only if the client can connect with ssl.CERT_NONE."""
        return self._require(lambda: self.ssl_cert_none,
                             "Must be able to connect with ssl.CERT_NONE",
                             func=func)

    def require_ssl_certfile(self, func):
        """Run a test only if the client can connect with ssl_certfile."""
        return self._require(lambda: self.ssl_certfile,
                             "Must be able to connect with ssl_certfile",
                             func=func)

    def require_server_resolvable(self, func):
        """Run a test only if the hostname 'server' is resolvable."""
        return self._require(lambda: self.server_is_resolvable,
                             "No hosts entry for 'server'. Cannot validate "
                             "hostname in the certificate",
                             func=func)

    def require_sessions(self, func):
        """Run a test only if the deployment supports sessions."""
        return self._require(lambda: self.sessions_enabled,
                             "Sessions not supported",
                             func=func)

    def require_transactions(self, func):
        """Run a test only if the deployment might support transactions.

        *Might* because this does not test the storage engine or FCV.
        """
        new_func = self.require_version_min(4, 0, 0, -1)(func)
        return self.require_replica_set(new_func)


# Reusable client context
client_context = ClientContext()


def sanitize_cmd(cmd):
    cp = cmd.copy()
    cp.pop('$clusterTime', None)
    cp.pop('$db', None)
    cp.pop('$readPreference', None)
    cp.pop('lsid', None)
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
    cp.pop('$clusterTime', None)
    cp.pop('operationTime', None)
    return cp


class PyMongoTestCase(unittest.TestCase):
    def assertEqualCommand(self, expected, actual, msg=None):
        self.assertEqual(sanitize_cmd(expected), sanitize_cmd(actual), msg)

    def assertEqualReply(self, expected, actual, msg=None):
        self.assertEqual(sanitize_reply(expected), sanitize_reply(actual), msg)


class IntegrationTest(PyMongoTestCase):
    """Base class for TestCases that need a connection to MongoDB to pass."""

    @classmethod
    @client_context.require_connection
    def setUpClass(cls):
        cls.client = client_context.client
        cls.db = cls.client.pymongo_test
        if client_context.auth_enabled:
            cls.credentials = {'username': db_user, 'password': db_pwd}
        else:
            cls.credentials = {}


# Use assertRaisesRegex if available, otherwise use Python 2.7's
# deprecated assertRaisesRegexp, with a 'p'.
if not hasattr(unittest.TestCase, 'assertRaisesRegex'):
    IntegrationTest.assertRaisesRegex = unittest.TestCase.assertRaisesRegexp


class MockClientTest(unittest.TestCase):
    """Base class for TestCases that use MockClient.

    This class is *not* an IntegrationTest: if properly written, MockClient
    tests do not require a running server.

    The class temporarily overrides HEARTBEAT_FREQUENCY to speed up tests.
    """

    def setUp(self):
        super(MockClientTest, self).setUp()

        self.client_knobs = client_knobs(
            heartbeat_frequency=0.001,
            min_heartbeat_interval=0.001)

        self.client_knobs.enable()

    def tearDown(self):
        self.client_knobs.disable()
        super(MockClientTest, self).tearDown()


def setup():
    client_context.init()
    warnings.resetwarnings()
    warnings.simplefilter("always")


def teardown():
    c = client_context.client
    c.drop_database("pymongo-pooling-tests")
    c.drop_database("pymongo_test")
    c.drop_database("pymongo_test1")
    c.drop_database("pymongo_test2")
    c.drop_database("pymongo_test_mike")
    c.drop_database("pymongo_test_bernie")


class PymongoTestRunner(unittest.TextTestRunner):
    def run(self, test):
        setup()
        result = super(PymongoTestRunner, self).run(test)
        try:
            teardown()
        finally:
            return result


def test_cases(suite):
    """Iterator over all TestCases within a TestSuite."""
    for suite_or_case in suite._tests:
        if isinstance(suite_or_case, unittest.TestCase):
            # unittest.TestCase
            yield suite_or_case
        else:
            # unittest.TestSuite
            for case in test_cases(suite_or_case):
                yield case
