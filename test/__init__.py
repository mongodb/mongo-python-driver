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

import base64
import gc
import os
import socket
import sys
import threading
import traceback
import unittest
import warnings

try:
    from xmlrunner import XMLTestRunner

    HAVE_XML = True
# ValueError is raised when version 3+ is installed on Jython 2.7.
except (ImportError, ValueError):
    HAVE_XML = False

try:
    import ipaddress  # noqa

    HAVE_IPADDRESS = True
except ImportError:
    HAVE_IPADDRESS = False

from contextlib import contextmanager
from functools import wraps
from test.version import Version
from typing import Dict, no_type_check
from unittest import SkipTest
from urllib.parse import quote_plus

import pymongo
import pymongo.errors
from bson.son import SON
from pymongo import common, message
from pymongo.common import partition_node
from pymongo.database import Database
from pymongo.hello import HelloCompat
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.ssl_support import HAVE_SSL, _ssl
from pymongo.uri_parser import parse_uri

if HAVE_SSL:
    import ssl

try:
    # Enable the fault handler to dump the traceback of each running thread
    # after a segfault.
    import faulthandler

    faulthandler.enable()
except ImportError:
    pass

# Enable debug output for uncollectable objects. PyPy does not have set_debug.
if hasattr(gc, "set_debug"):
    gc.set_debug(
        gc.DEBUG_UNCOLLECTABLE | getattr(gc, "DEBUG_OBJECTS", 0) | getattr(gc, "DEBUG_INSTANCES", 0)
    )

# The host and port of a single mongod or mongos, or the seed host
# for a replica set.
host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))

db_user = os.environ.get("DB_USER", "user")
db_pwd = os.environ.get("DB_PASSWORD", "password")

CERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "certificates")
CLIENT_PEM = os.environ.get("CLIENT_PEM", os.path.join(CERT_PATH, "client.pem"))
CA_PEM = os.environ.get("CA_PEM", os.path.join(CERT_PATH, "ca.pem"))

TLS_OPTIONS: Dict = dict(tls=True)
if CLIENT_PEM:
    TLS_OPTIONS["tlsCertificateKeyFile"] = CLIENT_PEM
if CA_PEM:
    TLS_OPTIONS["tlsCAFile"] = CA_PEM

COMPRESSORS = os.environ.get("COMPRESSORS")
MONGODB_API_VERSION = os.environ.get("MONGODB_API_VERSION")
TEST_LOADBALANCER = bool(os.environ.get("TEST_LOADBALANCER"))
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


def is_server_resolvable():
    """Returns True if 'server' is resolvable."""
    socket_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(1)
    try:
        try:
            socket.gethostbyname("server")
            return True
        except socket.error:
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


class client_knobs(object):
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
            def wrap(*args, **kwargs):
                with self:
                    return f(*args, **kwargs)

            return wrap

        return make_wrapper(func)

    def __del__(self):
        if self._enabled:
            msg = (
                "ERROR: client_knobs still enabled! HEARTBEAT_FREQUENCY=%s, "
                "MIN_HEARTBEAT_INTERVAL=%s, KILL_CURSOR_FREQUENCY=%s, "
                "EVENTS_QUEUE_FREQUENCY=%s, stack:\n%s"
                % (
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
    return set(u["user"] for u in db.command("usersInfo").get("users", []))


class ClientContext(object):
    client: MongoClient

    MULTI_MONGOS_LB_URI = MULTI_MONGOS_LB_URI

    def __init__(self):
        """Create a client and grab essential information from the server."""
        self.connection_attempts = []
        self.connected = False
        self.w = None
        self.nodes = set()
        self.replica_set_name = None
        self.cmd_line = None
        self.server_status = None
        self.version = Version(-1)  # Needs to be comparable with Version
        self.auth_enabled = False
        self.test_commands_enabled = False
        self.server_parameters = {}
        self._hello = None
        self.is_mongos = False
        self.mongoses = []
        self.is_rs = False
        self.has_ipv6 = False
        self.tls = False
        self.tlsCertificateKeyFile = False
        self.server_is_resolvable = is_server_resolvable()
        self.default_client_options: Dict = {}
        self.sessions_enabled = False
        self.client = None  # type: ignore
        self.conn_lock = threading.Lock()
        self.is_data_lake = False
        self.load_balancer = TEST_LOADBALANCER
        self.serverless = TEST_SERVERLESS
        if self.load_balancer or self.serverless:
            self.default_client_options["loadBalanced"] = True
        if COMPRESSORS:
            self.default_client_options["compressors"] = COMPRESSORS
        if MONGODB_API_VERSION:
            server_api = ServerApi(MONGODB_API_VERSION)
            self.default_client_options["server_api"] = server_api

    @property
    def client_options(self):
        """Return the MongoClient options for creating a duplicate client."""
        opts = client_context.default_client_options.copy()
        if client_context.auth_enabled:
            opts["username"] = db_user
            opts["password"] = db_pwd
        if self.replica_set_name:
            opts["replicaSet"] = self.replica_set_name
        return opts

    @property
    def uri(self):
        """Return the MongoClient URI for creating a duplicate client."""
        opts = client_context.default_client_options.copy()
        opts.pop("server_api", None)  # Cannot be set from the URI
        opts_parts = []
        for opt, val in opts.items():
            strval = str(val)
            if isinstance(val, bool):
                strval = strval.lower()
            opts_parts.append(f"{opt}={quote_plus(strval)}")
        opts_part = "&".join(opts_parts)
        auth_part = ""
        if client_context.auth_enabled:
            auth_part = f"{quote_plus(db_user)}:{quote_plus(db_pwd)}@"
        return f"mongodb://{auth_part}{self.pair}/?{opts_part}"

    @property
    def hello(self):
        if not self._hello:
            self._hello = self.client.admin.command(HelloCompat.LEGACY_CMD)
        return self._hello

    def _connect(self, host, port, **kwargs):
        kwargs.update(self.default_client_options)
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000, **kwargs)
        try:
            try:
                client.admin.command(HelloCompat.LEGACY_CMD)  # Can we connect?
            except pymongo.errors.OperationFailure as exc:
                # SERVER-32063
                self.connection_attempts.append(
                    "connected client %r, but legacy hello failed: %s" % (client, exc)
                )
            else:
                self.connection_attempts.append("successfully connected client %r" % (client,))
            # If connected, then return client with default timeout
            return pymongo.MongoClient(host, port, **kwargs)
        except pymongo.errors.ConnectionFailure as exc:
            self.connection_attempts.append("failed to connect client %r: %s" % (client, exc))
            return None
        finally:
            client.close()

    def _init_client(self):
        self.client = self._connect(host, port)

        if self.client is not None:
            # Return early when connected to dataLake as mongohoused does not
            # support the getCmdLineOpts command and is tested without TLS.
            build_info = self.client.admin.command("buildInfo")
            if "dataLake" in build_info:
                self.is_data_lake = True
                self.auth_enabled = True
                self.client = self._connect(host, port, username=db_user, password=db_pwd)
                self.connected = True
                return

        if HAVE_SSL and not self.client:
            # Is MongoDB configured for SSL?
            self.client = self._connect(host, port, **TLS_OPTIONS)
            if self.client:
                self.tls = True
                self.default_client_options.update(TLS_OPTIONS)
                self.tlsCertificateKeyFile = True

        if self.client:
            self.connected = True

            if self.serverless:
                self.auth_enabled = True
            else:
                try:
                    self.cmd_line = self.client.admin.command("getCmdLineOpts")
                except pymongo.errors.OperationFailure as e:
                    assert e.details is not None
                    msg = e.details.get("errmsg", "")
                    if e.code == 13 or "unauthorized" in msg or "login" in msg:
                        # Unauthorized.
                        self.auth_enabled = True
                    else:
                        raise
                else:
                    self.auth_enabled = self._server_started_with_auth()

            if self.auth_enabled:
                if not self.serverless:
                    # See if db_user already exists.
                    if not self._check_user_provided():
                        _create_user(self.client.admin, db_user, db_pwd)

                self.client = self._connect(
                    host,
                    port,
                    username=db_user,
                    password=db_pwd,
                    replicaSet=self.replica_set_name,
                    **self.default_client_options,
                )

                # May not have this if OperationFailure was raised earlier.
                self.cmd_line = self.client.admin.command("getCmdLineOpts")

            if self.serverless:
                self.server_status = {}
            else:
                self.server_status = self.client.admin.command("serverStatus")
                if self.storage_engine == "mmapv1":
                    # MMAPv1 does not support retryWrites=True.
                    self.default_client_options["retryWrites"] = False

            hello = self.hello
            self.sessions_enabled = "logicalSessionTimeoutMinutes" in hello

            if "setName" in hello:
                self.replica_set_name = str(hello["setName"])
                self.is_rs = True
                if self.auth_enabled:
                    # It doesn't matter which member we use as the seed here.
                    self.client = pymongo.MongoClient(
                        host,
                        port,
                        username=db_user,
                        password=db_pwd,
                        replicaSet=self.replica_set_name,
                        **self.default_client_options,
                    )
                else:
                    self.client = pymongo.MongoClient(
                        host, port, replicaSet=self.replica_set_name, **self.default_client_options
                    )

                # Get the authoritative hello result from the primary.
                self._hello = None
                hello = self.hello
                nodes = [partition_node(node.lower()) for node in hello.get("hosts", [])]
                nodes.extend([partition_node(node.lower()) for node in hello.get("passives", [])])
                nodes.extend([partition_node(node.lower()) for node in hello.get("arbiters", [])])
                self.nodes = set(nodes)
            else:
                self.nodes = set([(host, port)])
            self.w = len(hello.get("hosts", [])) or 1
            self.version = Version.from_client(self.client)

            if self.serverless:
                self.server_parameters = {
                    "requireApiVersion": False,
                    "enableTestCommands": True,
                }
                self.test_commands_enabled = True
                self.has_ipv6 = False
            else:
                self.server_parameters = self.client.admin.command("getParameter", "*")
                assert self.cmd_line is not None
                if "enableTestCommands=1" in self.cmd_line["argv"]:
                    self.test_commands_enabled = True
                elif "parsed" in self.cmd_line:
                    params = self.cmd_line["parsed"].get("setParameter", [])
                    if "enableTestCommands=1" in params:
                        self.test_commands_enabled = True
                    else:
                        params = self.cmd_line["parsed"].get("setParameter", {})
                        if params.get("enableTestCommands") == "1":
                            self.test_commands_enabled = True
                    self.has_ipv6 = self._server_started_with_ipv6()

            self.is_mongos = self.hello.get("msg") == "isdbgrid"
            if self.is_mongos:
                address = self.client.address
                self.mongoses.append(address)
                if not self.serverless:
                    # Check for another mongos on the next port.
                    assert address is not None
                    next_address = address[0], address[1] + 1
                    mongos_client = self._connect(*next_address, **self.default_client_options)
                    if mongos_client:
                        hello = mongos_client.admin.command(HelloCompat.LEGACY_CMD)
                        if hello.get("msg") == "isdbgrid":
                            self.mongoses.append(next_address)

    def init(self):
        with self.conn_lock:
            if not self.client and not self.connection_attempts:
                self._init_client()

    def connection_attempt_info(self):
        return "\n".join(self.connection_attempts)

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

    @property
    def storage_engine(self):
        try:
            return self.server_status.get("storageEngine", {}).get("name")
        except AttributeError:
            # Raised if self.server_status is None.
            return None

    def _check_user_provided(self):
        """Return True if db_user/db_password is already an admin user."""
        client = pymongo.MongoClient(
            host,
            port,
            username=db_user,
            password=db_pwd,
            serverSelectionTimeoutMS=100,
            **self.default_client_options,
        )

        try:
            return db_user in _all_users(client.admin)
        except pymongo.errors.OperationFailure as e:
            assert e.details is not None
            msg = e.details.get("errmsg", "")
            if e.code == 18 or "auth fails" in msg:
                # Auth failed.
                return False
            else:
                raise

    def _server_started_with_auth(self):
        # MongoDB >= 2.0
        assert self.cmd_line is not None
        if "parsed" in self.cmd_line:
            parsed = self.cmd_line["parsed"]
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
        argv = self.cmd_line["argv"]
        return "--auth" in argv or "--keyFile" in argv

    def _server_started_with_ipv6(self):
        if not socket.has_ipv6:
            return False

        assert self.cmd_line is not None
        if "parsed" in self.cmd_line:
            if not self.cmd_line["parsed"].get("net", {}).get("ipv6"):
                return False
        else:
            if "--ipv6" not in self.cmd_line["argv"]:
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
                    raise SkipTest("Cannot connect to MongoDB on %s" % (self.pair,))
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
        kwargs["writeConcern"] = {"w": self.w}
        return _create_user(self.client[dbname], user, pwd, roles, **kwargs)

    def drop_user(self, dbname, user):
        self.client[dbname].command("dropUser", user, writeConcern={"w": self.w})

    def require_connection(self, func):
        """Run a test only if we can connect to MongoDB."""
        return self._require(
            lambda: True,  # _require checks if we're connected
            "Cannot connect to MongoDB on %s" % (self.pair,),
            func=func,
        )

    def require_data_lake(self, func):
        """Run a test only if we are connected to Atlas Data Lake."""
        return self._require(
            lambda: self.is_data_lake,
            "Not connected to Atlas Data Lake on %s" % (self.pair,),
            func=func,
        )

    def require_no_mmap(self, func):
        """Run a test only if the server is not using the MMAPv1 storage
        engine. Only works for standalone and replica sets; tests are
        run regardless of storage engine on sharded clusters."""

        def is_not_mmap():
            if self.is_mongos:
                return True
            return self.storage_engine != "mmapv1"

        return self._require(is_not_mmap, "Storage engine must not be MMAPv1", func=func)

    def require_version_min(self, *ver):
        """Run a test only if the server version is at least ``version``."""
        other_version = Version(*ver)
        return self._require(
            lambda: self.version >= other_version,
            "Server version must be at least %s" % str(other_version),
        )

    def require_version_max(self, *ver):
        """Run a test only if the server version is at most ``version``."""
        other_version = Version(*ver)
        return self._require(
            lambda: self.version <= other_version,
            "Server version must be at most %s" % str(other_version),
        )

    def require_auth(self, func):
        """Run a test only if the server is running with auth enabled."""
        return self._require(
            lambda: self.auth_enabled, "Authentication is not enabled on the server", func=func
        )

    def require_no_auth(self, func):
        """Run a test only if the server is running without auth enabled."""
        return self._require(
            lambda: not self.auth_enabled,
            "Authentication must not be enabled on the server",
            func=func,
        )

    def require_replica_set(self, func):
        """Run a test only if the client is connected to a replica set."""
        return self._require(lambda: self.is_rs, "Not connected to a replica set", func=func)

    def require_secondaries_count(self, count):
        """Run a test only if the client is connected to a replica set that has
        `count` secondaries.
        """

        def sec_count():
            return 0 if not self.client else len(self.client.secondaries)

        return self._require(lambda: sec_count() >= count, "Not enough secondaries available")

    @property
    def supports_secondary_read_pref(self):
        if self.has_secondaries:
            return True
        if self.is_mongos:
            shard = self.client.config.shards.find_one()["host"]
            num_members = shard.count(",") + 1
            return num_members > 1
        return False

    def require_secondary_read_pref(self):
        """Run a test only if the client is connected to a cluster that
        supports secondary read preference
        """
        return self._require(
            lambda: self.supports_secondary_read_pref,
            "This cluster does not support secondary read preference",
        )

    def require_no_replica_set(self, func):
        """Run a test if the client is *not* connected to a replica set."""
        return self._require(
            lambda: not self.is_rs, "Connected to a replica set, not a standalone mongod", func=func
        )

    def require_ipv6(self, func):
        """Run a test only if the client can connect to a server via IPv6."""
        return self._require(lambda: self.has_ipv6, "No IPv6", func=func)

    def require_no_mongos(self, func):
        """Run a test only if the client is not connected to a mongos."""
        return self._require(
            lambda: not self.is_mongos, "Must be connected to a mongod, not a mongos", func=func
        )

    def require_mongos(self, func):
        """Run a test only if the client is connected to a mongos."""
        return self._require(lambda: self.is_mongos, "Must be connected to a mongos", func=func)

    def require_multiple_mongoses(self, func):
        """Run a test only if the client is connected to a sharded cluster
        that has 2 mongos nodes."""
        return self._require(
            lambda: len(self.mongoses) > 1, "Must have multiple mongoses available", func=func
        )

    def require_standalone(self, func):
        """Run a test only if the client is connected to a standalone."""
        return self._require(
            lambda: not (self.is_mongos or self.is_rs),
            "Must be connected to a standalone",
            func=func,
        )

    def require_no_standalone(self, func):
        """Run a test only if the client is not connected to a standalone."""
        return self._require(
            lambda: self.is_mongos or self.is_rs,
            "Must be connected to a replica set or mongos",
            func=func,
        )

    def require_load_balancer(self, func):
        """Run a test only if the client is connected to a load balancer."""
        return self._require(
            lambda: self.load_balancer, "Must be connected to a load balancer", func=func
        )

    def require_no_load_balancer(self, func):
        """Run a test only if the client is not connected to a load balancer."""
        return self._require(
            lambda: not self.load_balancer, "Must not be connected to a load balancer", func=func
        )

    def is_topology_type(self, topologies):
        unknown = set(topologies) - {
            "single",
            "replicaset",
            "sharded",
            "sharded-replicaset",
            "load-balanced",
        }
        if unknown:
            raise AssertionError("Unknown topologies: %r" % (unknown,))
        if self.load_balancer:
            if "load-balanced" in topologies:
                return True
            return False
        if "single" in topologies and not (self.is_mongos or self.is_rs):
            return True
        if "replicaset" in topologies and self.is_rs:
            return True
        if "sharded" in topologies and self.is_mongos:
            return True
        if "sharded-replicaset" in topologies and self.is_mongos:
            shards = list(client_context.client.config.shards.find())
            for shard in shards:
                # For a 3-member RS-backed sharded cluster, shard['host']
                # will be 'replicaName/ip1:port1,ip2:port2,ip3:port3'
                # Otherwise it will be 'ip1:port1'
                host_spec = shard["host"]
                if not len(host_spec.split("/")) > 1:
                    return False
            return True
        return False

    def require_cluster_type(self, topologies=[]):  # noqa
        """Run a test only if the client is connected to a cluster that
        conforms to one of the specified topologies. Acceptable topologies
        are 'single', 'replicaset', and 'sharded'."""

        def _is_valid_topology():
            return self.is_topology_type(topologies)

        return self._require(_is_valid_topology, "Cluster type not in %s" % (topologies))

    def require_test_commands(self, func):
        """Run a test only if the server has test commands enabled."""
        return self._require(
            lambda: self.test_commands_enabled, "Test commands must be enabled", func=func
        )

    def require_failCommand_fail_point(self, func):
        """Run a test only if the server supports the failCommand fail
        point."""
        return self._require(
            lambda: self.supports_failCommand_fail_point,
            "failCommand fail point must be supported",
            func=func,
        )

    def require_failCommand_appName(self, func):
        """Run a test only if the server supports the failCommand appName."""
        # SERVER-47195
        return self._require(
            lambda: (self.test_commands_enabled and self.version >= (4, 4, -1)),
            "failCommand appName must be supported",
            func=func,
        )

    def require_failCommand_blockConnection(self, func):
        """Run a test only if the server supports failCommand blockConnection."""
        return self._require(
            lambda: (
                self.test_commands_enabled
                and (
                    (not self.is_mongos and self.version >= (4, 2, 9))
                    or (self.is_mongos and self.version >= (4, 4))
                )
            ),
            "failCommand blockConnection is not supported",
            func=func,
        )

    def require_tls(self, func):
        """Run a test only if the client can connect over TLS."""
        return self._require(lambda: self.tls, "Must be able to connect via TLS", func=func)

    def require_no_tls(self, func):
        """Run a test only if the client can connect over TLS."""
        return self._require(lambda: not self.tls, "Must be able to connect without TLS", func=func)

    def require_tlsCertificateKeyFile(self, func):
        """Run a test only if the client can connect with tlsCertificateKeyFile."""
        return self._require(
            lambda: self.tlsCertificateKeyFile,
            "Must be able to connect with tlsCertificateKeyFile",
            func=func,
        )

    def require_server_resolvable(self, func):
        """Run a test only if the hostname 'server' is resolvable."""
        return self._require(
            lambda: self.server_is_resolvable,
            "No hosts entry for 'server'. Cannot validate hostname in the certificate",
            func=func,
        )

    def require_sessions(self, func):
        """Run a test only if the deployment supports sessions."""
        return self._require(lambda: self.sessions_enabled, "Sessions not supported", func=func)

    def supports_retryable_writes(self):
        if self.storage_engine == "mmapv1":
            return False
        if not self.sessions_enabled:
            return False
        return self.is_mongos or self.is_rs

    def require_retryable_writes(self, func):
        """Run a test only if the deployment supports retryable writes."""
        return self._require(
            self.supports_retryable_writes,
            "This server does not support retryable writes",
            func=func,
        )

    def supports_transactions(self):
        if self.storage_engine == "mmapv1":
            return False

        if self.version.at_least(4, 1, 8):
            return self.is_mongos or self.is_rs

        if self.version.at_least(4, 0):
            return self.is_rs

        return False

    def require_transactions(self, func):
        """Run a test only if the deployment might support transactions.

        *Might* because this does not test the storage engine or FCV.
        """
        return self._require(
            self.supports_transactions, "Transactions are not supported", func=func
        )

    def require_no_api_version(self, func):
        """Skip this test when testing with requireApiVersion."""
        return self._require(
            lambda: not MONGODB_API_VERSION,
            "This test does not work with requireApiVersion",
            func=func,
        )

    def mongos_seeds(self):
        return ",".join("%s:%s" % address for address in self.mongoses)

    @property
    def supports_failCommand_fail_point(self):
        """Does the server support the failCommand fail point?"""
        if self.is_mongos:
            return self.version.at_least(4, 1, 5) and self.test_commands_enabled
        else:
            return self.version.at_least(4, 0) and self.test_commands_enabled

    @property
    def requires_hint_with_min_max_queries(self):
        """Does the server require a hint with min/max queries."""
        # Changed in SERVER-39567.
        return self.version.at_least(4, 1, 10)

    @property
    def max_bson_size(self):
        return self.hello["maxBsonObjectSize"]

    @property
    def max_write_batch_size(self):
        return self.hello["maxWriteBatchSize"]


# Reusable client context
client_context = ClientContext()


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


class PyMongoTestCase(unittest.TestCase):
    def assertEqualCommand(self, expected, actual, msg=None):
        self.assertEqual(sanitize_cmd(expected), sanitize_cmd(actual), msg)

    def assertEqualReply(self, expected, actual, msg=None):
        self.assertEqual(sanitize_reply(expected), sanitize_reply(actual), msg)

    @contextmanager
    def fail_point(self, command_args):
        cmd_on = SON([("configureFailPoint", "failCommand")])
        cmd_on.update(command_args)
        client_context.client.admin.command(cmd_on)
        try:
            yield
        finally:
            client_context.client.admin.command(
                "configureFailPoint", cmd_on["configureFailPoint"], mode="off"
            )


class IntegrationTest(PyMongoTestCase):
    """Base class for TestCases that need a connection to MongoDB to pass."""

    client: MongoClient
    db: Database
    credentials: Dict[str, str]

    @classmethod
    @client_context.require_connection
    def setUpClass(cls):
        if client_context.load_balancer and not getattr(cls, "RUN_ON_LOAD_BALANCER", False):
            raise SkipTest("this test does not support load balancers")
        if client_context.serverless and not getattr(cls, "RUN_ON_SERVERLESS", False):
            raise SkipTest("this test does not support serverless")
        cls.client = client_context.client
        cls.db = cls.client.pymongo_test
        if client_context.auth_enabled:
            cls.credentials = {"username": db_user, "password": db_pwd}
        else:
            cls.credentials = {}

    def cleanup_colls(self, *collections):
        """Cleanup collections faster than drop_collection."""
        for c in collections:
            c = self.client[c.database.name][c.name]
            c.delete_many({})
            c.drop_indexes()

    def patch_system_certs(self, ca_certs):
        patcher = SystemCertsPatcher(ca_certs)
        self.addCleanup(patcher.disable)


class MockClientTest(unittest.TestCase):
    """Base class for TestCases that use MockClient.

    This class is *not* an IntegrationTest: if properly written, MockClient
    tests do not require a running server.

    The class temporarily overrides HEARTBEAT_FREQUENCY to speed up tests.
    """

    # MockClients tests that use replicaSet, directConnection=True, pass
    # multiple seed addresses, or wait for heartbeat events are incompatible
    # with loadBalanced=True.
    @classmethod
    @client_context.require_no_load_balancer
    def setUpClass(cls):
        pass

    def setUp(self):
        super(MockClientTest, self).setUp()

        self.client_knobs = client_knobs(heartbeat_frequency=0.001, min_heartbeat_interval=0.001)

        self.client_knobs.enable()

    def tearDown(self):
        self.client_knobs.disable()
        super(MockClientTest, self).tearDown()


# Global knobs to speed up the test suite.
global_knobs = client_knobs(events_queue_frequency=0.05)


def setup():
    client_context.init()
    warnings.resetwarnings()
    warnings.simplefilter("always")
    global_knobs.enable()


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
            "  Threads: %s\n"
            "  Topology: %s\n"
            "  Creation traceback:\n%s" % (running, topology, topology._settings._stack)
        )


def print_running_clients():
    from pymongo.topology import Topology

    processed = set()
    # Avoid false positives on the main test client.
    # XXX: Can be removed after PYTHON-1634 or PYTHON-1896.
    c = client_context.client
    if c:
        processed.add(c._topology._topology_id)
    # Call collect to manually cleanup any would-be gc'd clients to avoid
    # false positives.
    gc.collect()
    for obj in gc.get_objects():
        try:
            if isinstance(obj, Topology):
                # Avoid printing the same Topology multiple times.
                if obj._topology_id in processed:
                    continue
                print_running_topology(obj)
                processed.add(obj._topology_id)
        except ReferenceError:
            pass


def teardown():
    global_knobs.disable()
    garbage = []
    for g in gc.garbage:
        garbage.append("GARBAGE: %r" % (g,))
        garbage.append("  gc.get_referents: %r" % (gc.get_referents(g),))
        garbage.append("  gc.get_referrers: %r" % (gc.get_referrers(g),))
    if garbage:
        assert False, "\n".join(garbage)
    c = client_context.client
    if c:
        if not client_context.is_data_lake:
            c.drop_database("pymongo-pooling-tests")
            c.drop_database("pymongo_test")
            c.drop_database("pymongo_test1")
            c.drop_database("pymongo_test2")
            c.drop_database("pymongo_test_mike")
            c.drop_database("pymongo_test_bernie")
        c.close()

    print_running_clients()


class PymongoTestRunner(unittest.TextTestRunner):
    def run(self, test):
        setup()
        result = super(PymongoTestRunner, self).run(test)
        teardown()
        return result


if HAVE_XML:

    class PymongoXMLTestRunner(XMLTestRunner):  # type: ignore[misc]
        def run(self, test):
            setup()
            result = super(PymongoXMLTestRunner, self).run(test)
            teardown()
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


# Helper method to workaround https://bugs.python.org/issue21724
def clear_warning_registry():
    """Clear the __warningregistry__ for all modules."""
    for _, module in list(sys.modules.items()):
        if hasattr(module, "__warningregistry__"):
            setattr(module, "__warningregistry__", {})  # noqa


class SystemCertsPatcher(object):
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
