# Copyright 2010-2015 MongoDB, Inc.
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

"""Clean up databases after running `nosetests`.
"""

import os
import sys
import warnings

import pymongo
from nose.plugins.skip import SkipTest
from pymongo.errors import ConnectionFailure, OperationFailure

# Hostnames retrieved by MongoReplicaSetClient from isMaster will be of unicode
# type in Python 2, so ensure these hostnames are unicodes, too. It makes tests
# like `test_repr` predictable.

# A standalone mongod, mongos, or a seed for a replica set.
host = unicode(os.environ.get("DB_IP", 'localhost'))
port = int(os.environ.get("DB_PORT", 27017))

# Only used by test_master_slave_connection.
host2 = unicode(os.environ.get("DB_IP2", 'localhost'))
port2 = int(os.environ.get("DB_PORT2", 27018))

# Only used by test_master_slave_connection.
host3 = unicode(os.environ.get("DB_IP3", 'localhost'))
port3 = int(os.environ.get("DB_PORT3", 27019))

db_user = unicode(os.environ.get("DB_USER", "user"))
db_pwd = unicode(os.environ.get("DB_PASSWORD", "password"))

def _split_host_port(host_port):
    host, port = host_port.split(':', 1)
    return host, int(port)

# When testing against a replica set, 'host' and 'port' need
# to represent the primary.
try:
    _ismaster = pymongo.MongoClient(host, port).admin.command('ismaster')
    # Master slave topologies don't use setName. The members also aren't
    # aware of one another (e.g. ismaster doesn't have a 'hosts' field).
    if 'setName' in _ismaster:
        _primary = _ismaster['primary']
        host, port = _split_host_port(_primary)
        _secondaries = set(_ismaster['hosts']) - set([_primary])
        for _idx, _host_port in enumerate(_secondaries):
            _host, _port = _split_host_port(_host_port)
            # Enumerate doesn't accept a start argument until python 2.6.
            setattr(sys.modules[__name__], "host%d" % (_idx + 1,), _host)
            setattr(sys.modules[__name__], "port%d" % (_idx + 1,), _port)
except ConnectionFailure:
    pass

pair = '%s:%d' % (host, port)

class AuthContext(object):

    def __init__(self):
        self.auth_enabled = False
        self.restricted_localhost = False
        try:
            self.client = pymongo.MongoClient(host, port)
        except ConnectionFailure:
            self.client = None
        else:
            try:
                command_line = self.client.admin.command('getCmdLineOpts')
                if self._server_started_with_auth(command_line):
                    self.auth_enabled = True
            except OperationFailure, e:
                msg = e.details.get('errmsg', '')
                if e.code == 13 or 'unauthorized' in msg or 'login' in msg:
                    self.auth_enabled = True
                    self.restricted_localhost = True
                else:
                    raise
            # See if the user has already been set up.
            try:
                self.client.admin.authenticate(db_user, db_pwd)
                self.user_provided = True
            except OperationFailure, e:
                msg = e.details.get('errmsg', '')
                if e.code == 18 or 'auth fails' in msg:
                    self.user_provided = False
                else:
                    raise

    def _server_started_with_auth(self, command_line):
        # MongoDB >= 2.0
        if 'parsed' in command_line:
            parsed = command_line['parsed']
            # MongoDB >= 2.6
            if 'security' in parsed:
                security = parsed['security']
                if 'authorization' in security:
                    return security['authorization'] == 'enabled'
                return security.get('auth', bool(security.get('keyFile')))
            return parsed.get('auth', bool(parsed.get('keyFile')))
        # Legacy
        argv = command_line['argv']
        return '--auth' in argv or '--keyFile' in argv

    def add_user_and_log_in(self):
        if not self.user_provided:
            self.client.admin.add_user(db_user, db_pwd,
                                       roles=('userAdminAnyDatabase',
                                              'readWriteAnyDatabase',
                                              'dbAdminAnyDatabase',
                                              'clusterAdmin'))
        self.client.admin.authenticate(db_user, db_pwd)

    def remove_user_and_log_out(self):
        if not self.user_provided:
            self.client.admin.remove_user(db_user)
        self.client.admin.logout()
        self.client.disconnect()


auth_context = AuthContext()


def skip_restricted_localhost():
    """Skip tests when the localhost exception is restricted (SERVER-12621)."""
    if auth_context.restricted_localhost:
        raise SkipTest("Cannot test with restricted localhost exception "
                       "(SERVER-12621).")


# Make sure warnings are always raised, regardless of
# python version.
def setup():
    warnings.resetwarnings()
    warnings.simplefilter("always")


def teardown():
    client = auth_context.client
    if auth_context.auth_enabled:
        auth_context.add_user_and_log_in()

    client.drop_database("pymongo-pooling-tests")
    client.drop_database("pymongo_test")
    client.drop_database("pymongo_test1")
    client.drop_database("pymongo_test2")
    client.drop_database("pymongo_test_mike")
    client.drop_database("pymongo_test_bernie")

    if auth_context.auth_enabled:
        auth_context.remove_user_and_log_out()
