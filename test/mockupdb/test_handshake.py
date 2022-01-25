# Copyright 2016 MongoDB, Inc.
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


from mockupdb import MockupDB, OpReply, OpMsg, OpMsgReply, absent, Command, go
from pymongo import MongoClient, version as pymongo_version
from pymongo.errors import OperationFailure
from pymongo.server_api import ServerApi

import unittest


def test_handshake_with_option(self, compare_protocol, num_servers, **kwargs):
    primary = MockupDB(verbose=False, min_wire_version=6,
                            max_wire_version=20)
    secondaries = [MockupDB(verbose=False, min_wire_version=6,
                            max_wire_version=20) for _ in range(num_servers-1)]
    servers = [primary]+secondaries
    for server in servers:
        server.run()
        self.addCleanup(server.stop)
    hosts = [server.address_string for server in servers]
    primary_response = OpMsgReply("hello",
                                     hosts=hosts,
                                     secondary=False,
                                     minWireVersion=6, maxWireVersion=20,
                                     serviceId=int(kwargs.get(
                                         "loadBalanced", "0")))
    secondary_response = OpMsgReply('hello', False,
                                 hosts=hosts,
                                 secondary=True,
                                 minWireVersion=6, maxWireVersion=20,)
    address_str = "mongodb://"+','.join(hosts) if not kwargs.get("loadBalanced") else primary.address_string
    client = MongoClient(address_str,
                         appname='my app',
                         heartbeatFrequencyMS=500,
                         **kwargs)  # Speed up the test.

    self.addCleanup(client.close)
    future = go(client.db.command, 'whatever')
    # the primary always receives a handshake
    i = primary.receives(OpMsg("hello"))
    _check_handshake_data(i)
    i.ok(primary_response)
    # check each secondary to see if it received messages, if it did then
    # check them to make sure they're OP_MSG "hello"s
    for server in secondaries:
        if server.requests_count == 0:
            continue
        while not server._request_q.empty():
            i = server._request_q.get_nowait()
            i.assert_matches(OpMsg("hello"))
            i.ok(secondary_response)

def _check_handshake_data(request):

    assert 'client' in request
    data = request['client']

    assert data['application'] == {'name': 'my app'}
    assert data['driver'] == {'name': 'PyMongo', 'version': pymongo_version}

    # Keep it simple, just check these fields exist.
    assert 'os' in data
    assert 'platform' in data


class TestHandshake(unittest.TestCase):
    def test_client_handshake_data(self):
        primary, secondary = MockupDB(), MockupDB()
        for server in primary, secondary:
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in (primary, secondary)]
        primary_response = OpReply('ismaster', True,
                                   setName='rs', hosts=hosts,
                                   minWireVersion=2, maxWireVersion=6)
        error_response = OpReply(
            0, errmsg='Cache Reader No keys found for HMAC ...', code=211)

        secondary_response = OpReply('ismaster', False,
                                     setName='rs', hosts=hosts,
                                     secondary=True,
                                     minWireVersion=2, maxWireVersion=6)

        client = MongoClient(primary.uri,
                             replicaSet='rs',
                             appname='my app',
                             heartbeatFrequencyMS=500)  # Speed up the test.

        self.addCleanup(client.close)

        # New monitoring sockets send data during handshake.
        heartbeat = primary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(primary_response)

        heartbeat = secondary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(secondary_response)

        # Subsequent heartbeats have no client data.
        primary.receives('ismaster', 1, client=absent).ok(error_response)
        secondary.receives('ismaster', 1, client=absent).ok(error_response)

        # The heartbeat retry (on a new connection) does have client data.
        heartbeat = primary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(primary_response)

        heartbeat = secondary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(secondary_response)

        # Still no client data.
        primary.receives('ismaster', 1, client=absent).ok(primary_response)
        secondary.receives('ismaster', 1, client=absent).ok(secondary_response)

        # After a disconnect, next ismaster has client data again.
        primary.receives('ismaster', 1, client=absent).hangup()
        heartbeat = primary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(primary_response)

        secondary.autoresponds('ismaster', secondary_response)

        # Start a command, so the client opens an application socket.
        future = go(client.db.command, 'whatever')

        for request in primary:
            if request.matches(Command('ismaster')):
                if request.client_port == heartbeat.client_port:
                    # This is the monitor again, keep going.
                    request.ok(primary_response)
                else:
                    # Handshaking a new application socket.
                    _check_handshake_data(request)
                    request.ok(primary_response)
            else:
                # Command succeeds.
                request.assert_matches(OpMsg('whatever'))
                request.ok()
                assert future()
                return

    def test_client_handshake_saslSupportedMechs(self):
        server = MockupDB()
        server.run()
        self.addCleanup(server.stop)

        primary_response = OpReply('ismaster', True,
                                   minWireVersion=2, maxWireVersion=6)
        client = MongoClient(server.uri,
                             username='username',
                             password='password')

        self.addCleanup(client.close)

        # New monitoring sockets send data during handshake.
        heartbeat = server.receives('ismaster')
        heartbeat.ok(primary_response)

        future = go(client.db.command, 'whatever')
        for request in server:
            if request.matches('ismaster'):
                if request.client_port == heartbeat.client_port:
                    # This is the monitor again, keep going.
                    request.ok(primary_response)
                else:
                    # Handshaking a new application socket should send
                    # saslSupportedMechs and speculativeAuthenticate.
                    self.assertEqual(request['saslSupportedMechs'],
                                     'admin.username')
                    self.assertIn(
                        'saslStart', request['speculativeAuthenticate'])
                    auth = {'conversationId': 1, 'done': False,
                            'payload': b'r=wPleNM8S5p8gMaffMDF7Py4ru9bnmmoqb0'
                                       b'1WNPsil6o=pAvr6B1garhlwc6MKNQ93ZfFky'
                                       b'tXdF9r,s=4dcxugMJq2P4hQaDbGXZR8uR3ei'
                                       b'PHrSmh4uhkg==,i=15000'}
                    request.ok('ismaster', True,
                               saslSupportedMechs=['SCRAM-SHA-256'],
                               speculativeAuthenticate=auth,
                               minWireVersion=2, maxWireVersion=6)
                    # Authentication should immediately fail with:
                    # OperationFailure: Server returned an invalid nonce.
                    with self.assertRaises(OperationFailure):
                        future()
                    return

    def test_handshake_load_balanced(self):
        test_handshake_with_option(self, OpMsg, 10, loadBalanced=True)

    def test_handshake_versioned_api(self):
        test_handshake_with_option(self, OpMsg, 10, server_api=ServerApi("1"))


if __name__ == '__main__':
    unittest.main()
