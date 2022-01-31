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

from mockupdb import (MockupDB, OpReply, OpMsg, OpMsgReply, OpQuery, absent,
                      Command, go)

from pymongo import MongoClient, version as pymongo_version
from pymongo.errors import OperationFailure, AutoReconnect
from pymongo.server_api import ServerApi, ServerApiVersion
from bson.objectid import ObjectId

import unittest
from copy import deepcopy


def test_hello_with_option(self, protocol, **kwargs):
    hello = "ismaster" if isinstance(protocol(), OpQuery) else "hello"
    # `db.command("hello"|"ismaster")` commands are the same for primaries and
    # secondaries, so we only need one server.
    primary = MockupDB()
    # Set up a custom handler to save the first request from the driver.
    self.handshake_req = None

    def respond(r):
        # Only save the very first request from the driver.
        if self.handshake_req == None:
            self.handshake_req = r
        load_balanced_kwargs = {"serviceId": ObjectId()} if kwargs.get(
            "loadBalanced") else {}
        return r.reply(OpMsgReply(minWireVersion=0, maxWireVersion=13,
                                  **kwargs, **load_balanced_kwargs))

    primary.autoresponds(respond)
    primary.run()
    self.addCleanup(primary.stop)

    # We need a special dict because MongoClient uses "server_api" and all
    # of the commands use "apiVersion".
    k_map = {("apiVersion", "1"): ("server_api", ServerApi(
        ServerApiVersion.V1))}
    client = MongoClient("mongodb://" + primary.address_string,
                         appname='my app',  # For _check_handshake_data()
                         **dict([k_map.get((k, v), (k, v)) for k, v
                                 in kwargs.items()]))

    self.addCleanup(client.close)

    # We have an autoresponder luckily, so no need for `go()`.
    assert client.db.command(hello)

    # We do this checking here rather than in the autoresponder `respond()`
    # because it runs in another Python thread so there are some funky things
    # with error handling within that thread, and we want to be able to use 
    # self.assertRaises().
    self.handshake_req.assert_matches(protocol(hello, **kwargs))
    _check_handshake_data(self.handshake_req)


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
        primary_response = {'hello': 1,
                            'ok': 1,
                            "setName": 'rs', "hosts": hosts,
                            "secondary": True,
                            "minWireVersion": 2, "maxWireVersion": 13}
        error_response = OpMsgReply(
            0, errmsg='Cache Reader No keys found for HMAC ...', code=211)

        secondary_response = {'hello': 1,
                              'ok': 1,
                              "setName": 'rs', "hosts": hosts,
                              "secondary": True,
                              "minWireVersion": 2, "maxWireVersion": 13}

        client = MongoClient(primary.uri,
                             replicaSet='rs',
                             appname='my app',
                             heartbeatFrequencyMS=500)  # Speed up the test.

        self.addCleanup(client.close)

        # New monitoring sockets send data during handshake.
        heartbeat = primary.receives(Command('ismaster'))
        _check_handshake_data(heartbeat)
        heartbeat.ok(OpMsgReply(**primary_response))

        heartbeat = secondary.receives(Command('ismaster'))
        _check_handshake_data(heartbeat)
        heartbeat.ok(OpMsgReply(**secondary_response))

        # Subsequent heartbeats have no client data.
        primary.receives(OpMsg('hello', 1, client=absent)).ok(error_response)
        secondary.receives(OpMsg('hello', 1, client=absent)).ok(
            error_response)

        # The heartbeat retry (on a new connection) does have client data.
        heartbeat = primary.receives(Command('ismaster'))
        _check_handshake_data(heartbeat)
        heartbeat.reply(OpMsgReply(**primary_response))

        heartbeat = secondary.receives(Command('ismaster'))
        _check_handshake_data(heartbeat)
        heartbeat.reply(OpMsgReply(**secondary_response))

        # Still no client data.
        primary.receives(OpMsg('hello', 1, client=absent)).reply(
            OpMsgReply(**primary_response))
        secondary.receives(OpMsg('hello', 1, client=absent)).reply(
            OpMsgReply(**secondary_response))

        # After a disconnect, next ismaster has client data again.
        primary.receives('hello', 1, client=absent).hangup()
        heartbeat = primary.receives('ismaster')
        _check_handshake_data(heartbeat)
        hb_port = deepcopy(heartbeat.client_port)
        heartbeat.reply(OpMsgReply(**primary_response))
        primary.receives(OpMsg('hello')).hangup()
        secondary.receives(OpMsg('hello')).hangup()
        secondary.autoresponds('ismaster', OpMsgReply(**secondary_response))
        secondary.autoresponds('hello', OpMsgReply(**secondary_response))
        secondary.autoresponds('whatever', OpMsgReply(**secondary_response))
        # Start a command, so the client opens an application socket.
        future = go(client.db.command, "whatever")
        message_counter = 0
        for request in primary:
            if request.matches(OpMsg('hello')):
                if request.client_port == hb_port:
                    # This is the monitor again, keep going.
                    request.reply(OpMsgReply(**primary_response))
                else:
                    # Subsequent hellos do not have client data
                    message_counter += 1
                    with self.assertRaises(AssertionError):
                        _check_handshake_data(request)
                    request.reply(OpMsgReply(isWritablePrimary=True,
                                             **primary_response))
            elif request.matches(Command('ismaster')):
                message_counter += 1
                # Handshaking a new application socket.
                _check_handshake_data(request)
                request.reply(OpMsgReply(**primary_response))
            elif request.matches(OpMsg("whatever")):
                message_counter +=1
                # Command succeeds.
                request.assert_matches(OpMsg('whatever'))
                request.reply(OpMsgReply(**primary_response))
                assert future()
                return
            else:
                request.reply(OpMsgReply(**primary_response))
        assert message_counter == 3

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
        test_hello_with_option(self, OpMsg, loadBalanced=True)
        with self.assertRaisesRegex(AssertionError, "does not match"):
            test_hello_with_option(self, Command, loadBalanced=True)

    def test_handshake_versioned_api(self):
        test_hello_with_option(self, OpMsg, apiVersion="1")
        with self.assertRaisesRegex(AssertionError, "does not match"):
            test_hello_with_option(self, Command, apiVersion="1")

    def test_handshake_not_either(self):
        # If we don't specify either option then it should be using
        # OP_QUERY for the initial step of the handshake.
        test_hello_with_option(self, Command)
        with self.assertRaisesRegex(AssertionError, "does not match"):
            test_hello_with_option(self, OpMsg)

    def test_handshake_max_wire(self):
        server = MockupDB()
        primary_response = {"hello": 1, "ok": 1,
                            "minWireVersion": 0, "maxWireVersion": 6}
        self.found_auth_msg = False

        def responder(request):
            if request.matches(OpMsg, saslStart=1):
                self.found_auth_msg = True
                # Immediately closes the connection with
                request.reply(OpMsgReply(**primary_response,
                                         **{'payload':
                                                b'r=wPleNM8S5p8gMaffMDF7Py4ru9bnmmoqb0'
                                                b'1WNPsil6o=pAvr6B1garhlwc6MKNQ93ZfFky'
                                                b'tXdF9r,'
                                                b's=4dcxugMJq2P4hQaDbGXZR8uR3ei'
                                                b'PHrSmh4uhkg==,i=15000',
                                            "saslSupportedMechs": [
                                                "SCRAM-SHA-1"]}))
            else:
                return request.reply(OpMsgReply(**primary_response))

        server.autoresponds(responder)
        self.addCleanup(server.stop)
        server.run()
        client = MongoClient(server.uri,
                             username='username',
                             password='password',
                             appname='my app',
                             )
        self.addCleanup(client.close)
        self.assertRaises(OperationFailure, client.db.collection.find_one,
                          {"a": 1})
        assert self.found_auth_msg, """Could not find authentication command 
                                       with correct protocol"""


if __name__ == '__main__':
    unittest.main()
