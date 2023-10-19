# Copyright 2023-present MongoDB, Inc.
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

"""Test MONGODB-OIDC Authentication."""
from __future__ import annotations

import os
import sys
import time
import unittest
from contextlib import contextmanager
from typing import Dict

sys.path[0:0] = [""]

from test.utils import EventListener

from bson import SON
from pymongo import MongoClient
from pymongo.auth import _AUTH_MAP, _authenticate_oidc
from pymongo.cursor import CursorType
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.hello import HelloCompat
from pymongo.operations import InsertOne

# Force MONGODB-OIDC to be enabled.
_AUTH_MAP["MONGODB-OIDC"] = _authenticate_oidc  # type:ignore


class TestAuthOIDC(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri_single = os.environ["MONGODB_URI_SINGLE"]
        cls.uri_multiple = os.environ["MONGODB_URI_MULTI"]
        cls.uri_admin = os.environ["MONGODB_URI"]
        cls.token_dir = os.environ["OIDC_TOKEN_DIR"]

    def setUp(self):
        self.request_called = 0

    def create_request_cb(self, username="test_user1", sleep=0):

        token_file = os.path.join(self.token_dir, username).replace(os.sep, "/")

        def request_token(server_info, context):
            # Validate the info.
            self.assertIn("issuer", server_info)
            self.assertIn("clientId", server_info)

            # Validate the timeout.
            timeout_seconds = context["timeout_seconds"]
            self.assertEqual(timeout_seconds, 60 * 5)
            with open(token_file) as fid:
                token = fid.read()
            resp = {"access_token": token, "refresh_token": token}

            time.sleep(sleep)
            self.request_called += 1
            return resp

        return request_token

    @contextmanager
    def fail_point(self, command_args):
        cmd_on = SON([("configureFailPoint", "failCommand")])
        cmd_on.update(command_args)
        client = MongoClient(self.uri_admin)
        client.admin.command(cmd_on)
        try:
            yield
        finally:
            client.admin.command("configureFailPoint", cmd_on["configureFailPoint"], mode="off")

    def test_connect_request_callback_single_implicit_username(self):
        request_token = self.create_request_cb()
        props: Dict = {"request_token_callback": request_token}
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_single_explicit_username(self):
        request_token = self.create_request_cb()
        props: Dict = {"request_token_callback": request_token}
        client = MongoClient(self.uri_single, username="test_user1", authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_multiple_principal_user1(self):
        request_token = self.create_request_cb()
        props: Dict = {"request_token_callback": request_token}
        client = MongoClient(
            self.uri_multiple, username="test_user1", authmechanismproperties=props
        )
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_multiple_principal_user2(self):
        request_token = self.create_request_cb("test_user2")
        props: Dict = {"request_token_callback": request_token}
        client = MongoClient(
            self.uri_multiple, username="test_user2", authmechanismproperties=props
        )
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_multiple_no_username(self):
        request_token = self.create_request_cb()
        props: Dict = {"request_token_callback": request_token}
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        with self.assertRaises(OperationFailure):
            client.test.test.find_one()
        client.close()

    def test_allowed_hosts_blocked(self):
        request_token = self.create_request_cb()
        props: Dict = {"request_token_callback": request_token, "allowed_hosts": []}
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        with self.assertRaises(ConfigurationError):
            client.test.test.find_one()
        client.close()

        props: Dict = {"request_token_callback": request_token, "allowed_hosts": ["example.com"]}
        client = MongoClient(
            self.uri_single + "&ignored=example.com", authmechanismproperties=props, connect=False
        )
        with self.assertRaises(ConfigurationError):
            client.test.test.find_one()
        client.close()

    def test_valid_request_token_callback(self):
        request_cb = self.create_request_cb()

        props: Dict = {
            "request_token_callback": request_cb,
        }
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_request_callback_returns_null(self):
        def request_token_null(a, b):
            return None

        props: Dict = {"request_token_callback": request_token_null}
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_request_callback_invalid_result(self):
        def request_token_invalid(a, b):
            return {}

        props: Dict = {"request_token_callback": request_token_invalid}
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_cb_extra_value(server_info, context):
            result = self.create_request_cb()(server_info, context)
            result["foo"] = "bar"
            return result

        props: Dict = {"request_token_callback": request_cb_extra_value}
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_speculative_auth_success(self):
        request_token = self.create_request_cb()

        # Create a client with a request callback that returns a valid token.
        props: Dict = {"request_token_callback": request_token}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Set a fail point for saslStart commands.
        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["saslStart"], "errorCode": 18},
            }
        ):
            # Perform a find operation.
            client.test.test.find_one()

        # Close the client.
        client.close()

    def test_reauthenticate_succeeds(self):
        listener = EventListener()

        # Create request callback that returns valid credentials.
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(
            self.uri_single, event_listeners=[listener], authmechanismproperties=props
        )

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        listener.reset()

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            client.test.test.find_one()

        started_events = [
            i.command_name for i in listener.started_events if not i.command_name.startswith("sasl")
        ]
        succeeded_events = [
            i.command_name
            for i in listener.succeeded_events
            if not i.command_name.startswith("sasl")
        ]
        failed_events = [
            i.command_name for i in listener.failed_events if not i.command_name.startswith("sasl")
        ]

        self.assertEqual(
            started_events,
            [
                "find",
                "find",
            ],
        )
        self.assertEqual(succeeded_events, ["find"])
        self.assertEqual(failed_events, ["find"])

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_no_refresh(self):
        cb = self.create_request_cb()

        def request_cb(*args, **kwargs):
            result = cb(*args, **kwargs)
            del result["refresh_token"]
            return result

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            client.test.test.find_one()

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_after_refresh_fails(self):

        # Create request callback that returns valid credentials.
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["find", "saslContinue"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            client.test.test.find_one()

        # Assert that the request callback has been called three times.
        self.assertEqual(self.request_called, 3)

    def test_reauthenticate_fails(self):

        # Create request callback that returns valid credentials.
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation that fails.
            with self.assertRaises(OperationFailure):
                client.test.test.find_one()

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_bulk_write(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["insert"], "errorCode": 391},
            }
        ):
            # Perform a bulk write operation.
            client.test.test.bulk_write([InsertOne({})])

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_bulk_read(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform a find operation.
        client.test.test.find_one()

        # Perform a bulk write operation.
        client.test.test.bulk_write([InsertOne({})])

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a bulk read operation.
            cursor = client.test.test.find_raw_batches({})
            list(cursor)

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_cursor(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform an insert operation.
        client.test.test.insert_one({"a": 1})

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            cursor = client.test.test.find({"a": 1})
            self.assertGreaterEqual(len(list(cursor)), 1)

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_get_more(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform an insert operation.
        client.test.test.insert_many([{"a": 1}, {"a": 1}])

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["getMore"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            cursor = client.test.test.find({"a": 1}, batch_size=1)
            self.assertGreaterEqual(len(list(cursor)), 1)

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_get_more_exhaust(self):
        # Ensure no mongos
        props = {"request_token_callback": self.create_request_cb()}
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        hello = client.admin.command(HelloCompat.LEGACY_CMD)
        if hello.get("msg") != "isdbgrid":
            raise unittest.SkipTest("Must not be a mongos")

        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform an insert operation.
        client.test.test.insert_many([{"a": 1}, {"a": 1}])

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["getMore"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            cursor = client.test.test.find({"a": 1}, batch_size=1, cursor_type=CursorType.EXHAUST)
            self.assertGreaterEqual(len(list(cursor)), 1)

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthenticate_succeeds_command(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}

        print("start of test")
        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform an insert operation.
        client.test.test.insert_one({"a": 1})

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["count"], "errorCode": 391},
            }
        ):
            # Perform a count operation.
            cursor = client.test.command({"count": "test"})

        self.assertGreaterEqual(len(list(cursor)), 1)

        # Assert that the request callback has been called twice.
        self.assertEqual(self.request_called, 2)
        client.close()

    def test_reauthentication_succeeds_multiple_connections(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"request_token_callback": request_cb}

        client1 = MongoClient(self.uri_single, authmechanismproperties=props)
        client2 = MongoClient(self.uri_single, authmechanismproperties=props)

        # Perform an insert operation.
        client1.test.test.insert_many([{"a": 1}, {"a": 1}])
        client2.test.test.find_one()
        self.assertEqual(self.request_called, 2)

        # Use the same authenticator for both clients
        # to simulate a race condition with separate connections.
        # We should only see one extra callback despite both connections
        # needing to reauthenticate.
        client2.options.pool_options._credentials.cache.data = (
            client1.options.pool_options._credentials.cache.data
        )

        client1.test.test.find_one()
        client2.test.test.find_one()

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            client1.test.test.find_one()

        self.assertEqual(self.request_called, 3)

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            client2.test.test.find_one()

        self.assertEqual(self.request_called, 3)
        client1.close()
        client2.close()


if __name__ == "__main__":
    unittest.main()
