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
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Dict

sys.path[0:0] = [""]

from test.unified_format import generate_test_classes
from test.utils import EventListener

from bson import SON
from pymongo import MongoClient
from pymongo.auth_oidc import (
    OIDCCallback,
    OIDCCallbackResult,
)
from pymongo.azure_helpers import _get_azure_response
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect, ConfigurationError, OperationFailure
from pymongo.hello import HelloCompat
from pymongo.operations import InsertOne
from pymongo.uri_parser import parse_uri

ROOT = Path(__file__).parent.parent.resolve()
TEST_PATH = ROOT / "auth" / "unified"
PROVIDER_NAME = os.environ.get("OIDC_PROVIDER_NAME", "aws")


# Generate unified tests.
globals().update(generate_test_classes(str(TEST_PATH), module=__name__))


class OIDCTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.uri_single = os.environ["MONGODB_URI_SINGLE"]
        cls.uri_multiple = os.environ["MONGODB_URI_MULTI"]
        cls.uri_admin = os.environ["MONGODB_URI"]

    def setUp(self):
        self.request_called = 0

    def get_token(self, username=None):
        """Get a token for the current provider."""
        if PROVIDER_NAME == "aws":
            token_dir = os.environ["OIDC_TOKEN_DIR"]
            token_file = os.path.join(token_dir, username).replace(os.sep, "/")
            with open(token_file) as fid:
                return fid.read()
        elif PROVIDER_NAME == "azure":
            opts = parse_uri(self.uri_single)["options"]
            token_aud = opts["authmechanismproperties"]["TOKEN_AUDIENCE"]
            return _get_azure_response(token_aud, username)["access_token"]

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


class TestAuthOIDCHuman(OIDCTestBase):
    uri: str

    @classmethod
    def setUpClass(cls):
        if PROVIDER_NAME != "aws":
            raise unittest.SkipTest("Human workflows are only tested with the aws provider")
        super().setUpClass()

    def create_request_cb(self, username="test_user1", sleep=0):
        def request_token(context):
            # Validate the info.
            self.assertIsInstance(context.idp_info.issuer, str)
            self.assertIsInstance(context.idp_info.clientId, str)

            # Validate the timeout.
            timeout_seconds = context.timeout_seconds
            self.assertEqual(timeout_seconds, 60 * 5)
            token = self.get_token(username)
            resp = OIDCCallbackResult(access_token=token, refresh_token=token)

            time.sleep(sleep)
            self.request_called += 1
            return resp

        class Inner(OIDCCallback):
            def fetch(self, context):
                return request_token(context)

        return Inner()

    def create_client(self, username="test_user1"):
        request_cb = self.create_request_cb(username)
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}
        return MongoClient(self.uri_multiple, username=username, authmechanismproperties=props)

    def test_connect_request_callback_single_implicit_username(self):
        request_token = self.create_request_cb()
        props: Dict = {"callback": request_token, "CALLBACK_TYPE": "human"}
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_single_explicit_username(self):
        request_token = self.create_request_cb()
        props: Dict = {"callback": request_token, "CALLBACK_TYPE": "human"}
        client = MongoClient(self.uri_single, username="test_user1", authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_multiple_principal_user1(self):
        client = self.create_client()
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_multiple_principal_user2(self):
        client = self.create_client("test_user2")
        client.test.test.find_one()
        client.close()

    def test_connect_request_callback_multiple_no_username(self):
        request_token = self.create_request_cb()
        props: Dict = {"callback": request_token, "CALLBACK_TYPE": "human"}
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        with self.assertRaises(OperationFailure):
            client.test.test.find_one()
        client.close()

    def test_allowed_hosts_blocked(self):
        request_token = self.create_request_cb()
        props: Dict = {"callback": request_token, "allowed_hosts": [], "CALLBACK_TYPE": "human"}
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        with self.assertRaises(ConfigurationError):
            client.test.test.find_one()
        client.close()

        props: Dict = {
            "callback": request_token,
            "allowed_hosts": ["example.com"],
            "CALLBACK_TYPE": "human",
        }
        with warnings.catch_warnings():
            warnings.simplefilter("default")
            client = MongoClient(
                self.uri_single + "&ignored=example.com",
                authmechanismproperties=props,
                connect=False,
            )
        with self.assertRaises(ConfigurationError):
            client.test.test.find_one()
        client.close()

    def test_configuration_errors(self):
        request_token = self.create_request_cb()

        class CustomCB(OIDCCallback):
            def fetch(self, ctx):
                return None

        props: Dict = {"callback": request_token, "CALLBACK_TYPE": "human"}

        # Assert that providing a callback and a provider raises an error.
        props["PROVIDER_NAME"] = PROVIDER_NAME
        with self.assertRaises(ConfigurationError):
            _ = MongoClient(self.uri_single, authmechanismproperties=props)
        props["callback"] = CustomCB()

    def test_valid_callback(self):
        client = self.create_client()
        client.test.test.find_one()
        client.close()

        client = self.create_client()
        client.test.test.find_one()
        client.close()

    def test_request_callback_returns_null(self):
        class RequestTokenNull(OIDCCallback):
            def fetch(self, a):
                return None

        props: Dict = {"callback": RequestTokenNull(), "CALLBACK_TYPE": "human"}
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_request_callback_invalid_result(self):
        class CallbackInvalidToken(OIDCCallback):
            def fetch(self, a):
                return {}

        props: Dict = {"callback": CallbackInvalidToken(), "CALLBACK_TYPE": "human"}
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_speculative_auth_success(self):
        # Create a client with a request callback that returns a valid token.
        client = self.create_client()

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
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}
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

        class CustomRequest(OIDCCallback):
            def fetch(self, *args, **kwargs):
                result = cb.fetch(*args, **kwargs)
                result.refresh_token = None
                return result

        # Create a client with the callback.
        props: Dict = {"callback": CustomRequest(), "CALLBACK_TYPE": "human"}
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
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}
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

    def test_reauthentication_succeeds_multiple_connections(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}

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

    # PyMongo specific tests, since we have multiple code paths for reauth handling.

    def test_reauthenticate_succeeds_bulk_write(self):
        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}
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
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}
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
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}
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
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}
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
        props = {"callback": self.create_request_cb(), "CALLBACK_TYPE": "human"}
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        hello = client.admin.command(HelloCompat.LEGACY_CMD)
        if hello.get("msg") != "isdbgrid":
            raise unittest.SkipTest("Must not be a mongos")

        request_cb = self.create_request_cb()

        # Create a client with the callback.
        props: Dict = {"callback": request_cb}
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
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "human"}

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


class TestAuthOIDCMachine(OIDCTestBase):
    uri: str

    def setUp(self):
        self.request_called = 0
        if PROVIDER_NAME == "aws":
            self.default_username = "test_user1"
        else:
            self.default_username = None

    def create_request_cb(self, username=None, sleep=0):
        if username is None:
            username = self.default_username

        def request_token(_context):
            token = self.get_token(username)
            time.sleep(sleep)
            self.request_called += 1
            return OIDCCallbackResult(access_token=token)

        class Inner(OIDCCallback):
            def fetch(self, context):
                return request_token(context)

        return Inner()

    def create_client(self, **kwargs):
        request_cb = self.create_request_cb()
        props: Dict = {"callback": request_cb, "CALLBACK_TYPE": "machine"}
        return MongoClient(self.uri_single, authmechanismproperties=props, **kwargs)

    def test_01_custom_callback(self):
        # Create a ``MongoClient`` configured with a custom OIDC callback that
        # implements the provider logic.
        client = self.create_client()
        # Perform a ``find`` operation that succeeds.
        client.test.test.find_one()
        # Close the client.
        client.close()

    def test_02_callback_is_called_during_reauthentication(self):
        # Create a ``MongoClient`` configured with a custom OIDC callback that
        # implements the provider logic.
        client = self.create_client()

        # Set a fail point for the find command.s
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a ``find`` operation that succeeds.
            client.test.test.find_one()

        # Verify that the callback was called 2 times (once during the connection
        # handshake, and again during reauthentication).
        self.assertEqual(self.request_called, 2)

        # Close the client.
        client.close()

    def test_03_authentication_failures_with_cached_tokens_fetch_a_new_token_and_retry(self):
        # create a ``MongoClient`` configured with ``retryReads=false`` and a custom
        # OIDC callback that implements the provider logic.
        client = self.create_client(retryReads=False)

        # Poison the cache with an invalid access token.

        # Set a fail point for ``find`` command.
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391, "closeConnection": True},
            }
        ):
            # Perform a ``find`` operation that fails. This is to force the ``MongoClient``
            # to cache an access token.
            with self.assertRaises(AutoReconnect):
                client.test.test.find_one()

        # Poison the cache of the client.
        client.options.pool_options._credentials.cache.data.access_token = "bad"

        # Reset the request count.
        self.request_called = 0

        # Verify that a find succeeds.
        client.test.test.find_one()

        # Verify that the callback was called 1 time.
        self.assertEqual(self.request_called, 1)

        # Close the client.
        client.close()

    def test_04_authentication_failures_without_cached_tokens_return_an_error(self):
        # Create a ``MongoClient`` configured with ``retryReads=false`` and a custom
        # OIDC callback that always returns invalid access tokens.

        class CustomCallback(OIDCCallback):
            count = 0

            def fetch(self, a):
                self.count += 1
                return OIDCCallbackResult(access_token="bad value")

        callback = CustomCallback()
        props: Dict = {"callback": callback, "CALLBACK_TYPE": "machine"}
        client = MongoClient(self.uri_single, authMechanismProperties=props, retryReads=False)

        # Perform a ``find`` operation that fails.
        with self.assertRaises(OperationFailure):
            client.test.test.find_one()

        # Verify that the callback was called 1 time.
        self.assertEqual(callback.count, 1)

        # Close the client.
        client.close()

    def test_callback_is_called_once_on_handshake_authentication_failure(self):
        client = self.create_client()

        # Set a fail point for ``saslStart`` commands.
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["saslStart"], "errorCode": 18},
            }
        ):
            # Perform a find operation.
            client.test.test.find_one()

        # Assert that the request callback has been called once.
        self.assertEqual(self.request_called, 1)
        client.close()

    def test_invalid_prop_combination(self):
        request_cb = self.create_request_cb()
        props: Dict = {"callback": request_cb, "PROVIDER_NAME": PROVIDER_NAME}
        with self.assertRaises(ConfigurationError):
            _ = MongoClient(self.uri_single, authmechanismproperties=props)

    def test_request_callback_returns_null(self):
        class CallbackNullToken(OIDCCallback):
            def fetch(self, a):
                return None

        props: Dict = {"callback": CallbackNullToken(), "CALLBACK_TYPE": "machine"}
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_request_callback_invalid_result(self):
        class CallbackTokenInvalid(OIDCCallback):
            def fetch(self, a):
                return {}

        props: Dict = {"callback": CallbackTokenInvalid(), "CALLBACK_TYPE": "machine"}
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_speculative_auth_success(self):
        client1 = self.create_client()
        client1.test.test.find_one()
        client2 = self.create_client()

        # Prime the cache of the second client.
        client2.options.pool_options._credentials.cache.data = (
            client1.options.pool_options._credentials.cache.data
        )

        # Set a fail point for saslStart commands.
        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["saslStart"], "errorCode": 18},
            }
        ):
            # Perform a find operation.
            client2.test.test.find_one()

        # Close the clients.
        client2.close()
        client1.close()

    def test_reauthentication_succeeds_multiple_connections(self):
        client1 = self.create_client()
        client2 = self.create_client()

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

    def test_azure_no_username(self):
        if PROVIDER_NAME != "azure":
            raise unittest.SkipTest("Test is only supported on Azure")
        opts = parse_uri(self.uri_single)["options"]
        token_aud = opts["authmechanismproperties"]["TOKEN_AUDIENCE"]

        props = dict(TOKEN_AUDIENCE=token_aud, PROVIDER_NAME="azure")
        client = MongoClient(
            self.uri_admin, authMechanism="MONGODB-OIDC", authMechanismProperties=props
        )
        client.test.test.find_one()
        client.close()

    def test_azure_bad_username(self):
        if PROVIDER_NAME != "azure":
            raise unittest.SkipTest("Test is only supported on Azure")

        opts = parse_uri(self.uri_single)["options"]
        token_aud = opts["authmechanismproperties"]["TOKEN_AUDIENCE"]

        props = dict(TOKEN_AUDIENCE=token_aud, PROVIDER_NAME="azure")
        client = MongoClient(
            self.uri_admin,
            username="bad",
            authMechanism="MONGODB-OIDC",
            authMechanismProperties=props,
        )
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()


if __name__ == "__main__":
    unittest.main()
