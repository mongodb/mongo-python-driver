# Copyright 2020-present MongoDB, Inc.
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

import os
import sys
import unittest
from contextlib import contextmanager
from typing import Dict

sys.path[0:0] = [""]

from test.utils import EventListener

from bson import SON
from pymongo import MongoClient
from pymongo.auth_oidc import OperationFailure, _internal

_oidc_cache = _internal.cache


class TestAuthOIDC(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri_single = os.environ["MONGODB_URI_SINGLE"]
        cls.uri_multiple = os.environ["MONGODB_URI_MULTIPLE"]
        cls.uri_admin = os.environ["MONGODB_URI"]
        cls.token_dir = os.environ["OIDC_TOKEN_DIR"]

    def setUp(self):
        self.request_called = 0
        self.refresh_called = 0
        _oidc_cache.clear()
        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user1")

    def create_request_cb(self, username="test_user1", expires_in_seconds=None):

        token_file = os.path.join(self.token_dir, username)

        def request_token(principal, info, timeout):
            # Validate the principal.
            if principal is not None:
                self.assertIsInstance(principal, str)

            # Validate the info.
            if "device_authorization_endpoint" not in info:
                self.assertIn("authorization_endpoint", info)
            self.assertIn("token_endpoint", info)
            self.assertIn("client_id", info)
            for key in info:
                self.assertIn(
                    key,
                    [
                        "authorization_endpoint",
                        "token_endpoint",
                        "client_id",
                        "device_authorization_endpoint",
                    ],
                )

            # Validate the timeout.
            self.assertEqual(timeout, 60 * 5)
            with open(token_file) as fid:
                token = fid.read()
            resp = dict(access_token=token)
            if expires_in_seconds is not None:
                resp["expires_in_seconds"] = expires_in_seconds
            self.request_called += 1
            return resp

        return request_token

    def create_refresh_cb(self, username="test_user1", expires_in_seconds=None):

        token_file = os.path.join(self.token_dir, username)

        def refresh_token(principal, info, creds, timeout):
            with open(token_file) as fid:
                token = fid.read()

            # Validate the principal.
            if principal is not None:
                self.assertIsInstance(principal, str)

            # Validate the info.
            if "device_authorization_endpoint" not in info:
                self.assertIn("authorization_endpoint", info)
            self.assertIn("token_endpoint", info)
            self.assertIn("client_id", info)
            for key in info:
                self.assertIn(
                    key,
                    [
                        "authorization_endpoint",
                        "token_endpoint",
                        "client_id",
                        "device_authorization_endpoint",
                    ],
                )

            # Validate the creds
            self.assertIn("access_token", creds)
            for key in creds:
                self.assertIn(key, ["access_token", "expires_in_seconds", "refresh_token"])
            # Validate the timeout.
            self.assertEqual(timeout, 60 * 5)

            resp = dict(access_token=token)
            if expires_in_seconds is not None:
                resp["expires_in_seconds"] = expires_in_seconds
            self.refresh_called += 1
            return resp

        return refresh_token

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

    def test_connect_callbacks_single_implicit_username(self):
        request_token = self.create_request_cb()
        props: Dict = dict(on_oidc_request_token=request_token)
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_callbacks_single_explicit_username(self):
        request_token = self.create_request_cb()
        props: Dict = dict(on_oidc_request_token=request_token)
        client = MongoClient(self.uri_single, username="test_user1", authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_callbacks_multiple_principal_user1(self):
        request_token = self.create_request_cb()
        props: Dict = dict(on_oidc_request_token=request_token)
        client = MongoClient(
            self.uri_multiple, username="test_user1", authmechanismproperties=props
        )
        client.test.test.find_one()
        client.close()

    def test_connect_callbacks_multiple_principal_user2(self):
        request_token = self.create_request_cb("test_user2")
        props: Dict = dict(on_oidc_request_token=request_token)
        client = MongoClient(
            self.uri_multiple, username="test_user2", authmechanismproperties=props
        )
        client.test.test.find_one()
        client.close()

    def test_connect_callbacks_multiple_no_username(self):
        request_token = self.create_request_cb()
        props: Dict = dict(on_oidc_request_token=request_token)
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        with self.assertRaises(OperationFailure):
            client.test.test.find_one()
        client.close()

    def test_connect_aws_single_principal(self):
        props = dict(PROVIDER_NAME="aws")
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_aws_multiple_principal_user1(self):
        props = dict(PROVIDER_NAME="aws")
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_aws_multiple_principal_user2(self):
        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user2")
        props = dict(PROVIDER_NAME="aws")
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_valid_callbacks(self):
        request_cb = self.create_request_cb(expires_in_seconds=60)
        refresh_cb = self.create_refresh_cb()

        props: Dict = dict(on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb)
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_request_callback_returns_null(self):
        def request_token_null(principal, info, timeout):
            return None

        props: Dict = dict(on_oidc_request_token=request_token_null)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_refresh_callback_returns_null(self):
        request_cb = self.create_request_cb(expires_in_seconds=60)

        def refresh_token_null(principal, info, creds, timeout):
            return None

        props: Dict = dict(
            on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_token_null
        )
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_request_callback_invalid_result(self):
        def request_token_invalid(principal, info, timeout):
            return dict()

        props: Dict = dict(on_oidc_request_token=request_token_invalid)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_token_invalid2(principal, info, timeout):
            return dict(access_token="foo", other="bar")

        props: Dict = dict(on_oidc_request_token=request_token_invalid2)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_refresh_callback_invalid_result(self):
        request_cb = self.create_request_cb(expires_in_seconds=60)

        def refresh_cb_no_token(principal, info, cred, timeout):
            return dict()

        props: Dict = dict(
            on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb_no_token
        )
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def refresh_cb_extra_value(principal, info, cred, timeout):
            return dict(access_token="foo", other="bar")

        props: Dict = dict(
            on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb_extra_value
        )
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_cache_with_refresh(self):
        # Create a new client with a request callback and a refresh callback.  Both callbacks will read the contents of the ``AWS_WEB_IDENTITY_TOKEN_FILE`` location to obtain a valid access token.

        # Give a callback response with a valid accessToken and an expiresInSeconds that is within one minute.
        request_cb = self.create_request_cb(expires_in_seconds=60)
        refresh_cb = self.create_refresh_cb()

        props: Dict = dict(on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb)

        # Ensure that a ``find`` operation adds credentials to the cache.
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        assert len(_oidc_cache) == 1

        # Create a new client with the same request callback and a refresh callback.
        # Ensure that a ``find`` operation results in a call to the refresh callback.
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        assert self.refresh_called == 1
        assert len(_oidc_cache) == 1

    def test_cache_with_no_refresh(self):
        # Create a new client with a request callback callback.
        # Give a callback response with a valid accessToken and an expiresInSeconds that is within one minute.
        request_cb = self.create_request_cb()

        props = dict(on_oidc_request_token=request_cb)
        client = MongoClient(self.uri_single, authMechanismProperties=props)

        # Ensure that a ``find`` operation adds credentials to the cache.
        request_called = 0
        client.test.test.find_one()
        client.close()
        assert self.request_called == 1
        assert len(_oidc_cache) == 1

        # Create a new client with the same request callback.
        # Ensure that a ``find`` operation results in a call to the request callback.
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()
        assert self.request_called == 2
        assert len(_oidc_cache) == 1

    def test_cache_key_includes_callback(self):
        request_cb = self.create_request_cb()

        props: Dict = dict(on_oidc_request_token=request_cb)

        # Ensure that a ``find`` operation adds a new entry to the cache.
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        # Create a new client with a different request callback.
        def request_token_2(principal, info, timeout):
            return request_cb(principal, info, timeout)

        props["on_oidc_request_token"] = request_token_2
        client = MongoClient(self.uri_single, authMechanismProperties=props)

        # Ensure that a ``find`` operation adds a new entry to the cache.
        client.test.test.find_one()
        client.close()
        assert len(_oidc_cache) == 2

    def test_cache_clears_on_error(self):
        request_cb = self.create_request_cb()

        # Create a new client with a valid request callback that gives credentials that expire within 5 minutes and a refresh callback that gives invalid credentials.
        def refresh_cb(principal, info, creds, timeout):
            return dict(access_token="bad")

        # Add a token to the cache that will expire soon.
        props: Dict = dict(on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        # Create a new client with the same callbacks.
        client = MongoClient(self.uri_single, authMechanismProperties=props)

        # Ensure that another ``find`` operation results in an error.
        with self.assertRaises(OperationFailure):
            client.test.test.find_one()

        client.close()

        # Ensure that the cache has been cleared.
        assert len(_oidc_cache) == 0

    def test_cache_is_not_used_in_aws_automatic_workflow(self):
        # Create a new client using the AWS device workflow.
        # Ensure that a ``find`` operation does not add credentials to the cache.
        props = dict(PROVIDER_NAME="aws")
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()
        assert len(_oidc_cache) == 0

    def test_speculative_auth_success(self):
        # Clear the cache
        _oidc_cache.clear()
        token_file = os.path.join(self.token_dir, "test_user1")

        def request_token(principal, info, timeout):
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token, expires_in_seconds=1000)

        # Create a client with a request callback that returns a valid token
        # that will not expire soon.
        props: Dict = dict(on_oidc_request_token=request_token)
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

        # Create a new client.
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

        # Create request and refresh callbacks that return valid credentials
        # that will not expire soon.
        request_cb = self.create_request_cb()
        refresh_cb = self.create_refresh_cb()

        # Create a client with the callbacks.
        props: Dict = dict(on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb)
        client = MongoClient(
            self.uri_single, event_listeners=[listener], authmechanismproperties=props
        )

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the refresh callback has not been called.
        self.assertEqual(self.refresh_called, 0)

        listener.reset()

        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["find", "saslStart"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            client.test.test.find_one()

        started_events = [i.command_name for i in listener.started_events]
        succeeded_events = [i.command_name for i in listener.succeeded_events]
        failed_events = [i.command_name for i in listener.failed_events]

        assert started_events == [
            "find",
            "saslStart",
            "saslStart",
            "saslContinue",
            "find",
        ], started_events
        assert succeeded_events == ["saslStart", "saslContinue", "find"], succeeded_events
        assert failed_events == ["find", "saslStart"], failed_events

        # Assert that the refresh callback has been called.
        self.assertEqual(self.refresh_called, 1)
        client.close()

    def test_reauthenticate_retries_and_succees_with_cache(self):
        listener = EventListener()

        # Create request and refresh callbacks that return valid credentials
        # that will not expire soon.
        request_cb = self.create_request_cb()
        refresh_cb = self.create_refresh_cb()

        # Create a client with the callbacks.
        props: Dict = dict(on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb)
        client = MongoClient(
            self.uri_single, event_listeners=[listener], authmechanismproperties=props
        )

        # Perform a find operation.
        client.test.test.find_one()

        # Set a fail point for ``saslStart`` commands of the form
        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["find", "saslStart"], "errorCode": 391},
            }
        ):
            # Perform a find operation that succeeds.
            client.test.test.find_one()

        # Close the client.
        client.close()

    def test_reauthenticate_fails_with_no_cache(self):
        listener = EventListener()

        # Create request and refresh callbacks that return valid credentials
        # that will not expire soon.
        request_cb = self.create_request_cb()
        refresh_cb = self.create_refresh_cb()

        # Create a client with the callbacks.
        props: Dict = dict(on_oidc_request_token=request_cb, on_oidc_refresh_token=refresh_cb)
        client = MongoClient(
            self.uri_single, event_listeners=[listener], authmechanismproperties=props
        )

        # Perform a find operation.
        client.test.test.find_one()

        # Clear the cache.
        _oidc_cache.clear()

        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["find", "saslStart"], "errorCode": 391},
            }
        ):
            # Perform a find operation that fails.
            with self.assertRaises(OperationFailure):
                client.test.test.find_one()

        client.close()


if __name__ == "__main__":
    unittest.main()
