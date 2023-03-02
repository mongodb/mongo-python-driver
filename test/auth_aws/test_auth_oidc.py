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
from pymongo.auth import OperationFailure, _oidc_cache


class TestAuthOIDC(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri_single = os.environ["MONGODB_URI_SINGLE"]
        cls.uri_multiple = os.environ["MONGODB_URI_MULTIPLE"]
        cls.uri_admin = os.environ["MONGODB_URI"]
        cls.token_dir = os.environ["OIDC_TOKEN_DIR"]

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

    def test_connect_aws(self):
        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user1")
        props = dict(DEVICE_NAME="aws")
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user2")
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_callbacks(self):
        token_file = os.path.join(self.token_dir, "test_user1")

        def request_token(principal, info, timeout):
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token)

        props: Dict = dict(on_oidc_request_token=request_token)
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        _oidc_cache.clear()
        client = MongoClient(
            self.uri_multiple, username="test_user1", authmechanismproperties=props
        )
        client.test.test.find_one()
        client.close()

        _oidc_cache.clear()
        token_file = os.path.join(self.token_dir, "test_user2")
        client = MongoClient(
            self.uri_multiple, username="test_user2", authmechanismproperties=props
        )
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        with self.assertRaises(OperationFailure):
            client.test.test.find_one()
        client.close()

    def test_bad_callbacks(self):
        _oidc_cache.clear()

        def request_token_null(principal, info, timeout):
            return None

        props: Dict = dict(on_oidc_request_token=request_token_null)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_token_no_token(principal, info, timeout):
            return dict()

        _oidc_cache.clear()
        props: Dict = dict(on_oidc_request_token=request_token_no_token)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_refresh_null(principal, info, creds, timeout):
            return None

        token_file = os.path.join(self.token_dir, "test_user1")

        def request_token(principal, info, timeout):
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token)

        _oidc_cache.clear()
        props: Dict = dict(
            on_oidc_request_token=request_token, on_oidc_refresh_token=request_refresh_null
        )
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_refresh_no_token(principal, info, creds, timeout):
            return dict()

        _oidc_cache.clear()
        props["on_oidc_refresh_token"] = request_refresh_no_token
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

    def test_caching(self):
        request_called = 0
        refresh_called = 0

        # Clear the cache.
        _oidc_cache.clear()
        # Create a new client with a request callback and a refresh callback.  Both callbacks will read the contents of the ``AWS_WEB_IDENTITY_TOKEN_FILE`` location to obtain a valid access token.
        # Give a callback response with a valid accessToken and an expiresInSeconds that is within one minute.
        token_file = os.path.join(self.token_dir, "test_user1")

        def request_token(principal, info, timeout):
            nonlocal request_called
            assert "authorization_endpoint" in info
            assert "token_endpoint" in info
            assert "client_id" in info
            assert timeout == 60 * 5
            with open(token_file) as fid:
                token = fid.read()
            request_called += 1
            return dict(access_token=token, expires_in_seconds=60)

        def refresh_token(principal, info, creds, timeout):
            nonlocal refresh_called
            assert "authorization_endpoint" in info
            assert "token_endpoint" in info
            assert "client_id" in info
            assert timeout == 60 * 5
            assert "access_token" in creds
            refresh_called += 1
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token, expires_in_seconds=60)

        _oidc_cache.clear()
        props: Dict = dict(on_oidc_request_token=request_token, on_oidc_refresh_token=refresh_token)

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

        assert refresh_called == 1
        assert len(_oidc_cache) == 1

        # Clear the cache.
        _oidc_cache.clear()

        # Create a new client with a request callback callback.
        # Give a callback response with a valid accessToken and an expiresInSeconds that is within one minute.
        del props["on_oidc_refresh_token"]
        client = MongoClient(self.uri_single, authMechanismProperties=props)

        # Ensure that a ``find`` operation adds credentials to the cache.
        request_called = 0
        client.test.test.find_one()
        client.close()
        assert request_called == 1
        assert len(_oidc_cache) == 1

        # Create a new client with the same request callback.
        # Ensure that a ``find`` operation results in a call to the request callback.
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()
        assert request_called == 2
        assert len(_oidc_cache) == 1

        # Create a new client with a different request callback.
        def request_token_2(principal, info, timeout):
            return request_token(principal, info, timeout)

        props["on_oidc_request_token"] = request_token_2
        client = MongoClient(self.uri_single, authMechanismProperties=props)

        # Ensure that a ``find`` operation adds a new entry to the cache.
        client.test.test.find_one()
        client.close()
        assert request_called == 3
        assert len(_oidc_cache) == 2

        # Clear the cache
        _oidc_cache.clear()

        # Create a new client with a refresh callback that gives invalid credentials.
        def bad_refresh(principal, info, creds, timeout):
            return dict(access_token="bad")

        # Add a token to the cache that will expire soon.
        props["on_oidc_refresh_token"] = bad_refresh
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

        # Clear the cache.
        # Create a new client using the AWS device workflow.
        # Ensure that a ``find`` operation does not add credentials to the cache.
        _oidc_cache.clear()
        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user1")
        props = dict(DEVICE_NAME="aws")
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()
        assert len(_oidc_cache) == 0

    def test_speculative_auth_succeeds(self):
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

    def test_speculative_auth_fails(self):
        # Clear the cache
        _oidc_cache.clear()
        token_file = os.path.join(self.token_dir, "test_user1")

        def request_token(principal, info, timeout):
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token, expires_in_seconds=60)

        # Create a client with a request callback that returns a valid token
        # that will expire soon.
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

        client = MongoClient(self.uri_single, authmechanismproperties=props)

        # Set a fail point for saslStart commands.
        with self.fail_point(
            {
                "mode": {"times": 2},
                "data": {"failCommands": ["saslStart"], "errorCode": 18},
            }
        ):
            # Perform a find operation that fails.
            with self.assertRaises(OperationFailure):
                client.test.test.find_one()

        # Close the client.
        client.close()

    def test_reauthenticate_read(self):
        token_file = os.path.join(self.token_dir, "test_user1")
        refresh_called = 0
        listener = EventListener()

        # Clear the cache
        _oidc_cache.clear()

        # Create request and refresh callbacks that return valid credentials
        # that will not expire soon.
        def request_token(principal, info, timeout):
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token, expires_in_seconds=1000)

        def refresh_token(principal, info, creds, timeout):
            nonlocal refresh_called
            with open(token_file) as fid:
                token = fid.read()
            refresh_called += 1
            return dict(access_token=token, expires_in_seconds=1000)

        # Create a client with the callbacks.
        props: Dict = dict(on_oidc_request_token=request_token, on_oidc_refresh_token=refresh_token)
        client = MongoClient(
            self.uri_single, event_listeners=[listener], authmechanismproperties=props
        )

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the refresh callback has not been called.
        self.assertEqual(refresh_called, 0)

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
        self.assertEqual(refresh_called, 1)
        client.close()


if __name__ == "__main__":
    unittest.main()
