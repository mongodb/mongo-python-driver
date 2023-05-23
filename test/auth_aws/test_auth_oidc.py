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

import os
import sys
import threading
import time
import unittest
from contextlib import contextmanager
from typing import Dict

sys.path[0:0] = [""]

from test.utils import EventListener

from bson import SON
from pymongo import MongoClient
from pymongo.auth_oidc import _CACHE as _oidc_cache
from pymongo.cursor import CursorType
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.hello import HelloCompat
from pymongo.operations import InsertOne


class TestAuthOIDC(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri_single = os.environ["MONGODB_URI_SINGLE"]
        cls.uri_multiple = os.environ["MONGODB_URI_MULTIPLE"]
        cls.uri_admin = os.environ["MONGODB_URI"]
        cls.uri_provider = (
            os.environ["MONGODB_URI_SINGLE"] + "&authmechanismproperties=PROVIDER_NAME:aws"
        )
        cls.token_dir = os.environ["OIDC_TOKEN_DIR"]

    def setUp(self):
        self.request_called = 0
        self.refresh_called = 0
        _oidc_cache.clear()
        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user1")

    def create_request_cb(
        self, username="test_user1", expires_in_seconds=1e6, sleep=0, refresh_token=None
    ):

        token_file = os.path.join(self.token_dir, username)

        def request_token(server_info, context):
            # Validate the info.
            self.assertIn("issuer", server_info)
            self.assertIn("clientId", server_info)

            # Validate the timeout.
            timeout_seconds = context["timeout_seconds"]
            self.assertEqual(timeout_seconds, 60 * 5)
            with open(token_file) as fid:
                token = fid.read()
            resp = {"access_token": token}

            time.sleep(sleep)

            if expires_in_seconds is not None:
                resp["expires_in_seconds"] = expires_in_seconds

            if refresh_token is not None:
                resp["refresh_token"] = refresh_token

            self.request_called += 1
            return resp

        return request_token

    def create_refresh_cb(
        self, username="test_user1", expires_in_seconds=1e6, refresh_token_value="dummy"
    ):

        token_file = os.path.join(self.token_dir, username)

        def refresh_token(server_info, context):
            with open(token_file) as fid:
                token = fid.read()

            # Validate the info.
            self.assertIn("issuer", server_info)
            self.assertIn("clientId", server_info)

            # Validate the creds
            self.assertIsNotNone(context["refresh_token"])

            # Validate the timeout.
            self.assertEqual(context["timeout_seconds"], 60 * 5)

            resp = {"access_token": token}
            if expires_in_seconds is not None:
                resp["expires_in_seconds"] = expires_in_seconds
            if refresh_token_value is not None:
                resp["refresh_token"] = refresh_token_value

            self.refresh_called += 1
            return resp

        return refresh_token

    def set_fail_point(self, command_args):
        cmd_on = SON([("configureFailPoint", "failCommand")])
        cmd_on.update(command_args)
        client = MongoClient(self.uri_admin)
        client.admin.command(cmd_on)
        client.close()

    def clear_fail_point(self):
        client = MongoClient(self.uri_admin)
        client.admin.command("configureFailPoint", "failCommand", mode="off")
        client.close()

    @contextmanager
    def fail_point(self, command_args):
        self.set_fail_point(command_args)
        try:
            yield
        finally:
            self.clear_fail_point()

    # def test_connect_callbacks_single_implicit_username(self):
    #     request_token = self.create_request_cb()
    #     props: Dict = {"request_token_callback": request_token}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    # def test_connect_callbacks_single_explicit_username(self):
    #     request_token = self.create_request_cb()
    #     props: Dict = {"request_token_callback": request_token}
    #     client = MongoClient(self.uri_single, username="test_user1", authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    # def test_connect_callbacks_multiple_principal_user1(self):
    #     request_token = self.create_request_cb()
    #     props: Dict = {"request_token_callback": request_token}
    #     client = MongoClient(
    #         self.uri_multiple, username="test_user1", authmechanismproperties=props
    #     )
    #     client.test.test.find_one()
    #     client.close()

    # def test_connect_callbacks_multiple_principal_user2(self):
    #     request_token = self.create_request_cb("test_user2")
    #     props: Dict = {"request_token_callback": request_token}
    #     client = MongoClient(
    #         self.uri_multiple, username="test_user2", authmechanismproperties=props
    #     )
    #     client.test.test.find_one()
    #     client.close()

    # def test_connect_callbacks_multiple_no_username(self):
    #     request_token = self.create_request_cb()
    #     props: Dict = {"request_token_callback": request_token}
    #     client = MongoClient(self.uri_multiple, authmechanismproperties=props)
    #     with self.assertRaises(OperationFailure):
    #         client.test.test.find_one()
    #     client.close()

    # def test_allowed_hosts_blocked(self):
    #     request_token = self.create_request_cb()
    #     props: Dict = {"request_token_callback": request_token, "allowed_hosts": []}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)
    #     with self.assertRaises(ConfigurationError):
    #         client.test.test.find_one()
    #     client.close()

    #     props: Dict = {"request_token_callback": request_token, "allowed_hosts": ["example.com"]}
    #     client = MongoClient(
    #         self.uri_single + "&ignored=example.com", authmechanismproperties=props, connect=False
    #     )
    #     with self.assertRaises(ConfigurationError):
    #         client.test.test.find_one()
    #     client.close()

    # def test_connect_aws_single_principal(self):
    #     props = {"PROVIDER_NAME": "aws"}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    # def test_connect_aws_multiple_principal_user1(self):
    #     props = {"PROVIDER_NAME": "aws"}
    #     client = MongoClient(self.uri_multiple, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    # def test_connect_aws_multiple_principal_user2(self):
    #     os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user2")
    #     props = {"PROVIDER_NAME": "aws"}
    #     client = MongoClient(self.uri_multiple, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    # def test_connect_aws_allowed_hosts_ignored(self):
    #     props = {"PROVIDER_NAME": "aws", "allowed_hosts": []}
    #     client = MongoClient(self.uri_multiple, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    # def test_valid_callbacks(self):
    #     request_cb = self.create_request_cb(expires_in_seconds=60)
    #     refresh_cb = self.create_refresh_cb()

    #     props: Dict = {
    #         "request_token_callback": request_cb,
    #         "refresh_token_callback": refresh_cb,
    #     }
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     client = MongoClient(self.uri_single, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    # def test_lock_avoids_extra_callbacks(self):
    #     request_cb = self.create_request_cb(sleep=0.5)
    #     refresh_cb = self.create_refresh_cb()

    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}

    #     def run_test():
    #         client = MongoClient(self.uri_single, authMechanismProperties=props)
    #         client.test.test.find_one()
    #         client.close()

    #         client = MongoClient(self.uri_single, authMechanismProperties=props)
    #         client.test.test.find_one()
    #         client.close()

    #     t1 = threading.Thread(target=run_test)
    #     t2 = threading.Thread(target=run_test)
    #     t1.start()
    #     t2.start()
    #     t1.join()
    #     t2.join()

    #     self.assertEqual(self.request_called, 1)
    #     self.assertEqual(self.refresh_called, 0)

    def run_scenario_no_error(self, args, kwargs, expected_results=None):
        if not expected_results:
            expected_results = dict()

        # Clear the cache.
        _oidc_cache.clear()

        # Does it work with a first client?
        client = MongoClient(*args, **kwargs)
        if isinstance(expected_results.get("first"), Exception):
            with self.assertRaises(expected_results["first"]):
                client.test.test.find_one()
        else:
            client.test.test.find_one()
        client.close()

        # Does it work with a second client?
        client = MongoClient(*args, **kwargs)
        if isinstance(expected_results.get("second"), Exception):
            with self.assertRaises(expected_results["second"]):
                client.test.test.find_one()
        else:
            client.test.test.find_one()
        client.close()

    # def run_scenario_auth_error(self, args, kwargs, expected_result=None):
    #     _oidc_cache.clear()

    # if we get a 391, it means we need new creds
    # check for existing new creds -> if so, use them
    #   if not, call refresh if available
    #   if not, call request if available

    # If we fail for a given jwt:
    #   see what called us -> if it is refresh, then go back to idp_info unless idp_info guard is on, then raise
    #       if it is request, call refresh if available, otherwise idp_info unless idp_info guard is on, then raise

    # If we fail on idp_info -> raise

    # Okay, then how to simulate these failures?
    # We need spec auth to fail, and then simulate extra failures
    # So, how do we get spec auth to fail?
    # We can either override how spec auth works, or make sure it uses
    # bad credentials somehow.  In our case we can override how it works.

    # listener = EventListener()
    # kwargs['event_listeners'] = [listener]

    # # 18 no cache -> fail
    # client = MongoClient(*args, **kwargs)
    # with self.fail_point(
    #     {
    #         "mode": {"times": 2},
    #         "data": {"failCommands": ["saslStart", "hello"], "errorCode": 18},
    #     }
    # ):
    #     # Perform a find operation.
    #     with self.assertRaises(OperationFailure):
    #         client.test.test.find_one()
    # client.close()

    # # 18 w/cache -> then either succeed or fail
    # client = MongoClient(*args, **kwargs)
    # client.test.test.find_one()
    # client.close()

    # client = MongoClient(*args, **kwargs)
    # with self.fail_point(
    #     {
    #         "mode": {"times": 2},
    #         "data": {"failCommands": ["saslStart", "isMaster"], "errorCode": 18},
    #     }
    # ):
    #     # Perform a find operation.
    #     if isinstance(expected_result, Exception):
    #         with self.assertRaises(expected_result):
    #                 client.test.test.find_one()
    #     else:
    #         client.test.test.find_one()
    # client.close()

    def run_scenario_reauth_error(self, args, kwargs, expected_failures=None):
        if expected_failures is None:
            expected_failures = dict()

        _oidc_cache.clear()

        self._test_single_reauth(args, kwargs.copy(), expected_failures.get("singleReauth"))

        _oidc_cache.clear()

        self._test_double_reauth(args, kwargs.copy(), expected_failures.get("doubleReauth"))

        # Populate cache.
        client = MongoClient(*args, **kwargs)
        client.test.test.find_one()
        client.close()

        self._test_single_reauth(args, kwargs.copy(), expected_failures.get("singleReauthCache"))

        # Populate cache.
        client = MongoClient(*args, **kwargs)
        client.test.test.find_one()
        client.close()

        self._test_double_reauth(args, kwargs.copy(), expected_failures.get("doubleReauthCache"))

        # # Populate cache.
        # client = MongoClient(*args, **kwargs)
        # client.test.test.find_one()
        # client.close()

        # self._test_triple_reauth(args, kwargs.copy(), expected_failures.get('tripleReauth'))

    def _test_single_reauth(self, args, kwargs, expected_failure):
        listener = EventListener()
        kwargs["event_listeners"] = [listener]

        client = MongoClient(*args, **kwargs)
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            if expected_failure:
                with self.assertRaises(expected_failure):
                    client.test.test.find_one()
            else:
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

        if not expected_failure:
            self.assertEqual(
                started_events,
                [
                    "find",
                    "find",
                ],
            )
            self.assertEqual(succeeded_events, ["find"])
            self.assertEqual(failed_events, ["find"])
        else:
            raise ValueError

        client.close()

    def _test_double_reauth(self, args, kwargs, expected_failure):
        listener = EventListener()
        orig_failed_handler = listener.failed
        kwargs["event_listeners"] = [listener]

        def failed_handler(event):
            if event.command_name == "find":
                self.clear_fail_point()
                self.set_fail_point(
                    {
                        "mode": {"times": 1},
                        "data": {"failCommands": ["saslStart", "saslContinue"], "errorCode": 18},
                    }
                )
            orig_failed_handler(event)

        listener.failed = failed_handler
        listener.reset()
        client = MongoClient(*args, **kwargs)
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            if expected_failure:
                with self.assertRaises(expected_failure):
                    client.test.test.find_one()
            else:
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

        self.assertEqual(failed_events, ["find"])
        if not expected_failure:
            self.assertEqual(
                started_events,
                [
                    "find",
                    "find",
                ],
            )
            self.assertEqual(succeeded_events, ["find"])
        else:
            self.assertEqual(started_events, ["find"])
            self.assertEqual(succeeded_events, [])

        client.close()

    def _test_triple_reauth(self, args, kwargs, expected_failure):
        listener = EventListener()
        orig_failed_handler = listener.failed
        kwargs["event_listeners"] = [listener]

        def failed_handler(event):
            if event.command_name == "find":
                self.clear_fail_point()
                self.set_fail_point(
                    {
                        "mode": {"times": 2},
                        "data": {"failCommands": ["saslStart", "saslContinue"], "errorCode": 18},
                    }
                )
            orig_failed_handler(event)

        listener.failed = failed_handler
        listener.reset()
        client = MongoClient(*args, **kwargs)
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            if expected_failure:
                with self.assertRaises(expected_failure):
                    client.test.test.find_one()
            else:
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

        self.assertEqual(failed_events, ["find"])
        if not expected_failure:
            self.assertEqual(
                started_events,
                [
                    "find",
                    "find",
                ],
            )
            self.assertEqual(succeeded_events, ["find"])
        else:
            self.assertEqual(started_events, ["find"])
            self.assertEqual(succeeded_events, [])

        client.close()

    # def test_both_callbacks_no_exp(self):
    #     request_cb = self.create_request_cb(refresh_token="dummy")
    #     refresh_cb = self.create_refresh_cb()
    #     props = dict(request_token_callback=request_cb, refresh_token_callback=refresh_cb)
    #     args = (self.uri_single,)
    #     kwargs = dict(authMechanismProperties=props)
    #     self.run_scenario_no_error(args, kwargs)
    #     self.run_scenario_reauth_error(args, kwargs)

    def test_both_callbacks_no_exp_no_refresh_token(self):
        request_cb = self.create_request_cb()
        refresh_cb = self.create_refresh_cb(refresh_token_value="")
        props = dict(request_token_callback=request_cb, refresh_token_callback=refresh_cb)
        args = (self.uri_single,)
        kwargs = dict(authMechanismProperties=props)
        self.run_scenario_no_error(args, kwargs)
        expected = dict(doubleReauth=OperationFailure, doubleReauthCache=OperationFailure)
        self.run_scenario_reauth_error(args, kwargs, expected)

    # def test_single_callback_no_exp(self):
    #     request_cb = self.create_request_cb(refresh_token="dummy")
    #     props = dict(request_token_callback=request_cb)
    #     args = (self.uri_single,)
    #     kwargs = dict(authMechanismProperties=props)
    #     self.run_scenario_no_error(args, kwargs)
    #     self.run_scenario_reauth_error(args, kwargs)

    # def test_single_callback_exp(self):
    #     request_cb = self.create_request_cb(refresh_token="dummy", expires_in_seconds=10)
    #     props = dict(request_token_callback=request_cb)
    #     args = (self.uri_single,)
    #     kwargs = dict(authMechanismProperties=props)
    #     self.run_scenario_no_error(args, kwargs)
    #     self.run_scenario_reauth_error(args, kwargs)

    # def test_single_callback_no_exp_no_refresh(self):
    #     request_cb = self.create_request_cb()
    #     props = dict(request_token_callback=request_cb)
    #     args = (self.uri_single,)
    #     kwargs = dict(authMechanismProperties=props)
    #     self.run_scenario_no_error(args, kwargs)
    #     expected = dict(doubleReauth=OperationFailure, doubleReauthCache=OperationFailure)
    #     self.run_scenario_reauth_error(args, kwargs, expected)

    # def test_both_callbacks_exp(self):
    #     request_cb = self.create_request_cb(expires_in_seconds=10, refresh_token="dummy")
    #     refresh_cb = self.create_refresh_cb(expires_in_seconds=10)
    #     props = dict(request_token_callback=request_cb, refresh_token_callback=refresh_cb)
    #     args = (self.uri_single,)
    #     kwargs = dict(authMechanismProperties=props)
    #     self.run_scenario_no_error(args, kwargs)
    #     self.run_scenario_reauth_error(args, kwargs)

    # def test_automatic_creds(self):
    #     args = (self.uri_provider,)
    #     kwargs = dict()
    #     self.run_scenario_no_error(args, kwargs)
    #     self.run_scenario_reauth_error(args, kwargs)

    # TODO:
    # Implement logic where we only call refresh cb if there is a refresh token
    # Then make the refresh token opt-out
    # Add a test with two cbs, but the request doesn't return refresh - it should fail
    # Add a triple failure test with cache that succeeds - may have to tweak
    # the logic to get this to work.

    # def test_single_callback_with_refresh(self):

    #     # Test Scenario
    #     # Does it work with a first client?
    #     # Does it handle a second client?
    #     # Does it handle errors?
    #     # Does it handle errors when cache is cleared?
    #     # The inputs are the args and kwargs, and whether we expect any of these to fail.

    #     # So that is a test scenario
    #     # So, we get a provider name from environment
    #     # We also either have a single uri or not
    #     # If we have a single URI, we can run that test
    #     # We can always run the default URI and callbacks cases.
    #     # The callbacks have to get their data from the appropriate
    #     # Env.

    #     request_called = 0

    #     def request_token(_, __):
    #         nonlocal request_called
    #         token_file = os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"]
    #         with open(token_file) as fid:
    #             token = fid.read()
    #         resp = dict(access_token=token, expires_in_seconds=500, refresh_token="dummy")
    #         return resp

    #     props: Dict = {"request_token_callback": request_token_null}
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     with self.assertRaises(ValueError):
    #         client.test.test.find_one()
    #     client.close()

    # def test_single_callback_without_refresh(self):
    #     raise ValueError

    # def test_request_callback_returns_null(self):
    #     def request_token_null(a, b):
    #         return None

    #     props: Dict = {"request_token_callback": request_token_null}
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     with self.assertRaises(ValueError):
    #         client.test.test.find_one()
    #     client.close()

    # def test_refresh_callback_returns_null(self):
    #     request_cb = self.create_request_cb(expires_in_seconds=60)

    #     def refresh_token_null(a, b):
    #         return None

    #     props: Dict = {
    #         "request_token_callback": request_cb,
    #         "refresh_token_callback": refresh_token_null,
    #     }
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     with self.assertRaises(ValueError):
    #         client.test.test.find_one()
    #     client.close()

    # def test_request_callback_invalid_result(self):
    #     def request_token_invalid(a, b):
    #         return {}

    #     props: Dict = {"request_token_callback": request_token_invalid}
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     with self.assertRaises(ValueError):
    #         client.test.test.find_one()
    #     client.close()

    #     def request_cb_extra_value(server_info, context):
    #         result = self.create_request_cb()(server_info, context)
    #         result["foo"] = "bar"
    #         return result

    #     props: Dict = {"request_token_callback": request_cb_extra_value}
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     with self.assertRaises(ValueError):
    #         client.test.test.find_one()
    #     client.close()

    # def test_refresh_callback_missing_data(self):
    #     request_cb = self.create_request_cb(expires_in_seconds=60)

    #     def refresh_cb_no_token(a, b):
    #         return {}

    #     props: Dict = {
    #         "request_token_callback": request_cb,
    #         "refresh_token_callback": refresh_cb_no_token,
    #     }
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     with self.assertRaises(ValueError):
    #         client.test.test.find_one()
    #     client.close()

    # def test_refresh_callback_extra_data(self):
    #     request_cb = self.create_request_cb(expires_in_seconds=60)

    #     def refresh_cb_extra_value(server_info, context):
    #         result = self.create_refresh_cb()(server_info, context)
    #         result["foo"] = "bar"
    #         return result

    #     props: Dict = {
    #         "request_token_callback": request_cb,
    #         "refresh_token_callback": refresh_cb_extra_value,
    #     }
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     with self.assertRaises(ValueError):
    #         client.test.test.find_one()
    #     client.close()

    # def test_cache_with_refresh(self):
    #     # Create a new client with a request callback and a refresh callback.  Both callbacks will read the contents of the ``AWS_WEB_IDENTITY_TOKEN_FILE`` location to obtain a valid access token.

    #     # Give a callback response with a valid accessToken and an expiresInSeconds that is within one minute.
    #     request_cb = self.create_request_cb(expires_in_seconds=60)
    #     refresh_cb = self.create_refresh_cb()

    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}

    #     # Ensure that a ``find`` operation adds credentials to the cache.
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     self.assertEqual(len(_oidc_cache), 1)

    #     # Create a new client with the same request callback and a refresh callback.
    #     # Ensure that a ``find`` operation results in a call to the refresh callback.
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     self.assertEqual(self.refresh_called, 1)
    #     self.assertEqual(len(_oidc_cache), 1)

    # def test_cache_with_no_refresh(self):
    #     # Create a new client with a request callback callback.
    #     # Give a callback response with a valid accessToken and an expiresInSeconds that is within one minute.
    #     request_cb = self.create_request_cb(expires_in_seconds=60)

    #     props = {"request_token_callback": request_cb}
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)

    #     # Ensure that a ``find`` operation adds credentials to the cache.
    #     self.request_called = 0
    #     client.test.test.find_one()
    #     client.close()
    #     self.assertEqual(self.request_called, 1)
    #     self.assertEqual(len(_oidc_cache), 1)

    #     # Create a new client with the same request callback.
    #     # Ensure that a ``find`` operation results in a call to the request callback.
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()
    #     self.assertEqual(self.request_called, 2)
    #     self.assertEqual(len(_oidc_cache), 1)

    # def test_cache_key_includes_callback(self):
    #     request_cb = self.create_request_cb()

    #     props: Dict = {"request_token_callback": request_cb}

    #     # Ensure that a ``find`` operation adds a new entry to the cache.
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     # Create a new client with a different request callback.
    #     def request_token_2(a, b):
    #         return request_cb(a, b)

    #     props["request_token_callback"] = request_token_2
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)

    #     # Ensure that a ``find`` operation adds a new entry to the cache.
    #     client.test.test.find_one()
    #     client.close()
    #     self.assertEqual(len(_oidc_cache), 2)

    # def test_cache_clears_on_error(self):
    #     request_cb = self.create_request_cb(expires_in_seconds=5 * 60)

    #     # Create a new client with a valid request callback that gives credentials that expire within 5 minutes and a refresh callback that gives invalid credentials.
    #     def refresh_cb(a, b):
    #         return {"access_token": "bad"}

    #     # Add a token to the cache that will expire soon.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     # Create a new client with the same callbacks.
    #     client = MongoClient(self.uri_single, authMechanismProperties=props)

    #     # Ensure that another ``find`` operation results in an error.
    #     with self.assertRaises(OperationFailure):
    #         client.test.test.find_one()

    #     client.close()

    #     # Ensure that the cache has been cleared.
    #     authenticator = list(_oidc_cache.values())[0]
    #     self.assertIsNone(authenticator.idp_info)

    # def test_cache_is_not_used_in_aws_automatic_workflow(self):
    #     # Create a new client using the AWS device workflow.
    #     # Ensure that a ``find`` operation does not add credentials to the cache.
    #     props = {"PROVIDER_NAME": "aws"}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)
    #     client.test.test.find_one()
    #     client.close()

    #     # Ensure that the cache has been cleared.
    #     authenticator = list(_oidc_cache.values())[0]
    #     self.assertIsNone(authenticator.idp_info)

    # def test_speculative_auth_success(self):
    #     # Clear the cache
    #     _oidc_cache.clear()
    #     token_file = os.path.join(self.token_dir, "test_user1")

    #     def request_token(a, b):
    #         with open(token_file) as fid:
    #             token = fid.read()
    #         return {"access_token": token, "expires_in_seconds": 1000}

    #     # Create a client with a request callback that returns a valid token
    #     # that will not expire soon.
    #     props: Dict = {"request_token_callback": request_token}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Set a fail point for saslStart commands.
    #     with self.fail_point(
    #         {
    #             "mode": {"times": 2},
    #             "data": {"failCommands": ["saslStart"], "errorCode": 18},
    #         }
    #     ):
    #         # Perform a find operation.
    #         client.test.test.find_one()

    #     # Close the client.
    #     client.close()

    #     # Create a new client.
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Set a fail point for saslStart commands.
    #     with self.fail_point(
    #         {
    #             "mode": {"times": 2},
    #             "data": {"failCommands": ["saslStart"], "errorCode": 18},
    #         }
    #     ):
    #         # Perform a find operation.
    #         client.test.test.find_one()

    #     # Close the client.
    #     client.close()

    # def test_reauthenticate_succeeds(self):
    #     listener = EventListener()

    #     # Create request and refresh callbacks that return valid credentials
    #     # that will not expire soon.
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(
    #         self.uri_single, event_listeners=[listener], authmechanismproperties=props
    #     )

    #     # Perform a find operation.
    #     client.test.test.find_one()

    #     # Assert that the refresh callback has not been called.
    #     self.assertEqual(self.refresh_called, 0)

    #     listener.reset()

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a find operation.
    #         client.test.test.find_one()

    #     started_events = [
    #         i.command_name for i in listener.started_events if not i.command_name.startswith("sasl")
    #     ]
    #     succeeded_events = [
    #         i.command_name
    #         for i in listener.succeeded_events
    #         if not i.command_name.startswith("sasl")
    #     ]
    #     failed_events = [
    #         i.command_name for i in listener.failed_events if not i.command_name.startswith("sasl")
    #     ]

    #     self.assertEqual(
    #         started_events,
    #         [
    #             "find",
    #             "find",
    #         ],
    #     )
    #     self.assertEqual(succeeded_events, ["find"])
    #     self.assertEqual(failed_events, ["find"])

    #     # Assert that the refresh callback has been called.
    #     self.assertEqual(self.refresh_called, 1)
    #     client.close()

    # def test_reauthenticate_succeeds_aws(self):
    #     listener = EventListener()

    #     # Create a client.
    #     props = {"PROVIDER_NAME": "aws"}
    #     client = MongoClient(
    #         self.uri_single, event_listeners=[listener], authMechanismProperties=props
    #     )

    #     # Perform a find operation.
    #     client.test.test.find_one()

    #     listener.reset()

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a find operation.
    #         client.test.test.find_one()

    #     started_events = [
    #         i.command_name for i in listener.started_events if not i.command_name.startswith("sasl")
    #     ]
    #     succeeded_events = [
    #         i.command_name
    #         for i in listener.succeeded_events
    #         if not i.command_name.startswith("sasl")
    #     ]
    #     failed_events = [
    #         i.command_name for i in listener.failed_events if not i.command_name.startswith("sasl")
    #     ]

    #     self.assertEqual(
    #         started_events,
    #         [
    #             "find",
    #             "find",
    #         ],
    #     )
    #     self.assertEqual(succeeded_events, ["find"])
    #     self.assertEqual(failed_events, ["find"])

    #     client.close()

    # def test_reauthenticate_succeeds_bulk_write(self):
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Perform a find operation.
    #     client.test.test.find_one()

    #     # Assert that the refresh callback has not been called.
    #     self.assertEqual(self.refresh_called, 0)

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["insert"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a bulk write operation.
    #         client.test.test.bulk_write([InsertOne({})])

    #     # Assert that the refresh callback has been called.
    #     self.assertEqual(self.refresh_called, 1)
    #     client.close()

    # def test_reauthenticate_succeeds_bulk_read(self):
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Perform a find operation.
    #     client.test.test.find_one()

    #     # Perform a bulk write operation.
    #     client.test.test.bulk_write([InsertOne({})])

    #     # Assert that the refresh callback has not been called.
    #     self.assertEqual(self.refresh_called, 0)

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a bulk read operation.
    #         cursor = client.test.test.find_raw_batches({})
    #         list(cursor)

    #     # Assert that the refresh callback has been called.
    #     self.assertEqual(self.refresh_called, 1)
    #     client.close()

    # def test_reauthenticate_succeeds_cursor(self):
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Perform an insert operation.
    #     client.test.test.insert_one({"a": 1})

    #     # Assert that the refresh callback has not been called.
    #     self.assertEqual(self.refresh_called, 0)

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a find operation.
    #         cursor = client.test.test.find({"a": 1})
    #         self.assertGreaterEqual(len(list(cursor)), 1)

    #     # Assert that the refresh callback has been called.
    #     self.assertEqual(self.refresh_called, 1)
    #     client.close()

    # def test_reauthenticate_succeeds_get_more(self):
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Perform an insert operation.
    #     client.test.test.insert_many([{"a": 1}, {"a": 1}])

    #     # Assert that the refresh callback has not been called.
    #     self.assertEqual(self.refresh_called, 0)

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["getMore"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a find operation.
    #         cursor = client.test.test.find({"a": 1}, batch_size=1)
    #         self.assertGreaterEqual(len(list(cursor)), 1)

    #     # Assert that the refresh callback has been called.
    #     self.assertEqual(self.refresh_called, 1)
    #     client.close()

    # def test_reauthenticate_succeeds_get_more_exhaust(self):
    #     # Ensure no mongos
    #     props = {"PROVIDER_NAME": "aws"}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)
    #     hello = client.admin.command(HelloCompat.LEGACY_CMD)
    #     if hello.get("msg") != "isdbgrid":
    #         raise unittest.SkipTest("Must not be a mongos")

    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Perform an insert operation.
    #     client.test.test.insert_many([{"a": 1}, {"a": 1}])

    #     # Assert that the refresh callback has not been called.
    #     self.assertEqual(self.refresh_called, 0)

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["getMore"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a find operation.
    #         cursor = client.test.test.find({"a": 1}, batch_size=1, cursor_type=CursorType.EXHAUST)
    #         self.assertGreaterEqual(len(list(cursor)), 1)

    #     # Assert that the refresh callback has been called.
    #     self.assertEqual(self.refresh_called, 1)
    #     client.close()

    # def test_reauthenticate_succeeds_command(self):
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}

    #     print("start of test")
    #     client = MongoClient(self.uri_single, authmechanismproperties=props)

    #     # Perform an insert operation.
    #     client.test.test.insert_one({"a": 1})

    #     # Assert that the refresh callback has not been called.
    #     self.assertEqual(self.refresh_called, 0)

    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["count"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a count operation.
    #         cursor = client.test.command({"count": "test"})

    #     self.assertGreaterEqual(len(list(cursor)), 1)

    #     # Assert that the refresh callback has been called.
    #     self.assertEqual(self.refresh_called, 1)
    #     client.close()

    # def test_reauthenticate_retries_and_succeeds_with_cache(self):
    #     listener = EventListener()

    #     # Create request and refresh callbacks that return valid credentials
    #     # that will not expire soon.
    #     request_cb = self.create_request_cb()
    #     _refresh_cb = self.create_refresh_cb()

    #     def refresh_cb(info, context):
    #         self.clear_fail_point()
    #         self.set_fail_point(
    #             {
    #                 "mode": {"times": 1},
    #                 "data": {"failCommands": ["saslStart"], "errorCode": 18},
    #             }
    #         )
    #         return _refresh_cb(info, context)

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(
    #         self.uri_single, event_listeners=[listener], authmechanismproperties=props
    #     )

    #     # Perform a find operation.
    #     client.test.test.find_one()

    #     self.assertEqual(self.request_called, 1)
    #     self.assertEqual(self.refresh_called, 0)

    #     # Set a fail point for ``saslStart`` commands of the form
    #     self.set_fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     )

    #     # Perform a find operation that succeeds.
    #     client.test.test.find_one()
    #     self.clear_fail_point()

    #     self.assertEqual(self.request_called, 2)
    #     self.assertEqual(self.refresh_called, 1)

    #     # Close the client.
    #     client.close()

    # def test_reauthenticate_fails_with_no_cache(self):
    #     #listener = EventListener()

    #     # Create request and refresh callbacks that return valid credentials
    #     # that will not expire soon.
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     def failed_handler(event):
    #         if event.command_name == "find":
    #             self.clear_fail_point()
    #             self.set_fail_point(
    #                 {
    #                     "mode": {"times": 1},
    #                     "data": {"failCommands": ["saslContinue"], "errorCode": 18},
    #                 }
    #             )

    #     listener = EventListener()
    #     listener.failed = failed_handler

    #     # Create a client with the callbacks.
    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client = MongoClient(
    #         self.uri_single, event_listeners=[listener], authmechanismproperties=props
    #     )

    #     # Perform a find operation.
    #     client.test.test.find_one()

    #     # Clear the cache.
    #     _oidc_cache.clear()

    #     self.set_fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     )

    #     # Perform a find operation that fails.
    #     with self.assertRaises(OperationFailure):
    #         client.test.test.find_one()

    #     self.clear_fail_point()

    #     client.close()

    # def test_late_reauth_avoids_callback(self):
    #     # Step 1: connect with both clients
    #     request_cb = self.create_request_cb()
    #     refresh_cb = self.create_refresh_cb()

    #     props: Dict = {"request_token_callback": request_cb, "refresh_token_callback": refresh_cb}
    #     client1 = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client1.test.test.find_one()
    #     client2 = MongoClient(self.uri_single, authMechanismProperties=props)
    #     client2.test.test.find_one()

    #     self.assertEqual(self.refresh_called, 0)
    #     self.assertEqual(self.request_called, 1)

    #     # Step 2: cause a find 391 on the first client
    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a find operation that succeeds.
    #         client1.test.test.find_one()

    #     self.assertEqual(self.refresh_called, 1)
    #     self.assertEqual(self.request_called, 1)

    #     # Step 3: cause a find 391 on the second client
    #     with self.fail_point(
    #         {
    #             "mode": {"times": 1},
    #             "data": {"failCommands": ["find"], "errorCode": 391},
    #         }
    #     ):
    #         # Perform a find operation that succeeds.
    #         client2.test.test.find_one()

    #     self.assertEqual(self.refresh_called, 1)
    #     self.assertEqual(self.request_called, 1)

    #     client1.close()
    #     client2.close()


if __name__ == "__main__":
    unittest.main()
