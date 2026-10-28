# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for asynchronous MONGODB-AWS authentication."""

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from pymongo.asynchronous.auth_aws import _authenticate_aws
from test.asynchronous import AsyncUnitTest


class FakeAwsSaslContext:
    def __init__(self, credential):
        pass

    def step(self, payload):
        return b"test-payload"


class TestAsyncAwsAuth(AsyncUnitTest):
    async def test_clears_cached_credentials_on_connection_error(self):
        fake_aws = ModuleType("pymongo_auth_aws")
        fake_aws.__path__ = []
        fake_aws.__version__ = "test"

        fake_aws.PyMongoAuthAwsError = type("PyMongoAuthAwsError", (Exception,), {})

        fake_aws.AwsCredential = Mock()

        fake_aws.AwsSaslContext = FakeAwsSaslContext

        fake_auth = ModuleType("pymongo_auth_aws.auth")
        fake_auth.set_cached_credentials = Mock()
        fake_auth.set_use_cached_credentials = Mock()

        credentials = SimpleNamespace(
            username="test-user",
            password="test-password",
            mechanism_properties=SimpleNamespace(aws_session_token=None),
        )

        conn = SimpleNamespace(
            command=AsyncMock(side_effect=RuntimeError("connection failed"))
        )

        with patch.dict(
            sys.modules,
            {
                "pymongo_auth_aws": fake_aws,
                "pymongo_auth_aws.auth": fake_auth,
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "connection failed"):
                await _authenticate_aws(credentials, conn)

        fake_auth.set_use_cached_credentials.assert_called_once_with(True)
        fake_auth.set_cached_credentials.assert_called_once_with(None)