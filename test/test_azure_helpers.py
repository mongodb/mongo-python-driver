# Copyright 2026-present MongoDB, Inc.
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

"""Unit tests for _azure_helpers.py.

These tests mock urlopen to avoid requiring a live Azure IMDS endpoint.
Integration tests that exercise the real endpoint are gated by environment
variables in test_on_demand_csfle.py and test_auth_oidc.py.
"""

from __future__ import annotations

import json
import sys
import unittest
from unittest.mock import patch

sys.path[0:0] = [""]

from pymongo._azure_helpers import _get_azure_response


class _MockResponse:
    """Minimal context-manager response for urlopen."""

    def __init__(self, body: str, status: int = 200):
        self.status = status
        self._body = body.encode("utf8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class TestGetAzureResponse(unittest.TestCase):
    def _call(self, resource="https://example.com/", client_id=None, timeout=5):
        return _get_azure_response(resource, client_id=client_id, timeout=timeout)

    def test_success_without_client_id(self):
        body = json.dumps({"access_token": "tok", "expires_in": "3600"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)) as mock_open:
            result = self._call()

        self.assertEqual(result["access_token"], "tok")
        self.assertEqual(result["expires_in"], "3600")

        # Verify client_id was NOT added to the URL
        url = mock_open.call_args[0][0].full_url
        self.assertNotIn("client_id", url)

    def test_success_with_client_id(self):
        body = json.dumps({"access_token": "tok", "expires_in": "3600"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)) as mock_open:
            result = self._call(client_id="my-client-id")

        self.assertEqual(result["access_token"], "tok")
        url = mock_open.call_args[0][0].full_url
        self.assertIn("client_id=my-client-id", url)

    def test_url_contains_resource_and_api_version(self):
        body = json.dumps({"access_token": "tok", "expires_in": "3600"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)) as mock_open:
            self._call(resource="https://test-resource.example.com")

        url = mock_open.call_args[0][0].full_url
        self.assertIn("api-version=2018-02-01", url)
        self.assertIn("resource=https://test-resource.example.com", url)

    def test_request_headers(self):
        body = json.dumps({"access_token": "tok", "expires_in": "3600"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)) as mock_open:
            self._call()

        request = mock_open.call_args[0][0]
        self.assertEqual(request.get_header("Metadata"), "true")
        self.assertEqual(request.get_header("Accept"), "application/json")

    def test_urlopen_exception_raises_value_error(self):
        with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
            with self.assertRaises(ValueError) as ctx:
                self._call()

        self.assertIn("Failed to acquire IMDS access token", str(ctx.exception))

    def test_non_200_status_raises_value_error(self):
        body = json.dumps({"error": "something went wrong"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body, status=400)):
            with self.assertRaises(ValueError) as ctx:
                self._call()

        self.assertIn("Failed to acquire IMDS access token", str(ctx.exception))

    def test_non_json_body_raises_value_error(self):
        with patch("urllib.request.urlopen", return_value=_MockResponse("not-json")):
            with self.assertRaises(ValueError) as ctx:
                self._call()

        self.assertIn("Azure IMDS response must be in JSON format", str(ctx.exception))

    def test_missing_access_token_raises_value_error(self):
        body = json.dumps({"expires_in": "3600"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)):
            with self.assertRaises(ValueError) as ctx:
                self._call()

        self.assertIn("access_token", str(ctx.exception))

    def test_missing_expires_in_raises_value_error(self):
        body = json.dumps({"access_token": "tok"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)):
            with self.assertRaises(ValueError) as ctx:
                self._call()

        self.assertIn("expires_in", str(ctx.exception))

    def test_empty_access_token_raises_value_error(self):
        body = json.dumps({"access_token": "", "expires_in": "3600"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)):
            with self.assertRaises(ValueError) as ctx:
                self._call()

        self.assertIn("access_token", str(ctx.exception))

    def test_empty_expires_in_raises_value_error(self):
        body = json.dumps({"access_token": "tok", "expires_in": ""})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)):
            with self.assertRaises(ValueError) as ctx:
                self._call()

        self.assertIn("expires_in", str(ctx.exception))

    def test_timeout_passed_to_urlopen(self):
        body = json.dumps({"access_token": "tok", "expires_in": "3600"})
        with patch("urllib.request.urlopen", return_value=_MockResponse(body)) as mock_open:
            self._call(timeout=42)

        _, kwargs = mock_open.call_args
        self.assertEqual(kwargs["timeout"], 42)


if __name__ == "__main__":
    unittest.main()
