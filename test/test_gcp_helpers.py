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

"""Unit tests for pymongo/_gcp_helpers.py."""

from __future__ import annotations

import sys
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

sys.path[0:0] = [""]

from pymongo._gcp_helpers import _get_gcp_response


@contextmanager
def _mock_urlopen(status: int, body: str):
    """Context manager that patches ``urllib.request.urlopen`` with a fake response."""
    mock_response = MagicMock()
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_response.status = status
    mock_response.read.return_value = body.encode("utf8")

    with patch("urllib.request.urlopen", return_value=mock_response) as mock_open:
        yield mock_open


class TestGetGcpResponse(unittest.TestCase):
    """Tests for :func:`pymongo._gcp_helpers._get_gcp_response`."""

    def test_successful_response_returns_access_token(self):
        """A 200 response yields ``{"access_token": <body>}``."""
        token = "ya29.some-gcp-token"
        with _mock_urlopen(200, token):
            result = _get_gcp_response("https://example.com")

        self.assertEqual(result, {"access_token": token})

    def test_non_200_status_raises_value_error(self):
        """A non-200 HTTP status raises :class:`ValueError`."""
        for status in (400, 401, 403, 500, 503):
            with self.subTest(status=status):
                with _mock_urlopen(status, "error"):
                    with self.assertRaises(ValueError) as ctx:
                        _get_gcp_response("https://example.com")
                self.assertIn("Failed to acquire IMDS access token", str(ctx.exception))

    def test_urlopen_exception_raises_value_error(self):
        """An exception from ``urlopen`` is wrapped in :class:`ValueError`."""
        with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
            with self.assertRaises(ValueError) as ctx:
                _get_gcp_response("https://example.com")

        self.assertIn("Failed to acquire IMDS access token", str(ctx.exception))
        self.assertIn("connection refused", str(ctx.exception))

    def test_url_contains_resource_as_audience(self):
        """The ``resource`` argument is appended as ``?audience=`` in the URL."""
        resource = "https://my-service.example.com"
        with _mock_urlopen(200, "token") as mock_open:
            _get_gcp_response(resource)

        request_obj = mock_open.call_args[0][0]
        self.assertIn(f"?audience={resource}", request_obj.full_url)

    def test_request_has_metadata_flavor_google_header(self):
        """The request must include the ``Metadata-Flavor: Google`` header."""
        with _mock_urlopen(200, "token") as mock_open:
            _get_gcp_response("https://example.com")

        request_obj = mock_open.call_args[0][0]
        self.assertEqual(request_obj.get_header("Metadata-flavor"), "Google")

    def test_default_timeout_is_five_seconds(self):
        """Without an explicit timeout, ``urlopen`` is called with ``timeout=5``."""
        with _mock_urlopen(200, "token") as mock_open:
            _get_gcp_response("https://example.com")

        _, kwargs = mock_open.call_args
        self.assertEqual(kwargs.get("timeout"), 5)

    def test_custom_timeout_is_forwarded(self):
        """An explicit ``timeout`` value is passed through to ``urlopen``."""
        with _mock_urlopen(200, "token") as mock_open:
            _get_gcp_response("https://example.com", timeout=30)

        _, kwargs = mock_open.call_args
        self.assertEqual(kwargs.get("timeout"), 30)

    def test_urlopen_exception_does_not_chain_original(self):
        """The raised ``ValueError`` suppresses the original exception (``from None``)."""
        with patch("urllib.request.urlopen", side_effect=RuntimeError("network error")):
            with self.assertRaises(ValueError) as ctx:
                _get_gcp_response("https://example.com")

        # ``raise ... from None`` sets __cause__ to None and __suppress_context__ to True.
        self.assertIs(ctx.exception.__cause__, None)
        self.assertIs(ctx.exception.__suppress_context__, True)


if __name__ == "__main__":
    unittest.main()
