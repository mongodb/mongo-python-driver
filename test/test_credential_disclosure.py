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

"""Test that URI parsing does not disclose credentials.

Implements the "Credential disclosure tests" in the connection string
specification (connection-string/tests/credential-disclosure.json).
"""

from __future__ import annotations

import json
import os
import sys
import traceback
import warnings

sys.path[0:0] = [""]

from pymongo.synchronous.uri_parser import parse_uri
from test import unittest

TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.path.join("connection_string", "test")
)

# The sentinel appears in the URI either literally or fully percent-encoded.
# Both forms are scanned case-insensitively: drivers lowercase option keys,
# and an undecoded echo still discloses a recoverable credential.
SENTINEL = "sentinelpassword1234"
ENCODED_SENTINEL = "%53%45%4e%54%49%4e%45%4c%70%61%73%73%77%6f%72%64%31%32%33%34"
SENTINEL_FORMS = (SENTINEL, ENCODED_SENTINEL)


class TestCredentialDisclosure(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("always")

    def _run_case(self, description: str, uri: str) -> None:
        """Parse ``uri`` and check every emitted message for the sentinel.

        The error is caught and both its message and its formatted
        traceback are inspected. The traceback stands in for letting the
        error escape to the interpreter's default handler: drivers have
        been observed to disclose on one path and not the other. Logged
        warnings are inspected as well.
        """
        for where, text in self._parse_and_capture(uri):
            for form in SENTINEL_FORMS:
                self.assertNotIn(
                    form,
                    text.lower(),
                    f"{where} disclosed the sentinel for '{description}'",
                )

    def _parse_and_capture(self, uri: str) -> list[tuple[str, str]]:
        captured: list[tuple[str, str]] = []
        with warnings.catch_warnings(record=True) as ctx:
            warnings.simplefilter("always")
            try:
                parse_uri(uri, warn=True)
            except Exception as exc:
                captured.append(("error message", str(exc)))
                captured.append(("traceback", "".join(traceback.format_exception(exc))))
            captured.extend(("warning", str(w.message)) for w in ctx)
        return captured

    def test_string_representation_redacted(self):
        """Spec prose test: redacted string representation.

        Idiomatic representations of a client constructed from a
        connection string MUST NOT include the username, the password, or
        the argument of a sensitive option. A sensitive option's argument
        is replaced with a fixed marker.
        """
        from pymongo.synchronous.mongo_client import MongoClient

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            client = MongoClient(
                "mongodb://SENTINELuser5678:SENTINELpassword1234@localhost:27017/db"
                "?tlsCertificateKeyFilePassword=SENTINELpassword1234"
            )
        for text in (repr(client), str(client)):
            for form in SENTINEL_FORMS:
                self.assertNotIn(form, text.lower())
        self.assertIn("tlscertificatekeyfilepassword='<redacted>'", repr(client).lower())


def _create_tests() -> None:
    with open(os.path.join(TEST_PATH, "credential-disclosure.json"), encoding="utf-8") as stream:
        cases = json.load(stream)["tests"]

    for case in cases:
        description = case["description"]

        def run_scenario(self, uri=case["uri"], description=description):
            self._run_case(description, uri)

        testname = "test_" + description.replace(" ", "_")
        run_scenario.__name__ = testname
        setattr(TestCredentialDisclosure, testname, run_scenario)


_create_tests()


if __name__ == "__main__":
    unittest.main()
