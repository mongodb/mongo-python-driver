# Copyright 2013-present MongoDB, Inc.
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
from __future__ import annotations

import asyncio
import os
import pathlib
import time
import unittest
from typing import Any, Optional, cast

import pytest

from pymongo import MongoClient
from pymongo.driver_info import DriverInfo
from pymongo.monitoring import ConnectionClosedEvent
from test import IntegrationTest
from test.unified_format import generate_test_classes, get_test_path
from test.utils_shared import CMAPListener

try:
    from mockupdb import MockupDB, OpMsgReply

    _HAVE_MOCKUPDB = True
except ImportError:
    _HAVE_MOCKUPDB = False

pytestmark = pytest.mark.mockupdb

_IS_SYNC = True

# Generate unified tests.
globals().update(generate_test_classes(get_test_path("handshake", "unified"), module=__name__))


def _get_handshake_driver_info(request):
    assert "client" in request
    return request["client"]


class TestClientMetadataProse(IntegrationTest):
    def setUp(self):
        super().setUp()
        self.server = MockupDB()
        self.handshake_req = None

        def respond(r):
            if "ismaster" in r:
                # then this is a handshake request
                self.handshake_req = r
            return r.reply(OpMsgReply(maxWireVersion=13))

        self.server.autoresponds(respond)
        self.server.run()
        self.addCleanup(self.server.stop)

    def send_ping_and_get_metadata(
        self, client: MongoClient, is_handshake: bool
    ) -> tuple[str, Optional[str], Optional[str], dict[str, Any]]:
        # reset if handshake request
        if is_handshake:
            self.handshake_req: Optional[dict] = None

        client.admin.command("ping")
        metadata = _get_handshake_driver_info(self.handshake_req)
        driver_metadata = metadata["driver"]
        name, version, platform = (
            driver_metadata["name"],
            driver_metadata["version"],
            metadata["platform"],
        )
        return name, version, platform, metadata

    def check_metadata_added(
        self,
        client: MongoClient,
        add_name: str,
        add_version: Optional[str],
        add_platform: Optional[str],
    ) -> None:
        # send initial metadata
        name, version, platform, metadata = self.send_ping_and_get_metadata(client, True)
        # wait for connection to become idle
        time.sleep(0.005)

        # add new metadata
        client.append_metadata(DriverInfo(add_name, add_version, add_platform))
        new_name, new_version, new_platform, new_metadata = self.send_ping_and_get_metadata(
            client, True
        )
        # Name and version always get a delimiter (empty string if None) to
        # preserve 1:1 index correspondence.
        self.assertEqual(new_name, f"{name}|{add_name or ''}")
        self.assertEqual(new_version, f"{version}|{add_version or ''}")
        self.assertEqual(
            new_platform,
            f"{platform}|{add_platform}" if add_platform is not None else platform,
        )

        metadata.pop("driver")
        metadata.pop("platform")
        new_metadata.pop("driver")
        new_metadata.pop("platform")
        self.assertEqual(metadata, new_metadata)

    def test_1_test_that_the_driver_updates_metadata(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", "2.0", "Framework Platform")

    def test_1_test_that_the_driver_updates_metadata_platform_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", "2.0", None)

    def test_1_test_that_the_driver_updates_metadata_version_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", None, "Framework Platform")

    def test_1_test_that_the_driver_updates_metadata_platform_version_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", None, None)

    def test_2_multiple_successive_metadata_updates(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string, maxIdleTimeMS=1, connect=False
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", "2.0", "Framework Platform")

    def test_2_multiple_successive_metadata_updates_platform_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", "2.0", None)

    def test_2_multiple_successive_metadata_updates_version_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", None, "Framework Platform")

    def test_2_multiple_successive_metadata_updates_platform_version_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", None, None)

    def test_doesnt_update_established_connections(self):
        listener = CMAPListener()
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
            event_listeners=[listener],
        )

        # send initial metadata
        name, version, platform, _metadata = self.send_ping_and_get_metadata(client, True)
        self.assertIsNotNone(name)
        self.assertIsNotNone(version)
        self.assertIsNotNone(platform)

        # add data
        add_name, add_version, add_platform = "framework", "2.0", "Framework Platform"
        client.append_metadata(DriverInfo(add_name, add_version, add_platform))
        # check new data isn't sent
        self.handshake_req: Optional[dict] = None
        client.admin.command("ping")
        self.assertIsNone(self.handshake_req)
        self.assertEqual(listener.event_count(ConnectionClosedEvent), 0)

    def test_duplicate_driver_name_no_op(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", None, None)
        # wait for connection to become idle
        time.sleep(0.005)
        # Append the exact same DriverInfo again: no-op.
        name, version, platform, _ = self.send_ping_and_get_metadata(client, True)
        time.sleep(0.005)
        client.append_metadata(DriverInfo("framework", None, None))
        new_name, new_version, new_platform, _ = self.send_ping_and_get_metadata(client, True)
        self.assertEqual(new_name, name)
        self.assertEqual(new_version, version)
        self.assertEqual(new_platform, platform)

    def test_9_handshake_documents_include_backpressure(self):
        # Create a `MongoClient` that is configured to record all handshake documents sent to the server as a part of
        # connection establishment.
        client = self.rs_or_single_client("mongodb://" + self.server.address_string)

        # Send a `ping` command to the server and verify that the command succeeds. This ensure that a connection is
        # established on all topologies.  Note: MockupDB only supports standalone servers.
        client.admin.command("ping")

        # Assert that for every handshake document intercepted:
        # the document has a field `backpressure` whose value is `"2"`.
        self.assertEqual(self.handshake_req["backpressure"], "2")

    def test_10_entries_in_driver_name_and_driver_version_correspond_by_index(self):
        cases = [
            ("Gap in middle (name)", [(None, None), ("F2", None)], "||F2", "||"),
            ("Gap in middle (version)", [("F1", None), ("F2", "2.0")], "|F1|F2", "||2.0"),
            ("Trailing delimiter retained", [("F1", None)], "|F1", "|"),
            (
                "Equal versions do not collapse",
                [("F1", "{driver_version}")],
                "|F1",
                "|{driver_version}",
            ),
            (
                "Equal names do not collapse",
                [("{driver_name}", "1.0")],
                "|{driver_name}",
                "|1.0",
            ),
            ("Duplicates still deduplicate", [("F1", "1.0"), ("F1", "1.0")], "|F1", "|1.0"),
            ("All versions absent", [("F1", None), ("F2", None)], "|F1|F2", "||"),
            ("All names absent", [(None, "1.0"), (None, "2.0")], "||", "|1.0|2.0"),
            (
                "Non-adjacent duplicate",
                [("F1", "1.0"), ("F2", "2.0"), ("F1", "1.0")],
                "|F1|F2",
                "|1.0|2.0",
            ),
            (
                "Platform-only difference is not a duplicate",
                [("F1", "1.0", "P1"), ("F1", "1.0", "P2")],
                "|F1|F1",
                "|1.0|1.0",
            ),
            (
                "Wrapper matching the driver's own identity",
                [("{driver_name}", "{driver_version}")],
                "|{driver_name}",
                "|{driver_version}",
            ),
        ]
        for (
            description,
            appended,
            expected_name_suffix,
            expected_version_suffix,
        ) in cases:
            with self.subTest(description=description):
                client = self.rs_or_single_client(
                    "mongodb://" + self.server.address_string,
                    maxIdleTimeMS=1,
                )
                self.addCleanup(client.close)
                # Capture the driver's own name and version from the first handshake.
                name0, version0, _, _ = self.send_ping_and_get_metadata(client, True)
                time.sleep(0.005)

                self.assertIsNotNone(name0)
                self.assertIsNotNone(version0)
                version0 = cast(str, version0)
                driver_name = name0.split("|")[0]
                driver_version = version0.split("|")[0]

                def resolve(value: Optional[str]) -> Optional[str]:
                    if value is None:
                        return None
                    return value.format(driver_name=driver_name, driver_version=driver_version)

                # Append each DriverInfo in order.
                for opts in appended:
                    d_name = resolve(opts[0]) if len(opts) > 0 else None
                    d_version = resolve(opts[1]) if len(opts) > 1 else None
                    d_platform = resolve(opts[2]) if len(opts) > 2 else None
                    client.append_metadata(DriverInfo(d_name or "", d_version, d_platform))

                # New handshake with the appended metadata.
                name1, version1, _, _ = self.send_ping_and_get_metadata(client, True)

                self.assertEqual(
                    name1,
                    name0
                    + expected_name_suffix.format(
                        driver_name=driver_name, driver_version=driver_version
                    ),
                )
                self.assertEqual(
                    version1,
                    version0
                    + expected_version_suffix.format(
                        driver_name=driver_name, driver_version=driver_version
                    ),
                )

    def test_11_appending_metadata_containing_the_delimiter_raises_an_error(self):
        cases = [
            ("frame|work", "2.0", "Framework Platform"),
            ("framework", "2|0", "Framework Platform"),
            ("framework", "2.0", "Framework|Platform"),
        ]
        for name, version, platform in cases:
            with self.subTest(name=name, version=version, platform=platform):
                client = self.rs_or_single_client(
                    "mongodb://" + self.server.address_string,
                    maxIdleTimeMS=1,
                    driver=DriverInfo("library", "1.2", "Library Platform"),
                )
                self.addCleanup(client.close)
                # Send initial handshake.
                name0, version0, platform0, _metadata = self.send_ping_and_get_metadata(
                    client, True
                )
                time.sleep(0.005)
                # Constructing metadata containing the delimiter raises.
                with self.assertRaises(ValueError):
                    DriverInfo(name, version, platform)
                # Metadata is unchanged on the next handshake.
                name1, version1, platform1, _ = self.send_ping_and_get_metadata(client, True)
                self.assertEqual(name1, name0)
                self.assertEqual(version1, version0)
                self.assertEqual(platform1, platform0)


if __name__ == "__main__":
    unittest.main()
