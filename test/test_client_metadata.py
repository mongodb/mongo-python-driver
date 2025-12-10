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
from test import IntegrationTest
from test.unified_format import generate_test_classes, get_test_path
from test.utils_shared import CMAPListener
from typing import Any, Optional

import pytest

from pymongo import MongoClient
from pymongo.driver_info import DriverInfo
from pymongo.monitoring import ConnectionClosedEvent

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
        if add_name is not None and add_name.lower() in name.lower().split("|"):
            self.assertEqual(name, new_name)
            self.assertEqual(version, new_version)
            self.assertEqual(platform, new_platform)
        else:
            self.assertEqual(new_name, f"{name}|{add_name}" if add_name is not None else name)
            self.assertEqual(
                new_version,
                f"{version}|{add_version}" if add_version is not None else version,
            )
            self.assertEqual(
                new_platform,
                f"{platform}|{add_platform}" if add_platform is not None else platform,
            )

        metadata.pop("driver")
        metadata.pop("platform")
        new_metadata.pop("driver")
        new_metadata.pop("platform")
        self.assertEqual(metadata, new_metadata)

    def test_append_metadata(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", "2.0", "Framework Platform")

    def test_append_metadata_platform_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", "2.0", None)

    def test_append_metadata_version_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", None, "Framework Platform")

    def test_append_metadata_platform_version_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        self.check_metadata_added(client, "framework", None, None)

    def test_multiple_successive_metadata_updates(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string, maxIdleTimeMS=1, connect=False
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", "2.0", "Framework Platform")

    def test_multiple_successive_metadata_updates_platform_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", "2.0", None)

    def test_multiple_successive_metadata_updates_version_none(self):
        client = self.rs_or_single_client(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        self.check_metadata_added(client, "framework", None, "Framework Platform")

    def test_multiple_successive_metadata_updates_platform_version_none(self):
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
        name, version, platform, metadata = self.send_ping_and_get_metadata(client, True)
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
        # add same metadata again
        self.check_metadata_added(client, "Framework", None, None)


if __name__ == "__main__":
    unittest.main()
