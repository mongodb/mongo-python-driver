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

import time
import unittest
from test.utils_shared import CMAPListener
from typing import Optional

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


def _get_handshake_driver_info(request):
    assert "client" in request
    return request["client"]


class TestClientMetadataProse(unittest.TestCase):
    def setUp(self):
        self.server = MockupDB()
        # there are two handshake requests, i believe one is from the monitor, and the other is from the client
        self.monitor_handshake = False
        self.handshake_req: Optional[dict] = None

        def respond(r):
            # Only save the very first request from the driver.
            if self.handshake_req is None:
                if not self.monitor_handshake:
                    self.monitor_handshake = True
                else:
                    self.handshake_req = r
            return r.reply(OpMsgReply(minWireVersion=0, maxWireVersion=13))

        self.server.autoresponds(respond)
        self.server.run()
        self.addCleanup(self.server.stop)

    def check_metadata_added(
        self, add_name: str, add_version: Optional[str], add_platform: Optional[str]
    ) -> None:
        client = MongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )

        # send initial metadata
        client.admin.command("ping")
        metadata = _get_handshake_driver_info(self.handshake_req)
        driver_metadata = metadata["driver"]
        name, version, platform = (
            driver_metadata["name"],
            driver_metadata["version"],
            metadata["platform"],
        )
        time.sleep(0.005)

        # add data
        client._append_metadata(DriverInfo(add_name, add_version, add_platform))
        # make sure new metadata is being sent
        self.handshake_req = None
        client.admin.command("ping")
        # self.assertIsNotNone(self.handshake_req)
        new_metadata = _get_handshake_driver_info(self.handshake_req)

        self.assertEqual(
            new_metadata["driver"]["name"], f"{name}|{add_name}" if add_name is not None else name
        )
        self.assertEqual(
            new_metadata["driver"]["version"],
            f"{version}|{add_version}" if add_version is not None else version,
        )
        self.assertEqual(
            new_metadata["platform"],
            f"{platform}|{add_platform}" if add_platform is not None else platform,
        )

        metadata.pop("driver")
        metadata.pop("platform")
        new_metadata.pop("driver")
        new_metadata.pop("platform")
        self.assertEqual(metadata, new_metadata)

        client.close()

    def test_append_metadata(self):
        self.check_metadata_added("framework", "2.0", "Framework Platform")

    def test_append_metadata_platform_none(self):
        self.check_metadata_added("framework", "2.0", None)

    def test_append_metadata_version_none(self):
        self.check_metadata_added("framework", None, "Framework Platform")

    def test_append_platform_version_none(self):
        self.check_metadata_added("framework", None, None)

    def test_doesnt_update_established_connections(self):
        listener = CMAPListener()
        client = MongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
            event_listeners=[listener],
        )

        # send initial metadata
        client.admin.command("ping")
        metadata = _get_handshake_driver_info(self.handshake_req)
        driver_metadata = metadata["driver"]
        name, version, platform = (
            driver_metadata["name"],
            driver_metadata["version"],
            metadata["platform"],
        )
        self.assertIsNotNone(name)
        self.assertIsNotNone(version)
        self.assertIsNotNone(platform)

        # add data
        add_name, add_version, add_platform = "framework", "2.0", "Framework Platform"
        client._append_metadata(DriverInfo(add_name, add_version, add_platform))
        # check new data isn't sent
        self.handshake_req: Optional[dict] = None
        client.admin.command("ping")
        self.assertIsNotNone(self.handshake_req)
        assert self.handshake_req is not None  # so mypy knows that it's not None
        self.assertNotIn("client", self.handshake_req)
        self.assertEqual(listener.event_count(ConnectionClosedEvent), 0)

        client.close()


if __name__ == "__main__":
    unittest.main()
