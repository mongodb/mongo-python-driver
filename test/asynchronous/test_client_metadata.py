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
import time
import unittest
from test.utils_shared import CMAPListener
from typing import Optional

import pytest
from mockupdb import MockupDB, OpMsgReply

from pymongo import AsyncMongoClient, MongoClient
from pymongo.driver_info import DriverInfo
from pymongo.monitoring import ConnectionClosedEvent

pytestmark = pytest.mark.mockupdb

_IS_SYNC = False


def _get_handshake_driver_info(request):
    assert "client" in request
    return request["client"]


class TestClientMetadataProse(unittest.TestCase):
    def setUp(self):
        self.server = MockupDB()
        self.handshake_req = None

        def respond(r):
            # Only save the very first request from the driver.
            if self.handshake_req is None:
                self.handshake_req = r
            return r.reply(OpMsgReply(minWireVersion=0, maxWireVersion=13))

        self.server.autoresponds(respond)
        self.server.run()
        self.addCleanup(self.server.stop)

    async def check_metadata_added(
        self, add_name: Optional[str], add_version: Optional[str], add_platform: Optional[str]
    ) -> None:
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )

        # send initial metadata
        await client.admin.command("ping")
        metadata = _get_handshake_driver_info(self.handshake_req)
        driver_metadata = metadata["driver"]
        name, version, platform = (
            driver_metadata["name"],
            driver_metadata["version"],
            metadata["platform"],
        )
        await asyncio.sleep(0.005)

        # add data
        client._append_metadata(DriverInfo(add_name, add_version, add_platform))
        # reset
        self.handshake_req = None
        await client.admin.command("ping")
        new_metadata = _get_handshake_driver_info(self.handshake_req)
        # compare
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

        await client.close()

    async def test_append_metadata(self):
        await self.check_metadata_added("framework", "2.0", "Framework Platform")

    async def test_append_metadata_platform_none(self):
        await self.check_metadata_added("framework", "2.0", None)

    async def test_append_metadata_version_none(self):
        await self.check_metadata_added("framework", None, "Framework Platform")

    async def test_append_platform_version_none(self):
        await self.check_metadata_added("framework", None, None)

    async def test_doesnt_update_established_connections(self):
        listener = CMAPListener()
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
            event_listeners=[listener],
        )

        # send initial metadata
        await client.admin.command("ping")
        metadata = _get_handshake_driver_info(self.handshake_req)
        driver_metadata = metadata["driver"]
        name, version, platform = (
            driver_metadata["name"],
            driver_metadata["version"],
            metadata["platform"],
        )
        # feels like i should do something to check that it is initially sent
        self.assertIsNotNone(name)
        self.assertIsNotNone(version)
        self.assertIsNotNone(platform)

        # add data
        add_name, add_version, add_platform = "framework", "2.0", "Framework Platform"
        client._append_metadata(DriverInfo(add_name, add_version, add_platform))
        # check new data isn't sent
        self.handshake_req = None
        await client.admin.command("ping")
        # if it was an actual handshake request, client data would be in the ping request which would start the handshake i think
        self.assertNotIn("client", self.handshake_req)
        self.assertEqual(listener.event_count(ConnectionClosedEvent), 0)

        await client.close()


# THESE ARE MY NOTES TO SELF, PLAESE IGNORE
# two options
# emit events with a flag, so when testing (like now), we can emit more stuff
# or we can mock it? but not sure if that ruins the spirit of the test -> this would be easier tho..
# we'd send, mock server would receive and send back to us?
# use mockup DB!
# usually, create mockupDB instance and then tell it to automatically respond to automatic responses, i'll need to change that for this but i also don't want to mess up SDAM stuff?
# i can get logs from server (in mongo orchestration) if mockup DB is too annoying (maybe get log?)
# ask the team generally but jib thinks we should emit events with a flag
# make sure to state pros and cons for each ticket

if __name__ == "__main__":
    unittest.main()
