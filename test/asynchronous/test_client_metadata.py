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
from test.asynchronous import AsyncIntegrationTest
from test.utils_shared import CMAPListener
from typing import Any, Optional

import pytest

from pymongo import AsyncMongoClient, MongoClient
from pymongo.driver_info import DriverInfo
from pymongo.monitoring import ConnectionClosedEvent

try:
    from mockupdb import MockupDB, OpMsgReply

    _HAVE_MOCKUPDB = True
except ImportError:
    _HAVE_MOCKUPDB = False

pytestmark = pytest.mark.mockupdb

_IS_SYNC = False


def _get_handshake_driver_info(request):
    assert "client" in request
    return request["client"]


class TestClientMetadataProse(AsyncIntegrationTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.server = MockupDB()
        self.handshake_req = None

        def respond(r):
            if "ismaster" in r:
                # then this is a handshake request
                self.handshake_req = r
            return r.reply(OpMsgReply(minWireVersion=0, maxWireVersion=13))

        self.server.autoresponds(respond)
        self.server.run()
        self.addAsyncCleanup(self.server.stop)

    async def send_ping_and_get_metadata(
        self, client: AsyncMongoClient, is_handshake: bool
    ) -> tuple[str, Optional[str], Optional[str], dict[str, Any]]:
        # reset if handshake request
        if is_handshake:
            self.handshake_req: Optional[dict] = None

        await client.admin.command("ping")
        metadata = _get_handshake_driver_info(self.handshake_req)
        driver_metadata = metadata["driver"]
        name, version, platform = (
            driver_metadata["name"],
            driver_metadata["version"],
            metadata["platform"],
        )
        return name, version, platform, metadata

    async def check_metadata_added(
        self,
        client: AsyncMongoClient,
        add_name: str,
        add_version: Optional[str],
        add_platform: Optional[str],
    ) -> None:
        # send initial metadata
        name, version, platform, metadata = await self.send_ping_and_get_metadata(client, True)
        await asyncio.sleep(0.005)

        # add new metadata
        client.append_metadata(DriverInfo(add_name, add_version, add_platform))
        new_name, new_version, new_platform, new_metadata = await self.send_ping_and_get_metadata(
            client, True
        )
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

    async def test_append_metadata(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        await self.check_metadata_added(client, "framework", "2.0", "Framework Platform")
        await client.close()

    async def test_append_metadata_platform_none(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        await self.check_metadata_added(client, "framework", "2.0", None)
        await client.close()

    async def test_append_metadata_version_none(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        await self.check_metadata_added(client, "framework", None, "Framework Platform")
        await client.close()

    async def test_append_metadata_platform_version_none(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
        )
        await self.check_metadata_added(client, "framework", None, None)
        await client.close()

    async def test_multiple_successive_metadata_updates(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string, maxIdleTimeMS=1, connect=False
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        await self.check_metadata_added(client, "framework", "2.0", "Framework Platform")
        await client.close()

    async def test_multiple_successive_metadata_updates_platform_none(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        await self.check_metadata_added(client, "framework", "2.0", None)
        await client.close()

    async def test_multiple_successive_metadata_updates_version_none(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        await self.check_metadata_added(client, "framework", None, "Framework Platform")
        await client.close()

    async def test_multiple_successive_metadata_updates_platform_version_none(self):
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
        )
        client.append_metadata(DriverInfo("library", "1.2", "Library Platform"))
        await self.check_metadata_added(client, "framework", None, None)
        await client.close()

    async def test_doesnt_update_established_connections(self):
        listener = CMAPListener()
        client = AsyncMongoClient(
            "mongodb://" + self.server.address_string,
            maxIdleTimeMS=1,
            driver=DriverInfo("library", "1.2", "Library Platform"),
            event_listeners=[listener],
        )

        # send initial metadata
        name, version, platform, metadata = await self.send_ping_and_get_metadata(client, True)
        self.assertIsNotNone(name)
        self.assertIsNotNone(version)
        self.assertIsNotNone(platform)

        # add data
        add_name, add_version, add_platform = "framework", "2.0", "Framework Platform"
        client.append_metadata(DriverInfo(add_name, add_version, add_platform))
        # check new data isn't sent
        self.handshake_req: Optional[dict] = None
        await client.admin.command("ping")
        self.assertIsNone(self.handshake_req)
        self.assertEqual(listener.event_count(ConnectionClosedEvent), 0)

        await client.close()


if __name__ == "__main__":
    unittest.main()
