# Copyright 2020-present MongoDB, Inc.
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

import os
import sys
from pathlib import Path
from test.unified_format import generate_test_classes, get_test_path

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils_shared import OvertCommandListener

from pymongo.server_api import ServerApi

_IS_SYNC = True

# Generate unified tests.
globals().update(generate_test_classes(get_test_path("versioned-api"), module=__name__))


class TestServerApiIntegration(IntegrationTest):
    RUN_ON_LOAD_BALANCER = True

    def assertServerApi(self, event):
        self.assertIn("apiVersion", event.command)
        self.assertEqual(event.command["apiVersion"], "1")

    def assertServerApiInAllCommands(self, events):
        for event in events:
            self.assertServerApi(event)

    @client_context.require_version_min(4, 7)
    def test_command_options(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(server_api=ServerApi("1"), event_listeners=[listener])
        coll = client.test.test
        coll.insert_many([{} for _ in range(100)])
        self.addCleanup(coll.delete_many, {})
        coll.find(batch_size=25).to_list()
        client.admin.command("ping")
        self.assertServerApiInAllCommands(listener.started_events)

    @client_context.require_version_min(4, 7)
    @client_context.require_transactions
    def test_command_options_txn(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(server_api=ServerApi("1"), event_listeners=[listener])
        coll = client.test.test
        coll.insert_many([{} for _ in range(100)])
        self.addCleanup(coll.delete_many, {})

        listener.reset()
        with client.start_session() as s, s.start_transaction():
            coll.insert_many([{} for _ in range(100)], session=s)
            coll.find(batch_size=25, session=s).to_list()
            client.test.command("find", "test", session=s)
            self.assertServerApiInAllCommands(listener.started_events)


if __name__ == "__main__":
    unittest.main()
