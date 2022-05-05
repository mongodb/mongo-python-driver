# Copyright 2019-present MongoDB, Inc.
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

"""Test retryable reads spec."""

import os
import pprint
import sys
import threading

sys.path[0:0] = [""]

from test import (
    IntegrationTest,
    PyMongoTestCase,
    client_context,
    client_knobs,
    unittest,
)
from test.utils import (
    CMAPListener,
    OvertCommandListener,
    TestCreator,
    rs_or_single_client,
)
from test.utils_spec_runner import SpecRunner

from pymongo.mongo_client import MongoClient
from pymongo.monitoring import (
    ConnectionCheckedOutEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckOutFailedReason,
    PoolClearedEvent,
)
from pymongo.write_concern import WriteConcern

# Location of JSON test specifications.
_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "retryable_reads", "legacy")


class TestClientOptions(PyMongoTestCase):
    def test_default(self):
        client = MongoClient(connect=False)
        self.assertEqual(client.options.retry_reads, True)

    def test_kwargs(self):
        client = MongoClient(retryReads=True, connect=False)
        self.assertEqual(client.options.retry_reads, True)
        client = MongoClient(retryReads=False, connect=False)
        self.assertEqual(client.options.retry_reads, False)

    def test_uri(self):
        client = MongoClient("mongodb://h/?retryReads=true", connect=False)
        self.assertEqual(client.options.retry_reads, True)
        client = MongoClient("mongodb://h/?retryReads=false", connect=False)
        self.assertEqual(client.options.retry_reads, False)


class TestSpec(SpecRunner):
    RUN_ON_LOAD_BALANCER = True
    RUN_ON_SERVERLESS = True

    @classmethod
    @client_context.require_failCommand_fail_point
    # TODO: remove this once PYTHON-1948 is done.
    @client_context.require_no_mmap
    def setUpClass(cls):
        super(TestSpec, cls).setUpClass()

    def maybe_skip_scenario(self, test):
        super(TestSpec, self).maybe_skip_scenario(test)
        skip_names = ["listCollectionObjects", "listIndexNames", "listDatabaseObjects"]
        for name in skip_names:
            if name.lower() in test["description"].lower():
                self.skipTest("PyMongo does not support %s" % (name,))

        # Serverless does not support $out and collation.
        if client_context.serverless:
            for operation in test["operations"]:
                if operation["name"] == "aggregate":
                    for stage in operation["arguments"]["pipeline"]:
                        if "$out" in stage:
                            self.skipTest("MongoDB Serverless does not support $out")
                if "collation" in operation["arguments"]:
                    self.skipTest("MongoDB Serverless does not support collations")

        # Skip changeStream related tests on MMAPv1 and serverless.
        test_name = self.id().rsplit(".")[-1]
        if "changestream" in test_name.lower():
            if client_context.storage_engine == "mmapv1":
                self.skipTest("MMAPv1 does not support change streams.")
            if client_context.serverless:
                self.skipTest("Serverless does not support change streams.")

    def get_scenario_coll_name(self, scenario_def):
        """Override a test's collection name to support GridFS tests."""
        if "bucket_name" in scenario_def:
            return scenario_def["bucket_name"]
        return super(TestSpec, self).get_scenario_coll_name(scenario_def)

    def setup_scenario(self, scenario_def):
        """Override a test's setup to support GridFS tests."""
        if "bucket_name" in scenario_def:
            data = scenario_def["data"]
            db_name = self.get_scenario_db_name(scenario_def)
            db = client_context.client[db_name]
            # Create a bucket for the retryable reads GridFS tests with as few
            # majority writes as possible.
            wc = WriteConcern(w="majority")
            if data:
                db["fs.chunks"].drop()
                db["fs.files"].drop()
                db["fs.chunks"].insert_many(data["fs.chunks"])
                db.get_collection("fs.files", write_concern=wc).insert_many(data["fs.files"])
            else:
                db.get_collection("fs.chunks").drop()
                db.get_collection("fs.files", write_concern=wc).drop()
        else:
            super(TestSpec, self).setup_scenario(scenario_def)


def create_test(scenario_def, test, name):
    @client_context.require_test_commands
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestSpec, _TEST_PATH)
test_creator.create_tests()


class FindThread(threading.Thread):
    def __init__(self, collection):
        super().__init__()
        self.daemon = True
        self.collection = collection
        self.passed = False

    def run(self):
        self.collection.find_one({})
        self.passed = True


class TestPoolPausedError(IntegrationTest):
    # Pools don't get paused in load balanced mode.
    RUN_ON_LOAD_BALANCER = False
    RUN_ON_SERVERLESS = False

    @client_context.require_failCommand_blockConnection
    @client_knobs(heartbeat_frequency=0.05, min_heartbeat_interval=0.05)
    def test_pool_paused_error_is_retryable(self):
        cmap_listener = CMAPListener()
        cmd_listener = OvertCommandListener()
        client = rs_or_single_client(maxPoolSize=1, event_listeners=[cmap_listener, cmd_listener])
        self.addCleanup(client.close)
        for _ in range(10):
            cmap_listener.reset()
            cmd_listener.reset()
            threads = [FindThread(client.pymongo_test.test) for _ in range(2)]
            fail_command = {
                "mode": {"times": 1},
                "data": {
                    "failCommands": ["find"],
                    "blockConnection": True,
                    "blockTimeMS": 1000,
                    "errorCode": 91,
                },
            }
            with self.fail_point(fail_command):
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                for thread in threads:
                    self.assertTrue(thread.passed)

            # It's possible that SDAM can rediscover the server and mark the
            # pool ready before the thread in the wait queue has a chance
            # to run. Repeat the test until the thread actually encounters
            # a PoolClearedError.
            if cmap_listener.event_count(ConnectionCheckOutFailedEvent):
                break

        # Via CMAP monitoring, assert that the first check out succeeds.
        cmap_events = cmap_listener.events_by_type(
            (ConnectionCheckedOutEvent, ConnectionCheckOutFailedEvent, PoolClearedEvent)
        )
        msg = pprint.pformat(cmap_listener.events)
        self.assertIsInstance(cmap_events[0], ConnectionCheckedOutEvent, msg)
        self.assertIsInstance(cmap_events[1], PoolClearedEvent, msg)
        self.assertIsInstance(cmap_events[2], ConnectionCheckOutFailedEvent, msg)
        self.assertEqual(cmap_events[2].reason, ConnectionCheckOutFailedReason.CONN_ERROR, msg)
        self.assertIsInstance(cmap_events[3], ConnectionCheckedOutEvent, msg)

        # Connection check out failures are not reflected in command
        # monitoring because we only publish command events _after_ checking
        # out a connection.
        started = cmd_listener.results["started"]
        msg = pprint.pformat(cmd_listener.results)
        self.assertEqual(3, len(started), msg)
        succeeded = cmd_listener.results["succeeded"]
        self.assertEqual(2, len(succeeded), msg)
        failed = cmd_listener.results["failed"]
        self.assertEqual(1, len(failed), msg)


if __name__ == "__main__":
    unittest.main()
