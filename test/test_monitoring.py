# Copyright 2015 MongoDB, Inc.
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

import sys

sys.path[0:0] = [""]

from bson.son import SON
from pymongo import monitoring
from pymongo.errors import OperationFailure
from test import unittest, IntegrationTest


class EventListener(monitoring.Subscriber):

    def __init__(self):
        self.results = {}

    def started(self, event):
        self.results['started'] = event

    def succeeded(self, event):
        self.results['succeeded'] = event

    def failed(self, event):
        self.results['failed'] = event


class TestCommandMonitoring(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        cls.listener = EventListener()
        cls.saved_subscribers = monitoring._SUBSCRIBERS
        monitoring.subscribe(cls.listener)
        super(TestCommandMonitoring, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        monitoring._SUBSCRIBERS = cls.saved_subscribers

    def tearDown(self):
        self.listener.results = {}

    def test_started_simple(self):
        self.client.pymongo_test.command('ismaster')
        results = self.listener.results
        started = results.get('started')
        succeeded = results.get('succeeded')
        self.assertIsNone(results.get('failed'))
        self.assertTrue(
            isinstance(succeeded, monitoring.CommandSucceededEvent))
        self.assertTrue(
            isinstance(started, monitoring.CommandStartedEvent))
        self.assertEqual(SON([('ismaster', 1)]), started.command)
        self.assertEqual('ismaster', started.command_name)
        self.assertEqual(self.client.address, started.connection_id)
        self.assertEqual('pymongo_test', started.database_name)
        self.assertTrue(isinstance(started.request_id, int))

    def test_succeeded_simple(self):
        self.client.pymongo_test.command('ismaster')
        results = self.listener.results
        started = results.get('started')
        succeeded = results.get('succeeded')
        self.assertIsNone(results.get('failed'))
        self.assertTrue(
            isinstance(started, monitoring.CommandStartedEvent))
        self.assertTrue(
            isinstance(succeeded, monitoring.CommandSucceededEvent))
        self.assertEqual('ismaster', succeeded.command_name)
        self.assertEqual(self.client.address, succeeded.connection_id)
        self.assertEqual(1, succeeded.reply.get('ok'))
        self.assertTrue(isinstance(succeeded.request_id, int))
        self.assertTrue(isinstance(succeeded.duration_micros, int))

    def test_failed_simple(self):
        try:
            self.client.pymongo_test.command('oops!')
        except OperationFailure:
            pass
        results = self.listener.results
        started = results.get('started')
        failed = results.get('failed')
        self.assertIsNone(results.get('succeeded'))
        self.assertTrue(
            isinstance(started, monitoring.CommandStartedEvent))
        self.assertTrue(
            isinstance(failed, monitoring.CommandFailedEvent))
        self.assertEqual('oops!', failed.command_name)
        self.assertEqual(self.client.address, failed.connection_id)
        self.assertEqual(0, failed.failure.get('ok'))
        self.assertTrue(isinstance(failed.request_id, int))
        self.assertTrue(isinstance(failed.duration_micros, int))


if __name__ == "__main__":
    unittest.main()
