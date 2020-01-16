# Copyright 2016 MongoDB, Inc.
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

"""Run the sdam monitoring spec tests."""

import json
import os
import sys
import weakref

sys.path[0:0] = [""]

from bson.json_util import object_hook
from pymongo import monitoring
from pymongo import periodic_executor
from pymongo.errors import (ConnectionFailure,
                            NotMasterError)
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription
from pymongo.server_type import SERVER_TYPE
from pymongo.topology_description import TOPOLOGY_TYPE
from test import unittest, client_context, client_knobs, IntegrationTest
from test.utils import (ServerAndTopologyEventListener,
                        single_client,
                        rs_or_single_client,
                        wait_until)

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'sdam_monitoring')


def compare_server_descriptions(expected, actual):
    if ((not expected['address'] == "%s:%s" % actual.address) or
            (not SERVER_TYPE.__getattribute__(expected['type']) ==
                actual.server_type)):
        return False
    expected_hosts = set(
        expected['arbiters'] + expected['passives'] + expected['hosts'])
    return expected_hosts == set("%s:%s" % s for s in actual.all_hosts)


def compare_topology_descriptions(expected, actual):
    if not (TOPOLOGY_TYPE.__getattribute__(
            expected['topologyType']) == actual.topology_type):
        return False
    expected = expected['servers']
    actual = actual.server_descriptions()
    if len(expected) != len(actual):
        return False
    for exp_server in expected:
        for address, actual_server in actual.items():
            if compare_server_descriptions(exp_server, actual_server):
                break
        else:
            return False
    return True


def compare_events(expected_dict, actual):
    if not expected_dict:
        return False, "Error: Bad expected value in YAML test"
    if not actual:
        return False, "Error: Event published was None"

    expected_type, expected = list(expected_dict.items())[0]

    if expected_type == "server_opening_event":
        if not isinstance(actual, monitoring.ServerOpeningEvent):
            return False, "Expected ServerOpeningEvent, got %s" % (
                actual.__class__)
        if not expected['address'] == "%s:%s" % actual.server_address:
            return (False,
                    "ServerOpeningEvent published with wrong address (expected"
                    " %s, got %s" % (expected['address'],
                                     actual.server_address))

    elif expected_type == "server_description_changed_event":

        if not isinstance(actual, monitoring.ServerDescriptionChangedEvent):
            return (False,
                    "Expected ServerDescriptionChangedEvent, got %s" % (
                        actual.__class__))
        if not expected['address'] == "%s:%s" % actual.server_address:
            return (False, "ServerDescriptionChangedEvent has wrong address"
                           " (expected %s, got %s" % (expected['address'],
                                                      actual.server_address))

        if not compare_server_descriptions(
                expected['newDescription'], actual.new_description):
            return (False, "New ServerDescription incorrect in"
                           " ServerDescriptionChangedEvent")
        if not compare_server_descriptions(expected['previousDescription'],
                                           actual.previous_description):
            return (False, "Previous ServerDescription incorrect in"
                           " ServerDescriptionChangedEvent")

    elif expected_type == "server_closed_event":
        if not isinstance(actual, monitoring.ServerClosedEvent):
            return False, "Expected ServerClosedEvent, got %s" % (
                actual.__class__)
        if not expected['address'] == "%s:%s" % actual.server_address:
            return (False, "ServerClosedEvent published with wrong address"
                           " (expected %s, got %s" % (expected['address'],
                                                      actual.server_address))

    elif expected_type == "topology_opening_event":
        if not isinstance(actual, monitoring.TopologyOpenedEvent):
            return False, "Expected TopologyOpeningEvent, got %s" % (
                actual.__class__)

    elif expected_type == "topology_description_changed_event":
        if not isinstance(actual, monitoring.TopologyDescriptionChangedEvent):
            return (False, "Expected TopologyDescriptionChangedEvent,"
                           " got %s" % (actual.__class__))
        if not compare_topology_descriptions(expected['newDescription'],
                                             actual.new_description):
            return (False, "New TopologyDescription incorrect in "
                           "TopologyDescriptionChangedEvent")
        if not compare_topology_descriptions(
                expected['previousDescription'],
                actual.previous_description):
            return (False, "Previous TopologyDescription incorrect in"
                           " TopologyDescriptionChangedEvent")

    elif expected_type == "topology_closed_event":
        if not isinstance(actual, monitoring.TopologyClosedEvent):
            return False, "Expected TopologyClosedEvent, got %s" % (
                actual.__class__)

    else:
        return False, "Incorrect event: expected %s, actual %s" % (
            expected_type, actual)

    return True, ""


def compare_multiple_events(i, expected_results, actual_results):
    events_in_a_row = []
    j = i
    while(j < len(expected_results) and isinstance(
            actual_results[j],
            actual_results[i].__class__)):
        events_in_a_row.append(actual_results[j])
        j += 1
    message = ''
    for event in events_in_a_row:
        for k in range(i, j):
            passed, message = compare_events(expected_results[k], event)
            if passed:
                expected_results[k] = None
                break
        else:
            return i, False, message
    return j, True, ''


class TestAllScenarios(unittest.TestCase):

    @classmethod
    @client_context.require_connection
    def setUp(cls):
        cls.all_listener = ServerAndTopologyEventListener()


def create_test(scenario_def):
    def run_scenario(self):
        responses = (r for r in scenario_def['phases'][0]['responses'])

        with client_knobs(events_queue_frequency=0.1):
            class MockMonitor(Monitor):
                def __init__(self, server_description, topology, pool,
                             topology_settings):
                    """Have to copy entire constructor from Monitor so that we
                    can override _run and change the periodic executor's
                     interval."""

                    self._server_description = server_description
                    self._pool = pool
                    self._settings = topology_settings
                    self._avg_round_trip_time = MovingAverage()
                    options = self._settings._pool_options
                    self._listeners = options.event_listeners
                    self._publish = self._listeners is not None

                    def target():
                        monitor = self_ref()
                        if monitor is None:
                            return False
                        MockMonitor._run(monitor)  # Change target to subclass
                        return True

                    # Shorten interval
                    executor = periodic_executor.PeriodicExecutor(
                        interval=0.1,
                        min_interval=0.1,
                        target=target,
                        name="pymongo_server_monitor_thread")
                    self._executor = executor
                    self_ref = weakref.ref(self, executor.close)
                    self._topology = weakref.proxy(topology, executor.close)

                def _run(self):
                    try:
                        if self._server_description.address != ('a', 27017):
                            # Because PyMongo doesn't keep information about
                            # the order of addresses, we might accidentally
                            # start a MockMonitor on the wrong server first,
                            # so we need to only mock responses for the server
                            # the test's response is supposed to come from.
                            return
                        response = next(responses)[1]
                        isMaster = IsMaster(response)
                        self._server_description = ServerDescription(
                            address=self._server_description.address,
                            ismaster=isMaster)
                        self._topology.on_change(self._server_description)
                    except (ReferenceError, StopIteration):
                        # Topology was garbage-collected.
                        self.close()

            m = single_client(h=scenario_def['uri'], p=27017,
                              event_listeners=(self.all_listener,),
                              _monitor_class=MockMonitor)

            expected_results = scenario_def['phases'][0]['outcome']['events']

            expected_len = len(expected_results)
            wait_until(lambda: len(self.all_listener.results) >= expected_len,
                       "publish all events", timeout=15)

        try:
            i = 0
            while i < expected_len:
                result = self.all_listener.results[i] if len(
                    self.all_listener.results) > i else None
                # The order of ServerOpening/ClosedEvents doesn't matter
                if (isinstance(result,
                               monitoring.ServerOpeningEvent) or
                        isinstance(result,
                                   monitoring.ServerClosedEvent)):
                    i, passed, message = compare_multiple_events(
                        i, expected_results, self.all_listener.results)
                    self.assertTrue(passed, message)
                else:
                    self.assertTrue(
                        *compare_events(expected_results[i], result))
                    i += 1

        finally:
            m.close()
    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(
                    scenario_stream, object_hook=object_hook)
            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s' % (os.path.splitext(filename)[0],)
            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()


class TestSdamMonitoring(IntegrationTest):

    @classmethod
    @client_context.require_failCommand_fail_point
    def setUpClass(cls):
        super(TestSdamMonitoring, cls).setUpClass()
        # Speed up the tests by decreasing the event publish frequency.
        cls.knobs = client_knobs(events_queue_frequency=0.1)
        cls.knobs.enable()
        cls.listener = ServerAndTopologyEventListener()
        retry_writes = client_context.supports_transactions()
        cls.test_client = rs_or_single_client(
            event_listeners=[cls.listener], retryWrites=retry_writes)
        cls.coll = cls.test_client[cls.client.db.name].test
        cls.coll.insert_one({})

    @classmethod
    def tearDownClass(cls):
        cls.test_client.close()
        cls.knobs.disable()
        super(TestSdamMonitoring, cls).tearDownClass()

    def setUp(self):
        self.listener.reset()

    def _test_app_error(self, fail_command_opts, expected_error):
        address = self.test_client.address

        # Test that an application error causes a ServerDescriptionChangedEvent
        # to be published.
        data = {'failCommands': ['insert']}
        data.update(fail_command_opts)
        fail_insert = {
            'configureFailPoint': 'failCommand',
            'mode': {'times': 1},
            'data': data,
        }
        with self.fail_point(fail_insert):
            if self.test_client.retry_writes:
                self.coll.insert_one({})
            else:
                with self.assertRaises(expected_error):
                    self.coll.insert_one({})
                self.coll.insert_one({})

        def marked_unknown(event):
            return (
                isinstance(event, monitoring.ServerDescriptionChangedEvent)
                and event.server_address == address
                and not event.new_description.is_server_type_known)

        def discovered_node(event):
            return (
                isinstance(event, monitoring.ServerDescriptionChangedEvent)
                and event.server_address == address
                and not event.previous_description.is_server_type_known
                and event.new_description.is_server_type_known)

        def marked_unknown_and_rediscovered():
            return (len(self.listener.matching(marked_unknown)) >= 1 and
                    len(self.listener.matching(discovered_node)) >= 1)

        # Topology events are published asynchronously
        wait_until(marked_unknown_and_rediscovered, 'rediscover node')

        # Expect a single ServerDescriptionChangedEvent for the network error.
        marked_unknown_events = self.listener.matching(marked_unknown)
        self.assertEqual(len(marked_unknown_events), 1)
        self.assertIsInstance(
            marked_unknown_events[0].new_description.error, expected_error)

    def test_network_error_publishes_events(self):
        self._test_app_error({'closeConnection': True}, ConnectionFailure)

    def test_not_master_error_publishes_events(self):
        self._test_app_error({'errorCode': 10107, 'closeConnection': False},
                             NotMasterError)

    def test_shutdown_error_publishes_events(self):
        self._test_app_error({'errorCode': 91, 'closeConnection': False},
                             NotMasterError)


if __name__ == "__main__":
    unittest.main()
