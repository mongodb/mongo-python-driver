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

"""Test maxStalenessSeconds support."""

import datetime
import os
import time
import sys
import warnings

sys.path[0:0] = [""]

from bson import json_util
from pymongo import MongoClient, read_preferences
from pymongo.common import clean_node, HEARTBEAT_FREQUENCY
from pymongo.errors import ConfigurationError, ConnectionFailure
from pymongo.ismaster import IsMaster
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import writable_server_selector
from pymongo.settings import TopologySettings
from pymongo.topology import Topology

from test import client_context, unittest
from test.utils import rs_or_single_client

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'max_staleness')


class MockSocketInfo(object):
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockPool(object):
    def __init__(self, *args, **kwargs):
        pass

    def reset(self):
        pass


class MockMonitor(object):
    def __init__(self, server_description, topology, pool, topology_settings):
        pass

    def open(self):
        pass

    def request_check(self):
        pass

    def close(self):
        pass


def get_addresses(server_list):
    seeds = []
    hosts = []
    for server in server_list:
        seeds.append(clean_node(server['address']))
        hosts.append(server['address'])
    return seeds, hosts


def make_last_write_date(server):
    epoch = datetime.datetime.utcfromtimestamp(0)
    millis = server.get('lastWrite', {}).get('lastWriteDate')
    if millis:
        diff = ((millis % 1000) + 1000) % 1000
        seconds = (millis - diff) / 1000
        micros = diff * 1000
        return epoch + datetime.timedelta(
            seconds=seconds, microseconds=micros)
    else:
        # "Unknown" server.
        return epoch


def make_server_description(server, hosts):
    """Make ServerDescription from server info from JSON file."""
    server_type = server['type']
    if server_type == "Unknown":
        return ServerDescription(clean_node(server['address']), IsMaster({}))

    ismaster_response = {'ok': True, 'hosts': hosts}
    if server_type != "Standalone" and server_type != "Mongos":
        ismaster_response['setName'] = "rs"

    if server_type == "RSPrimary":
        ismaster_response['ismaster'] = True
    elif server_type == "RSSecondary":
        ismaster_response['secondary'] = True
    elif server_type == "Mongos":
        ismaster_response['msg'] = 'isdbgrid'

    ismaster_response['lastWrite'] = {
        'lastWriteDate': make_last_write_date(server)
    }

    for field in 'maxWireVersion', 'tags', 'idleWritePeriodMillis':
        if field in server:
            ismaster_response[field] = server[field]

    # Sets _last_update_time to now.
    sd = ServerDescription(clean_node(server['address']),
                           IsMaster(ismaster_response),
                           round_trip_time=server['avg_rtt_ms'])

    sd._last_update_time = server['lastUpdateTime'] / 1000.0  # ms to sec.
    return sd


class TestAllScenarios(unittest.TestCase):
    pass


def create_test(scenario_def):
    def run_scenario(self):
        if 'heartbeatFrequencyMS' in scenario_def:
            frequency = int(scenario_def['heartbeatFrequencyMS']) / 1000.0
        else:
            frequency = HEARTBEAT_FREQUENCY

        # Initialize topologies.
        seeds, hosts = get_addresses(
            scenario_def['topology_description']['servers'])

        topology = Topology(
            TopologySettings(seeds=seeds,
                             monitor_class=MockMonitor,
                             pool_class=MockPool,
                             heartbeat_frequency=frequency))

        # Update topologies with server descriptions.
        for server in scenario_def['topology_description']['servers']:
            server_description = make_server_description(server, hosts)
            topology.on_change(server_description)

        # Create server selector.
        # Make first letter lowercase to match read_pref's modes.
        pref_def = scenario_def['read_preference']
        mode_string = pref_def.get('mode', 'primary')
        mode_string = mode_string[:1].lower() + mode_string[1:]
        mode = read_preferences.read_pref_mode_from_name(mode_string)
        max_staleness = pref_def.get('maxStalenessSeconds', -1)
        tag_sets = pref_def.get('tag_sets')

        if scenario_def.get('error'):
            with self.assertRaises(ConfigurationError):
                # Error can be raised when making Read Pref or selecting.
                pref = read_preferences.make_read_preference(
                    mode, tag_sets=tag_sets, max_staleness=max_staleness)

                topology.select_server(pref)
            return

        expected_addrs = set([
            server['address'] for server in scenario_def['in_latency_window']])

        # Select servers.
        pref = read_preferences.make_read_preference(
            mode, tag_sets=tag_sets, max_staleness=max_staleness)

        if not expected_addrs:
            with self.assertRaises(ConnectionFailure):
                topology.select_servers(pref, server_selection_timeout=0)
            return

        servers = topology.select_servers(pref, server_selection_timeout=0)
        actual_addrs = set(['%s:%d' % s.description.address for s in servers])

        for unexpected in actual_addrs - expected_addrs:
            self.fail("'%s' shouldn't have been selected, but was" % unexpected)

        for unselected in expected_addrs - actual_addrs:
            self.fail("'%s' should have been selected, but wasn't" % unselected)

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)
        dirname = os.path.split(dirname[-2])[-1] + '_' + dirname[-1]

        for filename in filenames:
            if not filename.endswith('.json'):
                continue

            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json_util.loads(scenario_stream.read())

            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s_%s' % (
                dirname, os.path.splitext(filename)[0])

            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()


class TestMaxStaleness(unittest.TestCase):
    def test_max_staleness(self):
        client = MongoClient()
        self.assertEqual(-1, client.read_preference.max_staleness)

        client = MongoClient("mongodb://a/?readPreference=secondary")
        self.assertEqual(-1, client.read_preference.max_staleness)

        # These tests are specified in max-staleness-tests.rst.
        with self.assertRaises(ConfigurationError):
            # Default read pref "primary" can't be used with max staleness.
            MongoClient("mongodb://a/?maxStalenessSeconds=120")

        with self.assertRaises(ConfigurationError):
            # Read pref "primary" can't be used with max staleness.
            MongoClient("mongodb://a/?readPreference=primary&"
                        "maxStalenessSeconds=120")

        client = MongoClient("mongodb://host/?readPreference=secondary&"
                             "maxStalenessSeconds=120")
        self.assertEqual(120, client.read_preference.max_staleness)

        client = MongoClient("mongodb://a/?readPreference=secondary&"
                             "maxStalenessSeconds=1")
        self.assertEqual(1, client.read_preference.max_staleness)

        client = MongoClient("mongodb://a/?readPreference=secondary&"
                             "maxStalenessSeconds=1.5")
        self.assertAlmostEqual(1.5, client.read_preference.max_staleness)

        client = MongoClient("mongodb://a/?readPreference=secondary&"
                             "maxStalenessSeconds=-1")
        self.assertEqual(-1, client.read_preference.max_staleness)

        client = MongoClient(maxStalenessSeconds=-1, readPreference="nearest")
        self.assertEqual(-1, client.read_preference.max_staleness)

        with self.assertRaises(TypeError):
            # Prohibit None.
            MongoClient(maxStalenessSeconds=None, readPreference="nearest")

    def test_max_staleness_zero(self):
        # Zero is too small.
        with self.assertRaises(ValueError) as ctx:
            rs_or_single_client(maxStalenessSeconds=0,
                                readPreference="nearest")

        self.assertIn("must be greater than 0", str(ctx.exception))

        with warnings.catch_warnings(record=True) as ctx:
            warnings.simplefilter("always")
            MongoClient("mongodb://host/?maxStalenessSeconds=0"
                        "&readPreference=nearest")

            self.assertIn("must be greater than 0", str(ctx[0]))

    @client_context.require_version_min(3, 3, 6)  # SERVER-8858
    def test_last_write_date(self):
        # From max-staleness-tests.rst, "Parse lastWriteDate".
        client = rs_or_single_client(heartbeatFrequencyMS=500)
        client.pymongo_test.test.insert_one({})
        time.sleep(1)
        server = client._topology.select_server(writable_server_selector)
        last_write = server.description.last_write_date
        self.assertTrue(last_write)
        client.pymongo_test.test.insert_one({})
        time.sleep(1)
        server = client._topology.select_server(writable_server_selector)
        self.assertGreater(server.description.last_write_date, last_write)
        self.assertLess(server.description.last_write_date, last_write + 10)

    @client_context.require_version_max(3, 3)
    def test_last_write_date_absent(self):
        # From max-staleness-tests.rst, "Absent lastWriteDate".
        client = rs_or_single_client()
        sd = client._topology.select_server(writable_server_selector)
        self.assertIsNone(sd.description.last_write_date)

if __name__ == "__main__":
    unittest.main()
