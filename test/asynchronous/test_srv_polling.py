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

"""Run the SRV support tests."""
from __future__ import annotations

import asyncio
import sys
import time
from test.asynchronous.utils import flaky
from test.utils_shared import FunctionCallRecorder
from typing import Any

sys.path[0:0] = [""]

from test.asynchronous import AsyncPyMongoTestCase, client_knobs, unittest
from test.asynchronous.utils import async_wait_until

import pymongo
from pymongo import common
from pymongo.asynchronous.srv_resolver import _have_dnspython
from pymongo.errors import ConfigurationError

_IS_SYNC = False

WAIT_TIME = 0.1


class SrvPollingKnobs:
    def __init__(
        self,
        ttl_time=None,
        min_srv_rescan_interval=None,
        nodelist_callback=None,
        count_resolver_calls=False,
    ):
        self.ttl_time = ttl_time
        self.min_srv_rescan_interval = min_srv_rescan_interval
        self.nodelist_callback = nodelist_callback
        self.count_resolver_calls = count_resolver_calls

        self.old_min_srv_rescan_interval = None
        self.old_dns_resolver_response = None

    def enable(self):
        self.old_min_srv_rescan_interval = common.MIN_SRV_RESCAN_INTERVAL
        self.old_dns_resolver_response = (
            pymongo.asynchronous.srv_resolver._SrvResolver.get_hosts_and_min_ttl
        )

        if self.min_srv_rescan_interval is not None:
            common.MIN_SRV_RESCAN_INTERVAL = self.min_srv_rescan_interval

        async def mock_get_hosts_and_min_ttl(resolver, *args):
            assert self.old_dns_resolver_response is not None
            nodes, ttl = await self.old_dns_resolver_response(resolver)
            if self.nodelist_callback is not None:
                nodes = self.nodelist_callback()
            if self.ttl_time is not None:
                ttl = self.ttl_time
            return nodes, ttl

        patch_func: Any
        if self.count_resolver_calls:
            patch_func = FunctionCallRecorder(mock_get_hosts_and_min_ttl)
        else:
            patch_func = mock_get_hosts_and_min_ttl

        pymongo.asynchronous.srv_resolver._SrvResolver.get_hosts_and_min_ttl = patch_func  # type: ignore

    def __enter__(self):
        self.enable()

    def disable(self):
        common.MIN_SRV_RESCAN_INTERVAL = self.old_min_srv_rescan_interval  # type: ignore
        pymongo.asynchronous.srv_resolver._SrvResolver.get_hosts_and_min_ttl = (  # type: ignore
            self.old_dns_resolver_response
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()


class TestSrvPolling(AsyncPyMongoTestCase):
    BASE_SRV_RESPONSE = [
        ("localhost.test.build.10gen.cc", 27017),
        ("localhost.test.build.10gen.cc", 27018),
    ]

    CONNECTION_STRING = "mongodb+srv://test1.test.build.10gen.cc"

    async def asyncSetUp(self):
        # Patch timeouts to ensure short rescan SRV interval.
        self.client_knobs = client_knobs(
            heartbeat_frequency=WAIT_TIME,
            min_heartbeat_interval=WAIT_TIME,
            events_queue_frequency=WAIT_TIME,
        )
        self.client_knobs.enable()

    async def asyncTearDown(self):
        self.client_knobs.disable()

    def get_nodelist(self, client):
        return client._topology.description.server_descriptions().keys()

    async def assert_nodelist_change(self, expected_nodelist, client, timeout=(100 * WAIT_TIME)):
        """Check if the client._topology eventually sees all nodes in the
        expected_nodelist.
        """

        def predicate():
            nodelist = self.get_nodelist(client)
            if set(expected_nodelist) == set(nodelist):
                return True
            return False

        await async_wait_until(predicate, "see expected nodelist", timeout=timeout)

    async def assert_nodelist_nochange(self, expected_nodelist, client, timeout=(100 * WAIT_TIME)):
        """Check if the client._topology ever deviates from seeing all nodes
        in the expected_nodelist. Consistency is checked after sleeping for
        (WAIT_TIME * 10) seconds. Also check that the resolver is called at
        least once.
        """

        def predicate():
            if set(expected_nodelist) == set(self.get_nodelist(client)):
                return (
                    pymongo.asynchronous.srv_resolver._SrvResolver.get_hosts_and_min_ttl.call_count
                    >= 1
                )
            return False

        await async_wait_until(predicate, "Node list equals expected nodelist", timeout=timeout)

        nodelist = self.get_nodelist(client)
        if set(expected_nodelist) != set(nodelist):
            msg = "Client nodelist %s changed unexpectedly (expected %s)"
            raise self.fail(msg % (nodelist, expected_nodelist))
        self.assertGreaterEqual(
            pymongo.asynchronous.srv_resolver._SrvResolver.get_hosts_and_min_ttl.call_count,  # type: ignore
            1,
            "resolver was never called",
        )
        return True

    async def run_scenario(self, dns_response, expect_change):
        self.assertEqual(_have_dnspython(), True)
        if callable(dns_response):
            dns_resolver_response = dns_response
        else:

            def dns_resolver_response():
                return dns_response

        if expect_change:
            assertion_method = self.assert_nodelist_change
            count_resolver_calls = False
            expected_response = dns_response
        else:
            assertion_method = self.assert_nodelist_nochange
            count_resolver_calls = True
            expected_response = self.BASE_SRV_RESPONSE

        # Patch timeouts to ensure short test running times.
        with SrvPollingKnobs(ttl_time=WAIT_TIME, min_srv_rescan_interval=WAIT_TIME):
            client = self.simple_client(self.CONNECTION_STRING)
            await client.aconnect()
            await self.assert_nodelist_change(self.BASE_SRV_RESPONSE, client)
            # Patch list of hosts returned by DNS query.
            with SrvPollingKnobs(
                nodelist_callback=dns_resolver_response, count_resolver_calls=count_resolver_calls
            ):
                await assertion_method(expected_response, client)

            # Close the client early to avoid affecting the next scenario run.
            await client.close()

    async def test_addition(self):
        response = self.BASE_SRV_RESPONSE[:]
        response.append(("localhost.test.build.10gen.cc", 27019))
        await self.run_scenario(response, True)

    async def test_removal(self):
        response = self.BASE_SRV_RESPONSE[:]
        response.remove(("localhost.test.build.10gen.cc", 27018))
        await self.run_scenario(response, True)

    async def test_replace_one(self):
        response = self.BASE_SRV_RESPONSE[:]
        response.remove(("localhost.test.build.10gen.cc", 27018))
        response.append(("localhost.test.build.10gen.cc", 27019))
        await self.run_scenario(response, True)

    async def test_replace_both_with_one(self):
        response = [("localhost.test.build.10gen.cc", 27019)]
        await self.run_scenario(response, True)

    async def test_replace_both_with_two(self):
        response = [
            ("localhost.test.build.10gen.cc", 27019),
            ("localhost.test.build.10gen.cc", 27020),
        ]
        await self.run_scenario(response, True)

    async def test_dns_failures(self):
        from dns import exception

        for exc in (exception.FormError, exception.TooBig, exception.Timeout):

            def response_callback(*args):
                raise exc("DNS Failure!")

            await self.run_scenario(response_callback, False)

    @flaky(reason="PYTHON-5500", max_runs=3)
    async def test_dns_failures_logging(self):
        from dns import exception

        with self.assertLogs("pymongo.topology", level="DEBUG") as cm:

            def response_callback(*args):
                raise exception.Timeout("DNS Failure!")

            await self.run_scenario(response_callback, False)

        srv_failure_logs = [r for r in cm.records if "SRV monitor check failed" in r.getMessage()]
        self.assertEqual(len(srv_failure_logs), 1)

    async def test_dns_record_lookup_empty(self):
        response: list = []
        await self.run_scenario(response, False)

    async def _test_recover_from_initial(self, initial_callback):
        # Construct a valid final response callback distinct from base.
        response_final = self.BASE_SRV_RESPONSE[:]
        response_final.pop()

        def final_callback():
            return response_final

        with SrvPollingKnobs(
            ttl_time=WAIT_TIME,
            min_srv_rescan_interval=WAIT_TIME,
            nodelist_callback=initial_callback,
            count_resolver_calls=True,
        ):
            # Client uses unpatched method to get initial nodelist
            client = self.simple_client(self.CONNECTION_STRING)
            await client.aconnect()
            # Invalid DNS resolver response should not change nodelist.
            await self.assert_nodelist_nochange(self.BASE_SRV_RESPONSE, client)

        with SrvPollingKnobs(
            ttl_time=WAIT_TIME, min_srv_rescan_interval=WAIT_TIME, nodelist_callback=final_callback
        ):
            # Nodelist should reflect new valid DNS resolver response.
            await self.assert_nodelist_change(response_final, client)

    @flaky(reason="PYTHON-5315")
    async def test_recover_from_initially_empty_seedlist(self):
        def empty_seedlist():
            return []

        await self._test_recover_from_initial(empty_seedlist)

    @flaky(reason="PYTHON-5315")
    async def test_recover_from_initially_erroring_seedlist(self):
        def erroring_seedlist():
            raise ConfigurationError

        await self._test_recover_from_initial(erroring_seedlist)

    async def test_10_all_dns_selected(self):
        response = [
            ("localhost.test.build.10gen.cc", 27017),
            ("localhost.test.build.10gen.cc", 27019),
            ("localhost.test.build.10gen.cc", 27020),
        ]

        def nodelist_callback():
            return response

        with SrvPollingKnobs(ttl_time=WAIT_TIME, min_srv_rescan_interval=WAIT_TIME):
            client = self.simple_client(self.CONNECTION_STRING, srvMaxHosts=0)
            await client.aconnect()
            with SrvPollingKnobs(nodelist_callback=nodelist_callback):
                await self.assert_nodelist_change(response, client)

    async def test_11_all_dns_selected(self):
        response = [
            ("localhost.test.build.10gen.cc", 27019),
            ("localhost.test.build.10gen.cc", 27020),
        ]

        def nodelist_callback():
            return response

        with SrvPollingKnobs(ttl_time=WAIT_TIME, min_srv_rescan_interval=WAIT_TIME):
            client = self.simple_client(self.CONNECTION_STRING, srvMaxHosts=2)
            await client.aconnect()
            with SrvPollingKnobs(nodelist_callback=nodelist_callback):
                await self.assert_nodelist_change(response, client)

    async def test_12_new_dns_randomly_selected(self):
        response = [
            ("localhost.test.build.10gen.cc", 27020),
            ("localhost.test.build.10gen.cc", 27019),
            ("localhost.test.build.10gen.cc", 27017),
        ]

        def nodelist_callback():
            return response

        with SrvPollingKnobs(ttl_time=WAIT_TIME, min_srv_rescan_interval=WAIT_TIME):
            client = self.simple_client(self.CONNECTION_STRING, srvMaxHosts=2)
            await client.aconnect()
            with SrvPollingKnobs(nodelist_callback=nodelist_callback):
                await asyncio.sleep(2 * common.MIN_SRV_RESCAN_INTERVAL)
                final_topology = set(client.topology_description.server_descriptions())
                self.assertIn(("localhost.test.build.10gen.cc", 27017), final_topology)
                self.assertEqual(len(final_topology), 2)

    async def test_does_not_flipflop(self):
        with SrvPollingKnobs(ttl_time=WAIT_TIME, min_srv_rescan_interval=WAIT_TIME):
            client = self.simple_client(self.CONNECTION_STRING, srvMaxHosts=1)
            await client.aconnect()
            old = set(client.topology_description.server_descriptions())
            await asyncio.sleep(4 * WAIT_TIME)
            new = set(client.topology_description.server_descriptions())
            self.assertSetEqual(old, new)

    async def test_srv_service_name(self):
        # Construct a valid final response callback distinct from base.
        response = [
            ("localhost.test.build.10gen.cc.", 27019),
            ("localhost.test.build.10gen.cc.", 27020),
        ]

        def nodelist_callback():
            return response

        with SrvPollingKnobs(ttl_time=WAIT_TIME, min_srv_rescan_interval=WAIT_TIME):
            client = self.simple_client(
                "mongodb+srv://test22.test.build.10gen.cc/?srvServiceName=customname"
            )
            await client.aconnect()
            with SrvPollingKnobs(nodelist_callback=nodelist_callback):
                await self.assert_nodelist_change(response, client)

    async def test_srv_waits_to_poll(self):
        modified = [("localhost.test.build.10gen.cc", 27019)]

        def resolver_response():
            return modified

        with SrvPollingKnobs(
            ttl_time=WAIT_TIME,
            min_srv_rescan_interval=WAIT_TIME,
            nodelist_callback=resolver_response,
        ):
            client = self.simple_client(self.CONNECTION_STRING)
            await client.aconnect()
            with self.assertRaises(AssertionError):
                await self.assert_nodelist_change(modified, client, timeout=WAIT_TIME / 2)

    def test_import_dns_resolver(self):
        # Regression test for PYTHON-4407
        import dns.resolver

        self.assertTrue(hasattr(dns.resolver, "resolve") or hasattr(dns.resolver, "query"))


if __name__ == "__main__":
    unittest.main()
