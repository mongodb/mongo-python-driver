# Copyright 2014 MongoDB, Inc.
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

"""Test the monitor module."""

import gc
import sys
import time

sys.path[0:0] = [""]

from pymongo.monitor import MONITORS
from test import unittest, port, host, IntegrationTest
from test.utils import single_client, wait_until, one


class TestMonitor(IntegrationTest):
    def test_atexit_hook(self):
        # Weakrefs to currently running Monitor instances.
        prior_monitors = MONITORS.copy()
        client = single_client(host, port)
        wait_until(lambda: MONITORS - prior_monitors,
                   'register new monitor')

        # Just one new monitor should have been registered.
        new_monitor_refs = MONITORS - prior_monitors
        self.assertEqual(1, len(new_monitor_refs))
        monitor_ref = one(new_monitor_refs)
        del client

        start = time.time()
        while time.time() - start < 30:
            gc.collect()
            if monitor_ref not in MONITORS:
                # New monitor was unregistered.
                break

            time.sleep(0.1)
        else:
            self.fail("Didn't ever unregister monitor")


if __name__ == "__main__":
    unittest.main()
