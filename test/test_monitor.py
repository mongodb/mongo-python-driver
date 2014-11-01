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
from functools import partial

sys.path[0:0] = [""]

from pymongo.monitor import MONITORS
from test import unittest, port, host, IntegrationTest
from test.utils import single_client, one, connected, wait_until


def find_monitor_ref(monitor):
    for ref in MONITORS.copy():
        if ref() is monitor:
            return ref

    return None


def unregistered(ref):
    gc.collect()
    return ref not in MONITORS


class TestMonitor(IntegrationTest):
    def test_atexit_hook(self):
        client = single_client(host, port)
        monitor = one(client._topology._servers.values())._monitor
        connected(client)

        # The client registers a weakref to the monitor.
        ref = wait_until(partial(find_monitor_ref, monitor),
                         'register monitor')

        del monitor
        del client

        wait_until(partial(unregistered, ref), 'unregister monitor')


if __name__ == "__main__":
    unittest.main()
