# Copyright 2013 10gen, Inc.
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

"""Test built in connection-pooling with lots of threads."""

import unittest
import sys

sys.path[0:0] = [""]

from test.test_pooling_base import _TestMaxPoolSize


class TestPoolWithLotsOfThreads(_TestMaxPoolSize, unittest.TestCase):
    use_greenlets = False

    def test_max_pool_size_with_leaked_request_super_massive(self):
        # Like test_max_pool_size_with_leaked_request_massive but even more
        # threads. Tests that socket reclamation works under high load,
        # especially in Python <= 2.7.0. You may need to raise ulimit.
        # See http://bugs.python.org/issue1868.
        nthreads = 1000
        self._test_max_pool_size(
            2, 1, max_pool_size=2 * nthreads, nthreads=nthreads)
