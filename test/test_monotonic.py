# Copyright 2018-present MongoDB, Inc.
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

"""Test the monotonic module."""

import sys

sys.path[0:0] = [""]

from pymongo.monotonic import time as pymongo_time

from test import unittest


class TestMonotonic(unittest.TestCase):
    def test_monotonic_time(self):
        try:
            from monotonic import monotonic
            self.assertIs(monotonic, pymongo_time)
        except ImportError:
            if sys.version_info[:2] >= (3, 3):
                from time import monotonic
                self.assertIs(monotonic, pymongo_time)
            else:
                from time import time
                self.assertIs(time, pymongo_time)


if __name__ == "__main__":
    unittest.main()
