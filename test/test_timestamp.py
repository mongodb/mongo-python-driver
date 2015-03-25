# Copyright 2009-2015 MongoDB, Inc.
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

"""Tests for the Timestamp class."""

import datetime
import sys
import copy
import pickle
sys.path[0:0] = [""]

from bson.timestamp import Timestamp
from bson.tz_util import utc
from test import unittest


class TestTimestamp(unittest.TestCase):
    def test_timestamp(self):
        t = Timestamp(123, 456)
        self.assertEqual(t.time, 123)
        self.assertEqual(t.inc, 456)
        self.assertTrue(isinstance(t, Timestamp))

    def test_datetime(self):
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)
        t = Timestamp(d, 0)
        self.assertEqual(1273017600, t.time)
        self.assertEqual(d, t.as_datetime())

    def test_datetime_copy_pickle(self):
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)
        t = Timestamp(d, 0)

        dc = copy.deepcopy(d)
        self.assertEqual(dc, t.as_datetime())

        for protocol in [0, 1, 2, -1]:
            pkl = pickle.dumps(d, protocol=protocol)
            dp = pickle.loads(pkl)
            self.assertEqual(dp, t.as_datetime())

    def test_exceptions(self):
        self.assertRaises(TypeError, Timestamp)
        self.assertRaises(TypeError, Timestamp, None, 123)
        self.assertRaises(TypeError, Timestamp, 1.2, 123)
        self.assertRaises(TypeError, Timestamp, 123, None)
        self.assertRaises(TypeError, Timestamp, 123, 1.2)
        self.assertRaises(ValueError, Timestamp, 0, -1)
        self.assertRaises(ValueError, Timestamp, -1, 0)
        self.assertTrue(Timestamp(0, 0))

    def test_equality(self):
        t = Timestamp(1, 1)
        self.assertNotEqual(t, Timestamp(0, 1))
        self.assertNotEqual(t, Timestamp(1, 0))
        self.assertEqual(t, Timestamp(1, 1))

        # Explicitly test inequality
        self.assertFalse(t != Timestamp(1, 1))

    def test_repr(self):
        t = Timestamp(0, 0)
        self.assertEqual(repr(t), "Timestamp(0, 0)")

if __name__ == "__main__":
    unittest.main()
