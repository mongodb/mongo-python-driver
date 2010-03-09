# Copyright 2009 10gen, Inc.
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

import unittest
import sys
sys.path[0:0] = [""]

from pymongo.timestamp import Timestamp


class TestTimestamp(unittest.TestCase):

    def setUp(self):
        pass

    def test_timestamp(self):
        t = Timestamp(123, 456)
        self.assertEqual(t.time, 123)
        self.assertEqual(t.inc, 456)
        self.assert_(isinstance(t, Timestamp))

    def test_exceptions(self):
        self.assertRaises(TypeError, Timestamp)
        self.assertRaises(TypeError, Timestamp, None, 123)
        self.assertRaises(TypeError, Timestamp, 1.2, 123)
        self.assertRaises(TypeError, Timestamp, 123, None)
        self.assertRaises(TypeError, Timestamp, 123, 1.2)
        self.assertRaises(ValueError, Timestamp, 0, -1)
        self.assertRaises(ValueError, Timestamp, -1, 0)
        self.assert_(Timestamp(0, 0))

    def test_equality(self):
        t = Timestamp(1,1)
        self.assertNotEqual(t, Timestamp(0,1))
        self.assertNotEqual(t, Timestamp(1,0))
        self.assertEqual(t, Timestamp(1,1))

    def test_repr(self):
        t = Timestamp(0, 0)
        self.assertEqual(repr(t), "Timestamp(0, 0)")

if __name__ == "__main__":
    unittest.main()
