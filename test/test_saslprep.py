# Copyright 2016-present MongoDB, Inc.
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
import warnings

sys.path[0:0] = [""]

from pymongo.saslprep import saslprep
from test import unittest

class TestSASLprep(unittest.TestCase):

    def test_saslprep(self):
        try:
            import stringprep
        except ImportError:
            self.assertRaises(TypeError, saslprep, u"anything...")
            # Bytes strings are ignored.
            self.assertEqual(saslprep(b"user"), b"user")
        else:
            # Examples from RFC4013, Section 3.
            self.assertEqual(saslprep(u"I\u00ADX"), u"IX")
            self.assertEqual(saslprep(u"user"), u"user")
            self.assertEqual(saslprep(u"USER"), u"USER")
            self.assertEqual(saslprep(u"\u00AA"), u"a")
            self.assertEqual(saslprep(u"\u2168"), u"IX")
            self.assertRaises(ValueError, saslprep, u"\u0007")
            self.assertRaises(ValueError, saslprep, u"\u0627\u0031")

            # Bytes strings are ignored.
            self.assertEqual(saslprep(b"user"), b"user")
