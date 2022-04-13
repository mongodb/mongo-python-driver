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

"""Run the unit tests for WriteConcern."""

import collections
import unittest

from pymongo.errors import ConfigurationError
from pymongo.write_concern import WriteConcern


class TestWriteConcern(unittest.TestCase):
    def test_invalid(self):
        # Can't use fsync and j options together
        self.assertRaises(ConfigurationError, WriteConcern, j=True, fsync=True)
        # Can't use w=0 and j options together
        self.assertRaises(ConfigurationError, WriteConcern, w=0, j=True)

    def test_equality(self):
        concern = WriteConcern(j=True, wtimeout=3000)
        self.assertEqual(concern, WriteConcern(j=True, wtimeout=3000))
        self.assertNotEqual(concern, WriteConcern())

    def test_equality_to_none(self):
        concern = WriteConcern()
        self.assertNotEqual(concern, None)
        # Explicitly use the != operator.
        self.assertTrue(concern != None)  # noqa

    def test_equality_compatible_type(self):
        class _FakeWriteConcern(object):
            def __init__(self, **document):
                self.document = document

            def __eq__(self, other):
                try:
                    return self.document == other.document
                except AttributeError:
                    return NotImplemented

            def __ne__(self, other):
                try:
                    return self.document != other.document
                except AttributeError:
                    return NotImplemented

        self.assertEqual(WriteConcern(j=True), _FakeWriteConcern(j=True))
        self.assertEqual(_FakeWriteConcern(j=True), WriteConcern(j=True))
        self.assertEqual(WriteConcern(j=True), _FakeWriteConcern(j=True))
        self.assertEqual(WriteConcern(wtimeout=42), _FakeWriteConcern(wtimeout=42))
        self.assertNotEqual(WriteConcern(wtimeout=42), _FakeWriteConcern(wtimeout=2000))

    def test_equality_incompatible_type(self):
        _fake_type = collections.namedtuple("NotAWriteConcern", ["document"])  # type: ignore
        self.assertNotEqual(WriteConcern(j=True), _fake_type({"j": True}))


if __name__ == "__main__":
    unittest.main()
