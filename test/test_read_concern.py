# Copyright 2015 MongoDB, Inc.
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

"""Test the read_concern module."""

import pymongo

from bson.son import SON
from pymongo import monitoring
from pymongo.errors import ConfigurationError
from pymongo.read_concern import ReadConcern

from test import client_context, IntegrationTest, pair
from test.utils import EventListener


class TestReadConcern(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestReadConcern, cls).setUpClass()
        cls.listener = EventListener()
        cls.db = client_context.client.pymongo_test
        monitoring.subscribe(cls.listener)

    def tearDown(self):
        self.db.coll.drop()
        self.listener.results.clear()

    def test_read_concern(self):
        rc = ReadConcern()
        self.assertIsNone(rc.level)
        self.assertTrue(rc.ok_for_legacy)

        rc = ReadConcern('majority')
        self.assertEqual('majority', rc.level)
        self.assertFalse(rc.ok_for_legacy)

        rc = ReadConcern('local')
        self.assertEqual('local', rc.level)
        self.assertTrue(rc.ok_for_legacy)

        self.assertRaises(TypeError, ReadConcern, 42)

    def test_read_concern_uri(self):
        uri = 'mongodb://%s/?readConcernLevel=majority' % (pair,)
        client = pymongo.MongoClient(uri)
        self.assertEqual(ReadConcern('majority'), client.read_concern)

    @client_context.require_version_max(3, 1)
    def test_invalid_read_concern(self):
        coll = self.db.get_collection(
            'coll', read_concern=ReadConcern('majority'))
        self.assertRaisesRegexp(
            ConfigurationError,
            'read concern level of majority is not valid '
            'with a max wire version of [0-3]',
            coll.count)

    @client_context.require_version_min(3, 1, 9, -1)
    def test_find_command(self):
        # readConcern not sent in command if not specified.
        coll = self.db.coll
        tuple(coll.find({'field': 'value'}))
        self.assertNotIn('readConcern',
                         self.listener.results['started'][0].command)

        self.listener.results.clear()

        # Explicitly set readConcern to 'local'.
        coll = self.db.get_collection('coll', read_concern=ReadConcern('local'))
        tuple(coll.find({'field': 'value'}))
        self.assertEqual(
            SON([('find', 'coll'),
                 ('filter', {'field': 'value'}),
                 ('readConcern', {'level': 'local'})]),
            self.listener.results['started'][0].command)

    @client_context.require_version_min(3, 1, 9, -1)
    def test_command_cursor(self):
        # readConcern not sent in command if not specified.
        coll = self.db.coll
        tuple(coll.aggregate([{'$match': {'field': 'value'}}]))
        self.assertNotIn('readConcern',
                         self.listener.results['started'][0].command)

        self.listener.results.clear()

        # Explicitly set readConcern to 'local'.
        coll = self.db.get_collection('coll', read_concern=ReadConcern('local'))
        tuple(coll.aggregate([{'$match': {'field': 'value'}}]))
        self.assertEqual(
            {'level': 'local'},
            self.listener.results['started'][0].command['readConcern'])
