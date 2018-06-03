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

"""Run the read and write concern tests."""

import json
import os
import sys
import warnings

sys.path[0:0] = [""]

from pymongo import DESCENDING
from pymongo.errors import (BulkWriteError,
                            ConfigurationError,
                            WTimeoutError,
                            WriteConcernError)
from pymongo.mongo_client import MongoClient
from pymongo.operations import IndexModel, InsertOne
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from test import client_context, unittest
from test.utils import (IMPOSSIBLE_WRITE_CONCERN,
                        EventListener,
                        disable_replication,
                        enable_replication,
                        rs_or_single_client)


_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'read_write_concern')


class TestReadWriteConcernSpec(unittest.TestCase):

    @client_context.require_connection
    def test_omit_default_read_write_concern(self):
        listener = EventListener()
        # Client with default readConcern and writeConcern
        client = rs_or_single_client(event_listeners=[listener])
        collection = client.pymongo_test.collection
        # Prepare for tests of find() and aggregate().
        collection.insert_many([{} for _ in range(10)])
        self.addCleanup(collection.drop)
        self.addCleanup(client.pymongo_test.collection2.drop)
        # Commands MUST NOT send the default read/write concern to the server.

        def rename_and_drop():
            # Ensure collection exists.
            collection.insert_one({})
            collection.rename('collection2')
            client.pymongo_test.collection2.drop()

        def insert_command_default_write_concern():
            collection.database.command(
                'insert', 'collection', documents=[{}],
                write_concern=WriteConcern())

        ops = [
            ('aggregate', lambda: list(collection.aggregate([]))),
            ('find', lambda: list(collection.find())),
            ('insert_one', lambda: collection.insert_one({})),
            ('update_one',
             lambda: collection.update_one({}, {'$set': {'x': 1}})),
            ('update_many',
             lambda: collection.update_many({}, {'$set': {'x': 1}})),
            ('delete_one', lambda: collection.delete_one({})),
            ('delete_many', lambda: collection.delete_many({})),
            ('bulk_write', lambda: collection.bulk_write([InsertOne({})])),
            ('rename_and_drop', rename_and_drop),
            ('command', insert_command_default_write_concern)
        ]

        for name, f in ops:
            listener.results.clear()
            f()

            self.assertGreaterEqual(len(listener.results['started']), 1)
            for i, event in enumerate(listener.results['started']):
                self.assertNotIn(
                    'readConcern', event.command,
                    "%s sent default readConcern with %s" % (
                        name, event.command_name))
                self.assertNotIn(
                    'writeConcern', event.command,
                    "%s sent default writeConcern with %s" % (
                        name, event.command_name))

    def assertWriteOpsRaise(self, write_concern, expected_exception):
        client = rs_or_single_client(**write_concern.document)
        db = client.get_database('pymongo_test')
        coll = db.test

        def insert_command():
            coll.database.command(
                'insert', 'new_collection', documents=[{}],
                writeConcern=write_concern.document,
                parse_write_concern_error=True)

        ops = [
            ('insert_one', lambda: coll.insert_one({})),
            ('insert_many', lambda: coll.insert_many([{}, {}])),
            ('update_one', lambda: coll.update_one({}, {'$set': {'x': 1}})),
            ('update_many', lambda: coll.update_many({}, {'$set': {'x': 1}})),
            ('delete_one', lambda: coll.delete_one({})),
            ('delete_many', lambda: coll.delete_many({})),
            ('bulk_write', lambda: coll.bulk_write([InsertOne({})])),
            ('command', insert_command),
        ]
        ops_require_34 = [
            ('aggregate', lambda: coll.aggregate([{'$out': 'out'}])),
            ('create_index', lambda: coll.create_index([('a', DESCENDING)])),
            ('create_indexes', lambda: coll.create_indexes([IndexModel('b')])),
            ('drop_index', lambda: coll.drop_index([('a', DESCENDING)])),
            ('create', lambda: db.create_collection('new')),
            ('rename', lambda: coll.rename('new')),
            ('drop', lambda: db.new.drop()),
        ]
        if client_context.version > (3, 4):
            ops.extend(ops_require_34)
            # SERVER-34776: Drop database does not respect wtimeout in 4.0.
            if client_context.version <= (3, 6):
                ops.append(('drop_database', lambda: client.drop_database(db)))

        for name, f in ops:
            # Ensure insert_many and bulk_write still raise BulkWriteError.
            if name in ('insert_many', 'bulk_write'):
                expected = BulkWriteError
            else:
                expected = expected_exception
            with self.assertRaises(expected, msg=name) as cm:
                f()
            if expected == BulkWriteError:
                bulk_result = cm.exception.details
                wc_errors = bulk_result['writeConcernErrors']
                self.assertTrue(wc_errors)

    @client_context.require_replica_set
    def test_raise_write_concern_error(self):
        self.addCleanup(client_context.client.drop_database, 'pymongo_test')
        self.assertWriteOpsRaise(
            WriteConcern(w=client_context.w+1, wtimeout=1), WriteConcernError)

    # MongoDB 3.2 introduced the stopReplProducer failpoint.
    @client_context.require_version_min(3, 2)
    @client_context.require_secondaries_count(1)
    @client_context.require_test_commands
    def test_raise_wtimeout(self):
        self.addCleanup(client_context.client.drop_database, 'pymongo_test')
        self.addCleanup(enable_replication, client_context.client)
        # Disable replication to guarantee a wtimeout error.
        disable_replication(client_context.client)
        self.assertWriteOpsRaise(WriteConcern(w=client_context.w, wtimeout=1),
                                 WTimeoutError)


def normalize_write_concern(concern):
    result = {}
    for key in concern:
        if key.lower() == 'wtimeoutms':
            result['wtimeout'] = concern[key]
        elif key == 'journal':
            result['j'] = concern[key]
        else:
            result[key] = concern[key]
    return result


def create_connection_string_test(test_case):

    def run_test(self):
        uri = test_case['uri']
        valid = test_case['valid']
        warning = test_case['warning']

        if not valid:
            if warning is False:
                self.assertRaises(
                    (ConfigurationError, ValueError),
                    MongoClient,
                    uri,
                    connect=False)
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter('error', UserWarning)
                    self.assertRaises(
                        UserWarning,
                        MongoClient,
                        uri,
                        connect=False)
        else:
            client = MongoClient(uri, connect=False)
            if 'writeConcern' in test_case:
                document = client.write_concern.document
                self.assertEqual(
                    document,
                    normalize_write_concern(test_case['writeConcern']))
            if 'readConcern' in test_case:
                document = client.read_concern.document
                self.assertEqual(document, test_case['readConcern'])

    return run_test


def create_document_test(test_case):

    def run_test(self):
        valid = test_case['valid']

        if 'writeConcern' in test_case:
            normalized = normalize_write_concern(test_case['writeConcern'])
            if not valid:
                self.assertRaises(
                    (ConfigurationError, ValueError),
                    WriteConcern,
                    **normalized)
            else:
                concern = WriteConcern(**normalized)
                self.assertEqual(
                    concern.document, test_case['writeConcernDocument'])
                self.assertEqual(
                    concern.acknowledged, test_case['isAcknowledged'])
                self.assertEqual(
                    concern.is_server_default, test_case['isServerDefault'])
        if 'readConcern' in test_case:
            # Any string for 'level' is equaly valid
            concern = ReadConcern(**test_case['readConcern'])
            self.assertEqual(concern.document, test_case['readConcernDocument'])
            self.assertEqual(
                not bool(concern.level), test_case['isServerDefault'])

    return run_test


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        if dirname == 'connection-string':
            create_test = create_connection_string_test
        else:
            create_test = create_document_test

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as test_stream:
                test_cases = json.load(test_stream)['tests']

            fname = os.path.splitext(filename)[0]
            for test_case in test_cases:
                new_test = create_test(test_case)
                test_name = 'test_%s_%s_%s' % (
                    dirname.replace('-', '_'),
                    fname.replace('-', '_'),
                    str(test_case['description'].lower().replace(' ', '_')))

                new_test.__name__ = test_name
                setattr(TestReadWriteConcernSpec, new_test.__name__, new_test)


create_tests()


if __name__ == '__main__':
    unittest.main()
