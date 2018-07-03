# Copyright 2017-present MongoDB, Inc.
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

"""Test results module."""

import sys

sys.path[0:0] = [""]

from pymongo.errors import InvalidOperation
from pymongo.results import (BulkWriteResult,
                             DeleteResult,
                             InsertManyResult,
                             InsertOneResult,
                             UpdateResult)

from test import unittest


class TestResults(unittest.TestCase):
    def repr_test(self, cls, result_arg):
        for acknowledged in (True, False):
            result = cls(result_arg, acknowledged)
            expected_repr = '%s(%r, %r)' % (cls.__name__, result_arg,
                                            acknowledged)
            self.assertEqual(acknowledged, result.acknowledged)
            self.assertEqual(expected_repr, repr(result))

    def test_bulk_write_result(self):
        raw_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 1,
            "nUpserted": 2,
            "nMatched": 2,
            "nModified": 2,
            "nRemoved": 2,
            "upserted": [
                {"index": 5, "_id": 1},
                {"index": 9, "_id": 2},
            ],
        }
        self.repr_test(BulkWriteResult, raw_result)

        result = BulkWriteResult(raw_result, True)
        self.assertEqual(raw_result, result.bulk_api_result)
        self.assertEqual(raw_result["nInserted"], result.inserted_count)
        self.assertEqual(raw_result["nMatched"], result.matched_count)
        self.assertEqual(raw_result["nModified"], result.modified_count)
        self.assertEqual(raw_result["nRemoved"], result.deleted_count)
        self.assertEqual(raw_result["nUpserted"], result.upserted_count)
        self.assertEqual({5: 1, 9: 2}, result.upserted_ids)

        result = BulkWriteResult(raw_result, False)
        self.assertEqual(raw_result, result.bulk_api_result)
        error_msg = 'A value for .* is not available when'
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.inserted_count
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.matched_count
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.modified_count
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.deleted_count
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.upserted_count
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.upserted_ids

    def test_delete_result(self):
        raw_result = {"n": 5}
        self.repr_test(DeleteResult, {"n": 0})

        result = DeleteResult(raw_result, True)
        self.assertEqual(raw_result, result.raw_result)
        self.assertEqual(raw_result["n"], result.deleted_count)

        result = DeleteResult(raw_result, False)
        self.assertEqual(raw_result, result.raw_result)
        error_msg = 'A value for .* is not available when'
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.deleted_count

    def test_insert_many_result(self):
        inserted_ids = [1, 2, 3]
        self.repr_test(InsertManyResult, inserted_ids)

        for acknowledged in (True, False):
            result = InsertManyResult(inserted_ids, acknowledged)
            self.assertEqual(inserted_ids, result.inserted_ids)

    def test_insert_one_result(self):
        self.repr_test(InsertOneResult, 0)

        for acknowledged in (True, False):
            result = InsertOneResult(0, acknowledged)
            self.assertEqual(0, result.inserted_id)

    def test_update_result(self):
        raw_result = {
            "n": 1,
            "nModified": 1,
            "upserted": None,
        }
        self.repr_test(UpdateResult, raw_result)

        result = UpdateResult(raw_result, True)
        self.assertEqual(raw_result, result.raw_result)
        self.assertEqual(raw_result["n"], result.matched_count)
        self.assertEqual(raw_result["nModified"], result.modified_count)
        self.assertEqual(raw_result["upserted"], result.upserted_id)

        result = UpdateResult(raw_result, False)
        self.assertEqual(raw_result, result.raw_result)
        error_msg = 'A value for .* is not available when'
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.matched_count
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.modified_count
        with self.assertRaisesRegex(InvalidOperation, error_msg):
            result.upserted_id
