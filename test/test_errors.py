# Copyright 2020-present MongoDB, Inc.
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
from __future__ import annotations

import pickle
import sys
import traceback

sys.path[0:0] = [""]

from test import PyMongoTestCase, unittest

from pymongo.errors import (
    BulkWriteError,
    EncryptionError,
    NotPrimaryError,
    OperationFailure,
)


class TestErrors(PyMongoTestCase):
    def test_not_primary_error(self):
        exc = NotPrimaryError("not primary test", {"errmsg": "error"})
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except NotPrimaryError:
            self.assertIn("full error", traceback.format_exc())

    def test_operation_failure(self):
        exc = OperationFailure("operation failure test", 10, {"errmsg": "error"})
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except OperationFailure:
            self.assertIn("full error", traceback.format_exc())

    def _test_unicode_strs(self, exc):
        if sys.implementation.name == "pypy" and sys.implementation.version < (7, 3, 7):
            # PyPy used to display unicode in repr differently.
            self.assertEqual(
                "unicode \U0001f40d, full error: {'errmsg': 'unicode \\U0001f40d'}", str(exc)
            )
        else:
            self.assertEqual(
                "unicode \U0001f40d, full error: {'errmsg': 'unicode \U0001f40d'}", str(exc)
            )
        try:
            raise exc
        except Exception:
            self.assertIn("full error", traceback.format_exc())

    def test_unicode_strs_operation_failure(self):
        exc = OperationFailure("unicode \U0001f40d", 10, {"errmsg": "unicode \U0001f40d"})
        self._test_unicode_strs(exc)

    def test_unicode_strs_not_primary_error(self):
        exc = NotPrimaryError("unicode \U0001f40d", {"errmsg": "unicode \U0001f40d"})
        self._test_unicode_strs(exc)

    def assertPyMongoErrorEqual(self, exc1, exc2):
        self.assertEqual(exc1._message, exc2._message)
        self.assertEqual(exc1._error_labels, exc2._error_labels)
        self.assertEqual(exc1.args, exc2.args)
        self.assertEqual(str(exc1), str(exc2))

    def assertOperationFailureEqual(self, exc1, exc2):
        self.assertPyMongoErrorEqual(exc1, exc2)
        self.assertEqual(exc1.code, exc2.code)
        self.assertEqual(exc1.details, exc2.details)
        self.assertEqual(exc1._max_wire_version, exc2._max_wire_version)

    def test_pickle_NotPrimaryError(self):
        exc = NotPrimaryError("not primary test", {"errmsg": "error"})
        self.assertPyMongoErrorEqual(exc, pickle.loads(pickle.dumps(exc)))

    def test_pickle_OperationFailure(self):
        exc = OperationFailure("error", code=5, details={}, max_wire_version=7)
        self.assertOperationFailureEqual(exc, pickle.loads(pickle.dumps(exc)))

    def test_pickle_BulkWriteError(self):
        exc = BulkWriteError({})
        self.assertOperationFailureEqual(exc, pickle.loads(pickle.dumps(exc)))
        self.assertIn("batch op errors occurred", str(exc))

    def test_pickle_EncryptionError(self):
        cause = OperationFailure("error", code=5, details={}, max_wire_version=7)
        exc = EncryptionError(cause)
        exc2 = pickle.loads(pickle.dumps(exc))
        self.assertPyMongoErrorEqual(exc, exc2)
        self.assertOperationFailureEqual(cause, exc2.cause)


if __name__ == "__main__":
    unittest.main()
