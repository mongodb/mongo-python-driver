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

import sys
import traceback

sys.path[0:0] = [""]

from pymongo.errors import (NotMasterError,
                            OperationFailure)
from test import (PyMongoTestCase,
                  unittest)


class TestErrors(PyMongoTestCase):
    def test_not_master_error(self):
        exc = NotMasterError("not master test", {"errmsg": "error"})
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except NotMasterError:
            self.assertIn("full error", traceback.format_exc())

    def test_operation_failure(self):
        exc = OperationFailure("operation failure test", 10,
                               {"errmsg": "error"})
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except OperationFailure:
            self.assertIn("full error", traceback.format_exc())

    def _test_unicode_strs(self, exc):
        if sys.version_info[0] == 2:
            self.assertEqual("unicode \xf0\x9f\x90\x8d, full error: {"
                             "'errmsg': u'unicode \\U0001f40d'}", str(exc))
        else:
            self.assertEqual("unicode \U0001f40d, full error: {"
                             "'errmsg': 'unicode \U0001f40d'}", str(exc))
        try:
            raise exc
        except Exception:
            self.assertIn("full error", traceback.format_exc())

    def test_unicode_strs_operation_failure(self):
        exc = OperationFailure(u'unicode \U0001f40d', 10,
                               {"errmsg": u'unicode \U0001f40d'})
        self._test_unicode_strs(exc)

    def test_unicode_strs_not_master_error(self):
        exc = NotMasterError(u'unicode \U0001f40d',
                             {"errmsg": u'unicode \U0001f40d'})
        self._test_unicode_strs(exc)



if __name__ == "__main__":
    unittest.main()
