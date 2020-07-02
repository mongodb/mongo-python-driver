# Copyright 2019-present MongoDB, Inc.
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
        exc = NotMasterError("not master test", {"ok": 0, "errmsg":"error"})
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except NotMasterError:
            self.assertIn("full error", traceback.format_exc())

    def test_operation_failure(self):
        exc = OperationFailure("operation failure test", {"ok": 0, "errmsg":"error"})
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except OperationFailure:
            self.assertIn("full error", traceback.format_exc())


if __name__ == "__main__":
    unittest.main()