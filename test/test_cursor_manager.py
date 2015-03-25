# Copyright 2014-2015 MongoDB, Inc.
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

"""Test the cursor_manager module."""

import sys

sys.path[0:0] = [""]

from pymongo.cursor_manager import CursorManager
from pymongo.errors import CursorNotFound
from test import (client_context,
                  client_knobs,
                  unittest,
                  IntegrationTest,
                  SkipTest)
from test.utils import rs_or_single_client, wait_until


class TestCursorManager(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestCursorManager, cls).setUpClass()
        cls.collection = cls.db.test
        cls.collection.drop()

        # Ensure two batches.
        cls.collection.insert_many([{'_id': i} for i in range(200)])

    @classmethod
    def tearDownClass(cls):
        cls.collection.drop()

    def test_cursor_manager_validation(self):
        with self.assertRaises(TypeError):
            client_context.client.set_cursor_manager(1)

    def test_cursor_manager(self):
        if (client_context.is_mongos
                and not client_context.version.at_least(2, 4, 7)):
            # Old mongos sends incorrectly formatted error response when
            # cursor isn't found, see SERVER-9738.
            raise SkipTest("Can't test kill_cursors against old mongos")

        self.close_was_called = False

        test_case = self

        class CM(CursorManager):
            def __init__(self, client):
                super(CM, self).__init__(client)

            def close(self, cursor_id, address):
                test_case.close_was_called = True
                super(CM, self).close(cursor_id, address)

        with client_knobs(kill_cursor_frequency=0.01):
            client = rs_or_single_client(maxPoolSize=1)
            client.set_cursor_manager(CM)

            # Create a cursor on the same client so we're certain the getMore
            # is sent after the killCursors message.
            cursor = client.pymongo_test.test.find().batch_size(1)
            next(cursor)
            client.close_cursor(cursor.cursor_id)

            def raises_cursor_not_found():
                try:
                    next(cursor)
                    return False
                except CursorNotFound:
                    return True

            wait_until(raises_cursor_not_found, 'close cursor')
            self.assertTrue(self.close_was_called)


if __name__ == "__main__":
    unittest.main()
