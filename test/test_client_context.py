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

import os
import sys

sys.path[0:0] = [""]

from test import client_context, SkipTest, unittest


class TestClientContext(unittest.TestCase):
    def test_must_connect(self):
        if 'PYMONGO_MUST_CONNECT' not in os.environ:
            raise SkipTest('PYMONGO_MUST_CONNECT is not set')

        self.assertTrue(client_context.connected,
                        'client context must be connected when '
                        'PYMONGO_MUST_CONNECT is set. Failed attempts:\n%s' %
                        (client_context.connection_attempt_info(),))


if __name__ == "__main__":
    unittest.main()
