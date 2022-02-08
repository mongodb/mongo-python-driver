# Copyright 2015-present MongoDB, Inc.
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

"""Run the command monitoring unified format spec tests."""

import os
import sys

sys.path[0:0] = [""]

from test import unittest
from test.unified_format import generate_test_classes

# Location of JSON test specifications.
_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "command_monitoring")


globals().update(
    generate_test_classes(
        os.path.join(_TEST_PATH, "unified"),
        module=__name__,
    )
)


if __name__ == "__main__":
    unittest.main()
