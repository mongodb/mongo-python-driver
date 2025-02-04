# Copyright 2024-Present MongoDB, Inc.
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

"""Run Command unified tests."""
from __future__ import annotations

import os
import unittest
from pathlib import Path
from test.asynchronous.unified_format import generate_test_classes

_IS_SYNC = False

# Location of JSON test specifications.
if _IS_SYNC:
    TEST_PATH = os.path.join(Path(__file__).resolve().parent, "run_command")
else:
    TEST_PATH = os.path.join(Path(__file__).resolve().parent.parent, "run_command")


globals().update(
    generate_test_classes(
        os.path.join(TEST_PATH, "unified"),
        module=__name__,
    )
)


if __name__ == "__main__":
    unittest.main()
