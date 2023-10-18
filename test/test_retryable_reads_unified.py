# Copyright 2022-present MongoDB, Inc.
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

"""Test the Retryable Reads unified spec tests."""
from __future__ import annotations

import os
import sys

sys.path[0:0] = [""]

from test import unittest
from test.unified_format import generate_test_classes

# Location of JSON test specifications.
TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "retryable_reads", "unified")

# Generate unified tests.
globals().update(generate_test_classes(TEST_PATH, module=__name__))

if __name__ == "__main__":
    unittest.main()
