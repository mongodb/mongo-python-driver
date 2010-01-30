# Copyright 2009-2010 10gen, Inc.
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

"""Some tests for the gridfs package that only work under Python >= 1.5.
"""

from __future__ import with_statement

def test_with_statement(test):
    with test.fs.open("test", "w") as f:
        f.write("hello world")

    with test.fs.open("test") as f:
        test.assertEqual("hello world", f.read())
