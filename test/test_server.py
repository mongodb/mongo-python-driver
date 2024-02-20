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

"""Test the server module."""
from __future__ import annotations

import sys

sys.path[0:0] = [""]

from test import unittest

from pymongo.hello import Hello
from pymongo.server import Server
from pymongo.server_description import ServerDescription


class TestServer(unittest.TestCase):
    def test_repr(self):
        hello = Hello({"ok": 1})
        sd = ServerDescription(("localhost", 27017), hello)
        server = Server(sd, pool=object(), monitor=object())  # type: ignore[arg-type]
        self.assertTrue("Standalone" in str(server))


if __name__ == "__main__":
    unittest.main()
