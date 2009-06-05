# Copyright 2009 10gen, Inc.
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

"""Test that pymongo is thread safe."""

import unittest
import threading

from test_connection import get_connection


class SaveAndFind(threading.Thread):

    def __init__(self, collection):
        threading.Thread.__init__(self)
        self.collection = collection

    def run(self):
        sum = 0
        for document in self.collection.find():
            sum += document["x"]
        assert sum == 499500


class TestThreads(unittest.TestCase):

    def setUp(self):
        self.db = get_connection().pymongo_test

    def test_threading(self):
        self.db.test.remove({})
        for i in xrange(1000):
            self.db.test.save({"x": i})

        for i in range(10):
            t = SaveAndFind(self.db.test)
            t.start()

if __name__ == "__main__":
    unittest.main()
