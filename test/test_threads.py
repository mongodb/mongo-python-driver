# Copyright 2009-present MongoDB, Inc.
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

import threading
from test import IntegrationTest, client_context, unittest
from test.utils import joinall


@client_context.require_connection
def setUpModule():
    pass


class AutoAuthenticateThreads(threading.Thread):
    def __init__(self, collection, num):
        threading.Thread.__init__(self)
        self.coll = collection
        self.num = num
        self.success = False
        self.setDaemon(True)

    def run(self):
        for i in range(self.num):
            self.coll.insert_one({"num": i})
            self.coll.find_one({"num": i})

        self.success = True


class SaveAndFind(threading.Thread):
    def __init__(self, collection):
        threading.Thread.__init__(self)
        self.collection = collection
        self.setDaemon(True)
        self.passed = False

    def run(self):
        sum = 0
        for document in self.collection.find():
            sum += document["x"]

        assert sum == 499500, "sum was %d not 499500" % sum
        self.passed = True


class Insert(threading.Thread):
    def __init__(self, collection, n, expect_exception):
        threading.Thread.__init__(self)
        self.collection = collection
        self.n = n
        self.expect_exception = expect_exception
        self.setDaemon(True)

    def run(self):
        for _ in range(self.n):
            error = True

            try:
                self.collection.insert_one({"test": "insert"})
                error = False
            except:
                if not self.expect_exception:
                    raise

            if self.expect_exception:
                assert error


class Update(threading.Thread):
    def __init__(self, collection, n, expect_exception):
        threading.Thread.__init__(self)
        self.collection = collection
        self.n = n
        self.expect_exception = expect_exception
        self.setDaemon(True)

    def run(self):
        for _ in range(self.n):
            error = True

            try:
                self.collection.update_one({"test": "unique"}, {"$set": {"test": "update"}})
                error = False
            except:
                if not self.expect_exception:
                    raise

            if self.expect_exception:
                assert error


class TestThreads(IntegrationTest):
    def setUp(self):
        self.db = self.client.pymongo_test

    def test_threading(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{"x": i} for i in range(1000)])

        threads = []
        for i in range(10):
            t = SaveAndFind(self.db.test)
            t.start()
            threads.append(t)

        joinall(threads)

    def test_safe_insert(self):
        self.db.drop_collection("test1")
        self.db.test1.insert_one({"test": "insert"})
        self.db.drop_collection("test2")
        self.db.test2.insert_one({"test": "insert"})

        self.db.test2.create_index("test", unique=True)
        self.db.test2.find_one()

        okay = Insert(self.db.test1, 2000, False)
        error = Insert(self.db.test2, 2000, True)

        error.start()
        okay.start()

        error.join()
        okay.join()

    def test_safe_update(self):
        self.db.drop_collection("test1")
        self.db.test1.insert_one({"test": "update"})
        self.db.test1.insert_one({"test": "unique"})
        self.db.drop_collection("test2")
        self.db.test2.insert_one({"test": "update"})
        self.db.test2.insert_one({"test": "unique"})

        self.db.test2.create_index("test", unique=True)
        self.db.test2.find_one()

        okay = Update(self.db.test1, 2000, False)
        error = Update(self.db.test2, 2000, True)

        error.start()
        okay.start()

        error.join()
        okay.join()


if __name__ == "__main__":
    unittest.main()
