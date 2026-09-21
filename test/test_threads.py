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

from __future__ import annotations

import sys
import textwrap
import threading
import uuid

try:
    from concurrent import interpreters
except ImportError:  # pragma: no cover - Python < 3.14
    interpreters = None  # type: ignore[assignment]

try:
    from concurrent.futures import InterpreterPoolExecutor
except ImportError:  # pragma: no cover - Python < 3.14
    InterpreterPoolExecutor = None  # type: ignore[assignment,misc]

from test import IntegrationTest, client_context, unittest
from test.utils import joinall


@client_context.require_connection
def setUpModule():
    pass


def _interpreter_pool_worker(i, uri, db_name, coll_name, path):
    import sys

    sys.path[:0] = list(path)

    from pymongo import MongoClient

    client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=30000)
    collection = client.get_database(db_name).get_collection(coll_name)
    collection.insert_one({"interp-pool": i})
    assert collection.find_one({"interp-pool": i}) is not None
    return i


class AutoAuthenticateThreads(threading.Thread):
    def __init__(self, collection, num):
        threading.Thread.__init__(self)
        self.coll = collection
        self.num = num
        self.success = False
        self.daemon = True

    def run(self):
        for i in range(self.num):
            self.coll.insert_one({"num": i})
            self.coll.find_one({"num": i})

        self.success = True


class SaveAndFind(threading.Thread):
    def __init__(self, collection):
        threading.Thread.__init__(self)
        self.collection = collection
        self.daemon = True
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
        self.daemon = True

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
        self.daemon = True

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
        super().setUp()
        self.db = self.client.pymongo_test

    def test_threading(self):
        self.db.drop_collection("coll")
        self.db.coll.insert_many([{"x": i} for i in range(1000)])

        threads = []
        for _i in range(10):
            t = SaveAndFind(self.db.coll)
            t.start()
            threads.append(t)

        joinall(threads)

    def test_safe_insert(self):
        self.db.drop_collection("coll1")
        self.db.coll1.insert_one({"test": "insert"})
        self.db.drop_collection("coll2")
        self.db.coll2.insert_one({"test": "insert"})

        self.db.coll2.create_index("test", unique=True)
        self.db.coll2.find_one()

        okay = Insert(self.db.coll1, 2000, False)
        error = Insert(self.db.coll2, 2000, True)

        error.start()
        okay.start()

        error.join()
        okay.join()

    def test_safe_update(self):
        self.db.drop_collection("coll1")
        self.db.coll1.insert_one({"test": "update"})
        self.db.coll1.insert_one({"test": "unique"})
        self.db.drop_collection("coll2")
        self.db.coll2.insert_one({"test": "update"})
        self.db.coll2.insert_one({"test": "unique"})

        self.db.coll2.create_index("test", unique=True)
        self.db.coll2.find_one()

        okay = Update(self.db.coll1, 2000, False)
        error = Update(self.db.coll2, 2000, True)

        error.start()
        okay.start()

        error.join()
        okay.join()

    @staticmethod
    def _get_n(queue, n, errors):
        try:
            return sorted(queue.get(timeout=30) for _ in range(n))
        except interpreters.QueueEmpty:
            raise AssertionError(f"subinterpreters failed to run: {errors!r}") from None

    @unittest.skipUnless(
        sys.version_info >= (3, 14), "concurrent.interpreters requires Python 3.14+"
    )
    def test_subinterpreters(self):
        if interpreters is None:
            self.skipTest("concurrent.interpreters is not available")

        # Run live MongoClients in more than one subinterpreter at the same
        # time. This mirrors the mod_wsgi test, which mounts the same app in
        # two interpreters, and covers pymongo shutting down its background
        # threads when an interpreter is destroyed (PYTHON-6114).
        n_interpreters = 2
        coll_name = f"subinterp-{uuid.uuid4().hex}"
        self.addCleanup(self.db.drop_collection, coll_name)

        ready = interpreters.create_queue()
        release = interpreters.create_queue()
        done = interpreters.create_queue()
        code = textwrap.dedent(
            """
            import sys
            sys.path[:0] = path

            from pymongo import MongoClient

            client = MongoClient(uri, serverSelectionTimeoutMS=30000)
            collection = client.get_database(db_name).get_collection(coll_name)
            collection.insert_one({"subinterp": i})
            assert collection.find_one({"subinterp": i}) is not None
            ready.put(i)
            # Hold the client open until every interpreter has connected, so
            # that all of the clients are live at the same time.
            release.get(timeout=60)
            assert collection.find_one({"subinterp": i}) is not None
            done.put(i)
            """
        )

        errors: list[BaseException] = []

        def run(interp):
            try:
                interp.exec(code)
            except BaseException as exc:
                errors.append(exc)

        interps = []
        threads = []
        try:
            for i in range(n_interpreters):
                interp = interpreters.create()
                interp.prepare_main(
                    uri=client_context.uri,
                    db_name=self.db.name,
                    coll_name=coll_name,
                    i=i,
                    path=tuple(sys.path),
                    ready=ready,
                    release=release,
                    done=done,
                )
                interps.append(interp)
                thread = threading.Thread(target=run, args=(interp,), name=f"subinterp-{i}")
                threads.append(thread)
                thread.start()

            started = self._get_n(ready, n_interpreters, errors)
            self.assertEqual(started, list(range(n_interpreters)))
            for _ in range(n_interpreters):
                release.put(True)

            for thread in threads:
                thread.join(60)
                self.assertFalse(thread.is_alive(), f"{thread.name} did not exit")

            finished = self._get_n(done, n_interpreters, errors)
            self.assertEqual(finished, list(range(n_interpreters)))
            if errors:
                self.fail(f"subinterpreter errors: {errors!r}")
        finally:
            # Unblock any interpreter still waiting, then destroy them all.
            for _ in range(n_interpreters):
                release.put(True)
            for interp in interps:
                interp.close()

        found = sorted(doc["subinterp"] for doc in self.db[coll_name].find({}, {"subinterp": 1}))
        self.assertEqual(found, list(range(n_interpreters)))

    @unittest.skipUnless(
        sys.version_info >= (3, 14), "InterpreterPoolExecutor requires Python 3.14+"
    )
    def test_interpreter_pool_executor(self):
        if InterpreterPoolExecutor is None:
            self.skipTest("InterpreterPoolExecutor is not available")

        # Run live MongoClients inside interpreters managed by the standard
        # InterpreterPoolExecutor (PYTHON-5418).  The pool's interpreters do
        # not allow daemon threads, so pymongo must start non-daemon monitor
        # threads and stop them when the interpreter is destroyed.
        n_interpreters = 2
        coll_name = f"interp-pool-{uuid.uuid4().hex}"
        self.addCleanup(self.db.drop_collection, coll_name)

        args = (client_context.uri, self.db.name, coll_name, tuple(sys.path))
        with InterpreterPoolExecutor(max_workers=n_interpreters) as executor:
            futures = [
                executor.submit(_interpreter_pool_worker, i, *args) for i in range(n_interpreters)
            ]
            for i, future in enumerate(futures):
                self.assertEqual(future.result(timeout=120), i)

        found = sorted(
            doc["interp-pool"] for doc in self.db[coll_name].find({}, {"interp-pool": 1})
        )
        self.assertEqual(found, list(range(n_interpreters)))


if __name__ == "__main__":
    unittest.main()
