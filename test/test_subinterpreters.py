# Copyright 2026-present MongoDB, Inc.
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

"""Test running pymongo in subinterpreters."""

from __future__ import annotations

import asyncio
import sys
import textwrap
import threading
import uuid

sys.path[0:0] = [""]

try:
    from concurrent import interpreters
except ImportError:  # pragma: no cover - Python < 3.14
    interpreters = None  # type: ignore[assignment]

try:
    from concurrent.futures import InterpreterPoolExecutor
except ImportError:  # pragma: no cover - Python < 3.14
    InterpreterPoolExecutor = None  # type: ignore[assignment,misc]

from test import IntegrationTest, client_context, unittest

_IS_SYNC = True


class TestSubinterpreters(IntegrationTest):
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

        # Run live clients in multiple subinterpreters at once, mirroring the
        # mod_wsgi test, which mounts the same app in two interpreters.
        n_interpreters = 2
        coll_name = f"subinterp-{uuid.uuid4().hex}"
        self.addCleanup(self.db.drop_collection, coll_name)

        ready = interpreters.create_queue()
        release = interpreters.create_queue()
        done = interpreters.create_queue()
        # The async client's constructor starts a task, which requires a
        # running event loop; synchro translates the rest of the block.
        run_stmt = "asyncio.run(main())" if not _IS_SYNC else "main()"
        # The sync monitor reads the queue on its thread; the async monitor
        # reads it off-loop so the client's tasks keep running.
        release_stmt = (
            "asyncio.to_thread(release.get, timeout=60)"
            if not _IS_SYNC
            else "release.get(timeout=60)"
        )
        code = textwrap.dedent(
            f"""
            import asyncio
            import sys
            sys.path[:0] = path

            from pymongo import MongoClient

            def main():
                client = MongoClient(uri, serverSelectionTimeoutMS=30000)
                collection = client.get_database(db_name).get_collection(coll_name)
                collection.insert_one({{"subinterp": i}})
                assert collection.find_one({{"subinterp": i}}) is not None
                ready.put(i)
                # Hold the client open until every interpreter has connected.
                {release_stmt}
                assert collection.find_one({{"subinterp": i}}) is not None
                done.put(i)

            {run_stmt}
            """
        )

        errors: list[BaseException] = []

        def run(interp):
            try:
                interp.exec(code)
            except BaseException as exc:
                errors.append(exc)

        uri = client_context.uri
        interps = []
        threads = []
        try:
            for i in range(n_interpreters):
                interp = interpreters.create()
                interp.prepare_main(
                    uri=uri,
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

            started = (
                asyncio.to_thread(self._get_n, ready, n_interpreters, errors)
                if not _IS_SYNC
                else self._get_n(ready, n_interpreters, errors)
            )
            self.assertEqual(started, list(range(n_interpreters)))
            for _ in range(n_interpreters):
                release.put(True)

            for thread in threads:
                if _IS_SYNC:
                    thread.join(60)
                else:
                    asyncio.to_thread(thread.join, 60)
                self.assertFalse(thread.is_alive(), f"{thread.name} did not exit")

            finished = (
                asyncio.to_thread(self._get_n, done, n_interpreters, errors)
                if not _IS_SYNC
                else self._get_n(done, n_interpreters, errors)
            )
            self.assertEqual(finished, list(range(n_interpreters)))
            if errors:
                self.fail(f"subinterpreter errors: {errors!r}")
        finally:
            # Unblock any interpreter still waiting, join its worker, then
            # destroy them all.
            for _ in range(n_interpreters):
                release.put(True)
            for thread in threads:
                if _IS_SYNC:
                    thread.join(60)
                else:
                    asyncio.to_thread(thread.join, 60)
            for interp in interps:
                # Closing an idle interpreter runs threading._shutdown, which
                # stops and joins pymongo's monitor threads; skip running ones
                # to avoid masking errors.
                if not interp.is_running():
                    interp.close()

        docs = self.db[coll_name].find({}, {"subinterp": 1}).to_list()
        found = sorted(doc["subinterp"] for doc in docs)
        self.assertEqual(found, list(range(n_interpreters)))

    @unittest.skipUnless(
        sys.version_info >= (3, 14), "InterpreterPoolExecutor requires Python 3.14+"
    )
    def test_interpreter_pool_executor(self):
        if InterpreterPoolExecutor is None:
            self.skipTest("InterpreterPoolExecutor is not available")

        # The interpreters disallow daemon threads, so the sync client starts
        # non-daemon monitor threads; the async client runs tasks on the
        # interpreter's own event loop.
        n_interpreters = 2
        coll_name = f"interp-pool-{uuid.uuid4().hex}"
        self.addCleanup(self.db.drop_collection, coll_name)

        # The async client's constructor starts a task, which requires a
        # running event loop; synchro translates the rest of the block.
        run_stmt = "asyncio.run(main())" if not _IS_SYNC else "main()"

        # The worker's sys.path lacks the repo root, so submit the builtin exec
        # and fix sys.path in the code.
        code = textwrap.dedent(
            f"""
            import asyncio
            import sys
            sys.path[:0] = path

            from pymongo import MongoClient

            def main():
                client = MongoClient(uri, serverSelectionTimeoutMS=30000)
                collection = client.get_database(db_name).get_collection(coll_name)
                collection.insert_one({{"interp-pool": i}})
                assert collection.find_one({{"interp-pool": i}}) is not None

            {run_stmt}
            """
        )
        executor = InterpreterPoolExecutor(max_workers=n_interpreters)
        try:
            uri = client_context.uri
            futures = [
                executor.submit(
                    exec,
                    code,
                    {
                        "i": i,
                        "path": tuple(sys.path),
                        "uri": uri,
                        "db_name": self.db.name,
                        "coll_name": coll_name,
                    },
                )
                for i in range(n_interpreters)
            ]
            for future in futures:
                if _IS_SYNC:
                    future.result(timeout=120)
                else:
                    # Keep the event loop free while waiting for the workers.
                    asyncio.to_thread(future.result, 120)
        finally:
            # Worker interpreter teardown joins monitor threads, which can
            # take a while; keep the event loop free while waiting.
            if _IS_SYNC:
                executor.shutdown(wait=True)
            else:
                asyncio.to_thread(executor.shutdown)

        docs = self.db[coll_name].find({}, {"interp-pool": 1}).to_list()
        found = sorted(doc["interp-pool"] for doc in docs)
        self.assertEqual(found, list(range(n_interpreters)))


if __name__ == "__main__":
    unittest.main()
