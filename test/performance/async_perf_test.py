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

"""Asynchronous Tests for the MongoDB Driver Performance Benchmarking Spec.

See https://github.com/mongodb/specifications/blob/master/source/benchmarking/benchmarking.md


To set up the benchmarks locally::

    python -m pip install simplejson
    git clone --depth 1 https://github.com/mongodb/specifications.git
    pushd specifications/source/benchmarking/data
    tar xf extended_bson.tgz
    tar xf parallel.tgz
    tar xf single_and_multi_document.tgz
    popd
    export TEST_PATH="specifications/source/benchmarking/data"
    export OUTPUT_FILE="results.json"

Then to run all benchmarks quickly::

    FASTBENCH=1 python test/performance/async_perf_test.py -v

To run individual benchmarks quickly::

    FASTBENCH=1 python test/performance/async_perf_test.py -v TestRunCommand TestFindManyAndEmptyCursor
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
import warnings
from typing import Any, List, Optional, Union

import pytest

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore[no-redef]

sys.path[0:0] = [""]

from test.asynchronous import AsyncPyMongoTestCase, async_client_context, unittest

from bson import encode
from gridfs import AsyncGridFSBucket
from pymongo import (
    DeleteOne,
    InsertOne,
    ReplaceOne,
)

pytestmark = pytest.mark.perf

# Spec says to use at least 1 minute cumulative execution time and up to 100 iterations or 5 minutes but that
# makes the benchmarks too slow. Instead, we use at least 30 seconds and at most 60 seconds.
NUM_ITERATIONS = 100
MIN_ITERATION_TIME = 30
MAX_ITERATION_TIME = 120
NUM_DOCS = 10000
# When debugging or prototyping it's often useful to run the benchmarks locally, set FASTBENCH=1 to run quickly.
if bool(os.getenv("FASTBENCH")):
    NUM_ITERATIONS = 2
    MIN_ITERATION_TIME = 1
    MAX_ITERATION_TIME = 30
    NUM_DOCS = 1000

TEST_PATH = os.environ.get(
    "TEST_PATH", os.path.join(os.path.dirname(os.path.realpath(__file__)), os.path.join("data"))
)

OUTPUT_FILE = os.environ.get("OUTPUT_FILE")

result_data: List = []


def tearDownModule():
    output = json.dumps(result_data, indent=4)
    if OUTPUT_FILE:
        with open(OUTPUT_FILE, "w") as opf:
            opf.write(output)
    else:
        print(output)


class Timer:
    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, *args):
        self.end = time.monotonic()
        self.interval = self.end - self.start


async def concurrent(n_tasks, func):
    tasks = [func() for _ in range(n_tasks)]
    await asyncio.gather(*tasks)


class PerformanceTest:
    dataset: str
    data_size: int
    fail: Any
    n_tasks: int = 1
    did_init: bool = False

    async def asyncSetUp(self):
        await async_client_context.init()
        self.setup_time = time.monotonic()

    async def asyncTearDown(self):
        duration = time.monotonic() - self.setup_time
        # Remove "Test" so that TestFlatEncoding is reported as "FlatEncoding".
        name = self.__class__.__name__[4:]
        median = self.percentile(50)
        megabytes_per_sec = (self.data_size * self.n_tasks) / median / 1000000
        print(
            f"Completed {self.__class__.__name__} {megabytes_per_sec:.3f} MB/s, MEDIAN={self.percentile(50):.3f}s, "
            f"total time={duration:.3f}s, iterations={len(self.results)}"
        )
        result_data.append(
            {
                "info": {
                    "test_name": name,
                    "args": {
                        "tasks": self.n_tasks,
                    },
                },
                "metrics": [
                    {
                        "name": "megabytes_per_sec",
                        "type": "MEDIAN",
                        "value": megabytes_per_sec,
                        "metadata": {
                            "improvement_direction": "up",
                            "measurement_unit": "megabytes_per_second",
                        },
                    },
                ],
            }
        )

    async def before(self):
        pass

    async def do_task(self):
        raise NotImplementedError

    async def after(self):
        pass

    def percentile(self, percentile):
        if hasattr(self, "results"):
            sorted_results = sorted(self.results)
            percentile_index = int(len(sorted_results) * percentile / 100) - 1
            return sorted_results[percentile_index]
        else:
            self.fail("Test execution failed")
            return None

    async def runTest(self):
        results = []
        start = time.monotonic()
        i = 0
        while True:
            i += 1
            await self.before()
            with Timer() as timer:
                if self.n_tasks == 1:
                    await self.do_task()
                else:
                    await concurrent(self.n_tasks, self.do_task)
            await self.after()
            results.append(timer.interval)
            duration = time.monotonic() - start
            if duration > MIN_ITERATION_TIME and i >= NUM_ITERATIONS:
                break
            if i >= NUM_ITERATIONS:
                break
            if duration > MAX_ITERATION_TIME:
                with warnings.catch_warnings():
                    warnings.simplefilter("default")
                    warnings.warn(
                        f"{self.__class__.__name__} timed out after {MAX_ITERATION_TIME}s, completed {i}/{NUM_ITERATIONS} iterations."
                    )

                break

        self.results = results


# SINGLE-DOC BENCHMARKS
class TestRunCommand(PerformanceTest, AsyncPyMongoTestCase):
    data_size = len(encode({"hello": True})) * NUM_DOCS

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client = async_client_context.client
        await self.client.drop_database("perftest")

    async def do_task(self):
        command = self.client.perftest.command
        for _ in range(NUM_DOCS):
            await command("hello", True)


class TestRunCommand8Tasks(TestRunCommand):
    n_tasks = 8


class TestRunCommand80Tasks(TestRunCommand):
    n_tasks = 80


class TestRunCommandUnlimitedTasks(TestRunCommand):
    async def do_task(self):
        command = self.client.perftest.command
        await asyncio.gather(*[command("hello", True) for _ in range(NUM_DOCS)])


class TestDocument(PerformanceTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # Location of test data.
        with open(  # noqa: ASYNC101
            os.path.join(TEST_PATH, os.path.join("single_and_multi_document", self.dataset))
        ) as data:
            self.document = json.loads(data.read())

        self.client = async_client_context.client
        await self.client.drop_database("perftest")

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self.client.drop_database("perftest")

    async def before(self):
        self.corpus = await self.client.perftest.create_collection("corpus")

    async def after(self):
        await self.client.perftest.drop_collection("corpus")


class FindTest(TestDocument):
    dataset = "tweet.json"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.data_size = len(encode(self.document)) * NUM_DOCS
        documents = [self.document.copy() for _ in range(NUM_DOCS)]
        self.corpus = self.client.perftest.corpus
        result = await self.corpus.insert_many(documents)
        self.inserted_ids = result.inserted_ids

    async def before(self):
        pass

    async def after(self):
        pass


class TestFindOneByID(FindTest, AsyncPyMongoTestCase):
    async def do_task(self):
        find_one = self.corpus.find_one
        for _id in self.inserted_ids:
            await find_one({"_id": _id})


class TestFindOneByID8Tasks(TestFindOneByID):
    n_tasks = 8


class TestFindOneByID80Tasks(TestFindOneByID):
    n_tasks = 80


class TestFindOneByIDUnlimitedTasks(TestFindOneByID):
    async def do_task(self):
        find_one = self.corpus.find_one
        await asyncio.gather(*[find_one({"_id": _id}) for _id in self.inserted_ids])


class SmallDocInsertTest(TestDocument):
    dataset = "small_doc.json"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.data_size = len(encode(self.document)) * NUM_DOCS
        self.documents = [self.document.copy() for _ in range(NUM_DOCS)]


class SmallDocMixedTest(TestDocument):
    dataset = "small_doc.json"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.data_size = len(encode(self.document)) * NUM_DOCS * 2
        self.documents = [self.document.copy() for _ in range(NUM_DOCS)]


class TestSmallDocInsertOne(SmallDocInsertTest, AsyncPyMongoTestCase):
    async def do_task(self):
        insert_one = self.corpus.insert_one
        for doc in self.documents:
            await insert_one(doc)


class TestSmallDocInsertOneUnlimitedTasks(SmallDocInsertTest, AsyncPyMongoTestCase):
    async def do_task(self):
        insert_one = self.corpus.insert_one
        await asyncio.gather(*[insert_one(doc) for doc in self.documents])


class LargeDocInsertTest(TestDocument):
    dataset = "large_doc.json"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        n_docs = 10
        self.data_size = len(encode(self.document)) * n_docs
        self.documents = [self.document.copy() for _ in range(n_docs)]


class TestLargeDocInsertOne(LargeDocInsertTest, AsyncPyMongoTestCase):
    async def do_task(self):
        insert_one = self.corpus.insert_one
        for doc in self.documents:
            await insert_one(doc)


class TestLargeDocInsertOneUnlimitedTasks(LargeDocInsertTest, AsyncPyMongoTestCase):
    async def do_task(self):
        insert_one = self.corpus.insert_one
        await asyncio.gather(*[insert_one(doc) for doc in self.documents])


# MULTI-DOC BENCHMARKS
class TestFindManyAndEmptyCursor(FindTest, AsyncPyMongoTestCase):
    async def do_task(self):
        await self.corpus.find().to_list()


class TestFindManyAndEmptyCursor8Tasks(TestFindManyAndEmptyCursor):
    n_tasks = 8


class TestFindManyAndEmptyCursor80Tasks(TestFindManyAndEmptyCursor):
    n_tasks = 80


class TestSmallDocBulkInsert(SmallDocInsertTest, AsyncPyMongoTestCase):
    async def do_task(self):
        await self.corpus.insert_many(self.documents, ordered=True)


class TestSmallDocCollectionBulkInsert(SmallDocInsertTest, AsyncPyMongoTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.models = []
        for doc in self.documents:
            self.models.append(InsertOne(namespace="perftest.corpus", document=doc))

    async def do_task(self):
        await self.corpus.bulk_write(self.models, ordered=True)


class TestSmallDocClientBulkInsert(SmallDocInsertTest, AsyncPyMongoTestCase):
    @async_client_context.require_version_min(8, 0, 0, -24)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.models = []
        for doc in self.documents:
            self.models.append(InsertOne(namespace="perftest.corpus", document=doc))

    @async_client_context.require_version_min(8, 0, 0, -24)
    async def do_task(self):
        await self.client.bulk_write(self.models, ordered=True)


class TestSmallDocBulkMixedOps(SmallDocMixedTest, AsyncPyMongoTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.models: list[Union[InsertOne, ReplaceOne, DeleteOne]] = []
        for doc in self.documents:
            self.models.append(InsertOne(document=doc))
            self.models.append(ReplaceOne(filter={}, replacement=doc.copy(), upsert=True))
            self.models.append(DeleteOne(filter={}))

    async def do_task(self):
        await self.corpus.bulk_write(self.models, ordered=True)


class TestSmallDocClientBulkMixedOps(SmallDocMixedTest, AsyncPyMongoTestCase):
    @async_client_context.require_version_min(8, 0, 0, -24)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.models: list[Union[InsertOne, ReplaceOne, DeleteOne]] = []
        for doc in self.documents:
            self.models.append(InsertOne(namespace="perftest.corpus", document=doc))
            self.models.append(
                ReplaceOne(
                    namespace="perftest.corpus", filter={}, replacement=doc.copy(), upsert=True
                )
            )
            self.models.append(DeleteOne(namespace="perftest.corpus", filter={}))

    @async_client_context.require_version_min(8, 0, 0, -24)
    async def do_task(self):
        await self.client.bulk_write(self.models, ordered=True)


class TestLargeDocBulkInsert(LargeDocInsertTest, AsyncPyMongoTestCase):
    async def do_task(self):
        await self.corpus.insert_many(self.documents, ordered=True)


class TestLargeDocCollectionBulkInsert(LargeDocInsertTest, AsyncPyMongoTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.models = []
        for doc in self.documents:
            self.models.append(InsertOne(namespace="perftest.corpus", document=doc))

    async def do_task(self):
        await self.corpus.bulk_write(self.models, ordered=True)


class TestLargeDocClientBulkInsert(LargeDocInsertTest, AsyncPyMongoTestCase):
    @async_client_context.require_version_min(8, 0, 0, -24)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.models = []
        for doc in self.documents:
            self.models.append(InsertOne(namespace="perftest.corpus", document=doc))

    @async_client_context.require_version_min(8, 0, 0, -24)
    async def do_task(self):
        await self.client.bulk_write(self.models, ordered=True)


class GridFsTest(PerformanceTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client = async_client_context.client
        await self.client.drop_database("perftest")

        gridfs_path = os.path.join(
            TEST_PATH, os.path.join("single_and_multi_document", "gridfs_large.bin")
        )
        with open(gridfs_path, "rb") as data:  # noqa: ASYNC101
            self.document = data.read()
        self.data_size = len(self.document)
        self.bucket = AsyncGridFSBucket(self.client.perftest)

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self.client.drop_database("perftest")


class TestGridFsUpload(GridFsTest, AsyncPyMongoTestCase):
    async def before(self):
        # Create the bucket.
        await self.bucket.upload_from_stream("init", b"x")

    async def do_task(self):
        await self.bucket.upload_from_stream("gridfstest", self.document)


class TestGridFsDownload(GridFsTest, AsyncPyMongoTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.uploaded_id = await self.bucket.upload_from_stream("gridfstest", self.document)

    async def do_task(self):
        await (await self.bucket.open_download_stream(self.uploaded_id)).read()


if __name__ == "__main__":
    unittest.main()
