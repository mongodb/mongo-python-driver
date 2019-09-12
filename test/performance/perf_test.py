# Copyright 2015 MongoDB, Inc.
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

"""Tests for the MongoDB Driver Performance Benchmarking Spec."""

import multiprocessing as mp
import os
import sys
import tempfile
import warnings

try:
    import simplejson as json
except ImportError:
    import json

sys.path[0:0] = [""]

from bson import decode, encode
from bson.json_util import loads
from gridfs import GridFSBucket
from pymongo import MongoClient
from pymongo.monotonic import time
from test import client_context, host, port, unittest

NUM_ITERATIONS = 100
MAX_ITERATION_TIME = 300
NUM_DOCS = 10000

TEST_PATH = os.environ.get('TEST_PATH', os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('data')))

OUTPUT_FILE = os.environ.get('OUTPUT_FILE')

result_data = []

def tearDownModule():
    output = json.dumps({
        'results': result_data
        }, indent=4)
    if OUTPUT_FILE:
        with open(OUTPUT_FILE, 'w') as opf:
            opf.write(output)
    else:
        print(output)


class Timer(object):
    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, *args):
        self.end = time()
        self.interval = self.end - self.start


class PerformanceTest(object):

    @classmethod
    def setUpClass(cls):
        client_context.init()

    def setUp(self):
        pass

    def tearDown(self):
        name = self.__class__.__name__
        median = self.percentile(50)
        result = self.data_size / median
        print('Running %s. MEDIAN=%s' % (self.__class__.__name__,
                                         self.percentile(50)))
        result_data.append({
            'name': name,
            'results': {
                '1': {
                    'ops_per_sec': result
                }
            }
        })

    def before(self):
        pass

    def after(self):
        pass

    def percentile(self, percentile):
        if hasattr(self, 'results'):
            sorted_results = sorted(self.results)
            percentile_index = int(len(sorted_results) * percentile / 100) - 1
            return sorted_results[percentile_index]
        else:
            self.fail('Test execution failed')

    def runTest(self):
        results = []
        start = time()
        self.max_iterations = NUM_ITERATIONS
        for i in range(NUM_ITERATIONS):
            if time() - start > MAX_ITERATION_TIME:
                warnings.warn('Test timed out, completed %s iterations.' % i)
                break
            self.before()
            with Timer() as timer:
                self.do_task()
            self.after()
            results.append(timer.interval)

        self.results = results


# BSON MICRO-BENCHMARKS
class BsonEncodingTest(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(
            os.path.join(TEST_PATH,
                         os.path.join('extended_bson', self.dataset))) as data:
            self.document = loads(data.read())

    def do_task(self):
        for _ in range(NUM_DOCS):
            encode(self.document)


class BsonDecodingTest(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(
            os.path.join(TEST_PATH,
                         os.path.join('extended_bson', self.dataset))) as data:
            self.document = encode(json.loads(data.read()))

    def do_task(self):
        for _ in range(NUM_DOCS):
            decode(self.document)


class TestFlatEncoding(BsonEncodingTest, unittest.TestCase):
    dataset = 'flat_bson.json'
    data_size = 75310000


class TestFlatDecoding(BsonDecodingTest, unittest.TestCase):
    dataset = 'flat_bson.json'
    data_size = 75310000


class TestDeepEncoding(BsonEncodingTest, unittest.TestCase):
    dataset = 'deep_bson.json'
    data_size = 19640000


class TestDeepDecoding(BsonDecodingTest, unittest.TestCase):
    dataset = 'deep_bson.json'
    data_size = 19640000


class TestFullEncoding(BsonEncodingTest, unittest.TestCase):
    dataset = 'full_bson.json'
    data_size = 57340000


class TestFullDecoding(BsonDecodingTest, unittest.TestCase):
    dataset = 'full_bson.json'
    data_size = 57340000


# SINGLE-DOC BENCHMARKS
class TestRunCommand(PerformanceTest, unittest.TestCase):
    data_size = 160000
    def setUp(self):
        self.client = client_context.client
        self.client.drop_database('perftest')

    def do_task(self):
        command = self.client.perftest.command
        for _ in range(NUM_DOCS):
            command("ismaster")


class TestDocument(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(
            os.path.join(
                TEST_PATH, os.path.join(
                    'single_and_multi_document', self.dataset)), 'r') as data:
            self.document = json.loads(data.read())

        self.client = client_context.client
        self.client.drop_database('perftest')

    def tearDown(self):
        super(TestDocument, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.corpus = self.client.perftest.create_collection('corpus')

    def after(self):
        self.client.perftest.drop_collection('corpus')


class TestFindOneByID(TestDocument, unittest.TestCase):
    data_size = 16220000
    def setUp(self):
        self.dataset = 'tweet.json'
        super(TestFindOneByID, self).setUp()

        documents = [self.document.copy() for _ in range(NUM_DOCS)]
        self.corpus = self.client.perftest.corpus
        result = self.corpus.insert_many(documents)
        self.inserted_ids = result.inserted_ids

    def do_task(self):
        find_one = self.corpus.find_one
        for _id in self.inserted_ids:
            find_one({'_id': _id})

    def before(self):
        pass

    def after(self):
        pass


class TestSmallDocInsertOne(TestDocument, unittest.TestCase):
    data_size = 2750000
    def setUp(self):
        self.dataset = 'small_doc.json'
        super(TestSmallDocInsertOne, self).setUp()

        self.documents = [self.document.copy() for _ in range(NUM_DOCS)]

    def do_task(self):
        insert_one = self.corpus.insert_one
        for doc in self.documents:
            insert_one(doc)


class TestLargeDocInsertOne(TestDocument, unittest.TestCase):
    data_size = 27310890
    def setUp(self):
        self.dataset = 'large_doc.json'
        super(TestLargeDocInsertOne, self).setUp()

        self.documents = [self.document.copy() for _ in range(10)]

    def do_task(self):
        insert_one = self.corpus.insert_one
        for doc in self.documents:
            insert_one(doc)


# MULTI-DOC BENCHMARKS
class TestFindManyAndEmptyCursor(TestDocument, unittest.TestCase):
    data_size = 16220000
    def setUp(self):
        self.dataset = 'tweet.json'
        super(TestFindManyAndEmptyCursor, self).setUp()

        for _ in range(10):
            self.client.perftest.command(
                'insert', 'corpus',
                documents=[self.document] * 1000)
        self.corpus = self.client.perftest.corpus

    def do_task(self):
        list(self.corpus.find())

    def before(self):
        pass

    def after(self):
        pass


class TestSmallDocBulkInsert(TestDocument, unittest.TestCase):
    data_size = 2750000
    def setUp(self):
        self.dataset = 'small_doc.json'
        super(TestSmallDocBulkInsert, self).setUp()
        self.documents = [self.document.copy() for _ in range(NUM_DOCS)]

    def before(self):
        self.corpus = self.client.perftest.create_collection('corpus')

    def do_task(self):
        self.corpus.insert_many(self.documents, ordered=True)


class TestLargeDocBulkInsert(TestDocument, unittest.TestCase):
    data_size = 27310890
    def setUp(self):
        self.dataset = 'large_doc.json'
        super(TestLargeDocBulkInsert, self).setUp()
        self.documents = [self.document.copy() for _ in range(10)]

    def before(self):
        self.corpus = self.client.perftest.create_collection('corpus')

    def do_task(self):
        self.corpus.insert_many(self.documents, ordered=True)


class TestGridFsUpload(PerformanceTest, unittest.TestCase):
    data_size = 52428800
    def setUp(self):
        self.client = client_context.client
        self.client.drop_database('perftest')

        gridfs_path = os.path.join(
            TEST_PATH,
            os.path.join('single_and_multi_document', 'gridfs_large.bin'))
        with open(gridfs_path, 'rb') as data:
            self.document = data.read()

        self.bucket = GridFSBucket(self.client.perftest)

    def tearDown(self):
        super(TestGridFsUpload, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.bucket.upload_from_stream('init', b'x')

    def do_task(self):
        self.bucket.upload_from_stream('gridfstest', self.document)


class TestGridFsDownload(PerformanceTest, unittest.TestCase):
    data_size = 52428800
    def setUp(self):
        self.client = client_context.client
        self.client.drop_database('perftest')

        gridfs_path = os.path.join(
            TEST_PATH,
            os.path.join('single_and_multi_document', 'gridfs_large.bin'))

        self.bucket = GridFSBucket(self.client.perftest)
        with open(gridfs_path, 'rb') as gfile:
            self.uploaded_id = self.bucket.upload_from_stream(
                'gridfstest', gfile)

    def tearDown(self):
        super(TestGridFsDownload, self).tearDown()
        self.client.drop_database('perftest')

    def do_task(self):
        self.bucket.open_download_stream(self.uploaded_id).read()


proc_client = None


def proc_init(*dummy):
    global proc_client
    proc_client = MongoClient(host, port)


# PARALLEL BENCHMARKS
def mp_map(map_func, files):
    pool = mp.Pool(initializer=proc_init)
    pool.map(map_func, files)
    pool.close()


def insert_json_file(filename):
    with open(filename, 'r') as data:
        coll = proc_client.perftest.corpus
        coll.insert_many([json.loads(line) for line in data])


def insert_json_file_with_file_id(filename):
    documents = []
    with open(filename, 'r') as data:
        for line in data:
            doc = json.loads(line)
            doc['file'] = filename
            documents.append(doc)
    coll = proc_client.perftest.corpus
    coll.insert_many(documents)


def read_json_file(filename):
    coll = proc_client.perftest.corpus
    temp = tempfile.TemporaryFile()
    try:
        temp.writelines(
            [json.dumps(doc) + '\n' for
             doc in coll.find({'file': filename}, {'_id': False})])
    finally:
        temp.close()


def insert_gridfs_file(filename):
    bucket = GridFSBucket(proc_client.perftest)

    with open(filename, 'rb') as gfile:
        bucket.upload_from_stream(filename, gfile)


def read_gridfs_file(filename):
    bucket = GridFSBucket(proc_client.perftest)

    temp = tempfile.TemporaryFile()
    try:
        bucket.download_to_stream_by_name(filename, temp)
    finally:
        temp.close()


class TestJsonMultiImport(PerformanceTest, unittest.TestCase):
    data_size = 565000000
    def setUp(self):
        self.client = client_context.client
        self.client.drop_database('perftest')

    def before(self):
        self.client.perftest.command({'create': 'corpus'})
        self.corpus = self.client.perftest.corpus

        ldjson_path = os.path.join(
            TEST_PATH, os.path.join('parallel', 'ldjson_multi'))
        self.files = [os.path.join(
            ldjson_path, s) for s in os.listdir(ldjson_path)]

    def do_task(self):
        mp_map(insert_json_file, self.files)

    def after(self):
        self.client.perftest.drop_collection('corpus')

    def tearDown(self):
        super(TestJsonMultiImport, self).tearDown()
        self.client.drop_database('perftest')


class TestJsonMultiExport(PerformanceTest, unittest.TestCase):
    data_size = 565000000
    def setUp(self):
        self.client = client_context.client
        self.client.drop_database('perftest')
        self.client.perfest.corpus.create_index('file')

        ldjson_path = os.path.join(
            TEST_PATH, os.path.join('parallel', 'ldjson_multi'))
        self.files = [os.path.join(
            ldjson_path, s) for s in os.listdir(ldjson_path)]

        mp_map(insert_json_file_with_file_id, self.files)

    def do_task(self):
        mp_map(read_json_file, self.files)

    def tearDown(self):
        super(TestJsonMultiExport, self).tearDown()
        self.client.drop_database('perftest')


class TestGridFsMultiFileUpload(PerformanceTest, unittest.TestCase):
    data_size = 262144000
    def setUp(self):
        self.client = client_context.client
        self.client.drop_database('perftest')

    def before(self):
        self.client.perftest.drop_collection('fs.files')
        self.client.perftest.drop_collection('fs.chunks')

        self.bucket = GridFSBucket(self.client.perftest)
        gridfs_path = os.path.join(
            TEST_PATH, os.path.join('parallel', 'gridfs_multi'))
        self.files = [os.path.join(
            gridfs_path, s) for s in os.listdir(gridfs_path)]

    def do_task(self):
        mp_map(insert_gridfs_file, self.files)

    def tearDown(self):
        super(TestGridFsMultiFileUpload, self).tearDown()
        self.client.drop_database('perftest')


class TestGridFsMultiFileDownload(PerformanceTest, unittest.TestCase):
    data_size = 262144000
    def setUp(self):
        self.client = client_context.client
        self.client.drop_database('perftest')

        bucket = GridFSBucket(self.client.perftest)

        gridfs_path = os.path.join(
            TEST_PATH, os.path.join('parallel', 'gridfs_multi'))
        self.files = [os.path.join(
            gridfs_path, s) for s in os.listdir(gridfs_path)]

        for fname in self.files:
            with open(fname, 'rb') as gfile:
                bucket.upload_from_stream(fname, gfile)

    def do_task(self):
        mp_map(read_gridfs_file, self.files)

    def tearDown(self):
        super(TestGridFsMultiFileDownload, self).tearDown()
        self.client.drop_database('perftest')


if __name__ == "__main__":
    unittest.main()
