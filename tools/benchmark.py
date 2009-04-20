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

"""MongoDB benchmarking suite."""

import time
import sys
import os
import datetime
import subprocess

from pymongo.connection import Connection
from pymongo.bson import BSON
from pymongo.binary import Binary
from pymongo import ASCENDING

from mongodb_benchmark_tools import post_data

small = {"integer": 5,
         "number": 5.05,
         "boolean": False,
         "array": ["test", "benchmark"]
         }
medium = {"base_url": "http://www.example.com/test-me",
          "total_word_count": 6743,
          "access_time": datetime.datetime.utcnow(),
          "sub_object": small,
          "data": Binary("hello" * 40),
          "big_array": ["mongodb"] * 20
          }
large = {"bigger_array": [medium] * 5,
         "data": Binary("hello" * 500)
         }


class Benchmark(object):
    name = "benchmark"
    description = "a benchmark"
    categories = []

    def setup(self):
        pass

    def run(self, iterations):
        pass

    def teardown(self):
        pass


class Encode(Benchmark):
    def __init__(self, document, size):
        self.name = "encode %s" % size
        self.description = "test encoding 10000 %s documents" % size
        self.categories = ["encode", size]
        self.__doc = document

    def run(self, iterations):
        for _ in range(iterations):
            BSON.from_dict(self.__doc)


class Decode(Benchmark):
    def __init__(self, bson, size):
        self.name = "decode %s" % size
        self.description = "test decoding 10000 %s documents" % size
        self.categories = ["decode", size]
        self.__bson = bson

    def run(self, iterations):
        for _ in range(iterations):
            self.__bson.to_dict()


class Insert(Benchmark):
    def __init__(self, db, document, size):
        self.__db = db
        self.__collection_name = "%s_no_index" % size
        self.__document = document
        self.name = "insert %s" % size
        self.description = "test inserting 10000 %s sized documents into a single collection"
        self.categories = ["insert", size, "no index"]

    def setup(self):
        self.__db.drop_collection(self.__collection_name)
        self.__collection = self.__db[self.__collection_name]

    def run(self, iterations):
        for i in range(iterations):
            doc = self.__document.copy()
            doc["x"] = i
            self.__collection.insert(doc)


class FindOne(Benchmark):
    def __init__(self, collection, query, size):
        self.__collection = collection
        self.__query = query
        self.name = "find one %s" % size
        self.description = "test doing 10000 find one queries on a collection containing %s sized documents" % size
        self. categories = ["query", size, "find one", "no index"]

    def run(self, iterations):
        for _ in range(iterations):
            self.__collection.find_one(self.__query)


class BenchmarkRunner(object):
    def __init__(self, iterations, server_hash):
        self.__iterations = iterations
        self.__server_hash = server_hash
        self.__client_hash = self.get_client_hash()

    def get_client_hash(self):
        git_rev_parse = subprocess.Popen(["git", "rev-parse", "HEAD"],
                                         stdout=subprocess.PIPE)
        (hash, _) = git_rev_parse.communicate()
        return hash.strip()

    def report(self, benchmark, result):
        data = {"benchmark": {"project": "http://github.com/mongodb/mongo-python-driver",
                              "name": benchmark.name,
                              "description": benchmark.description,
                              "tags": benchmark.categories},
                "trial": {"server_hash": self.__server_hash,
                          "client_hash": self.__client_hash,
                          "result": result,
                          "extra_info": ""}}
        post_data(data, post_url="http://localhost:8080/benchmark")
        print "%s: %s" % (benchmark.name, result)

    def run_benchmark(self, benchmark):
        benchmark.setup()
        start = time.time()
        benchmark.run(self.__iterations)
        stop = time.time()
        benchmark.teardown()
        self.report(benchmark, stop - start)


def main():
    connection = Connection()
    runner = BenchmarkRunner(10000, connection.server_info()["gitVersion"])

    runner.run_benchmark(Encode(small, "small"))
    runner.run_benchmark(Encode(medium, "medium"))
    runner.run_benchmark(Encode(large, "large"))

    runner.run_benchmark(Decode(BSON.from_dict(small), "small"))
    runner.run_benchmark(Decode(BSON.from_dict(medium), "medium"))
    runner.run_benchmark(Decode(BSON.from_dict(large), "large"))

    runner.run_benchmark(Insert(connection.benchmark, medium, "medium"))

    runner.run_benchmark(FindOne(connection.benchmark.medium_no_index, {"x": 5000}, "medium"))

if __name__ == "__main__":
    main()
