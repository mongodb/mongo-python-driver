# Copyright 2009-2015 MongoDB, Inc.
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
sys.path[0:0] = [""]

import datetime

from pymongo import mongo_client
from pymongo import ASCENDING

trials = 2
per_trial = 5000
batch_size = 100
small = {}
medium = {"integer": 5,
          "number": 5.05,
          "boolean": False,
          "array": ["test", "benchmark"]
          }
# this is similar to the benchmark data posted to the user list
large = {"base_url": "http://www.example.com/test-me",
         "total_word_count": 6743,
         "access_time": datetime.datetime.utcnow(),
         "meta_tags": {"description": "i am a long description string",
                       "author": "Holly Man",
                       "dynamically_created_meta_tag": "who know\n what"
                       },
         "page_structure": {"counted_tags": 3450,
                            "no_of_js_attached": 10,
                            "no_of_images": 6
                            },
         "harvested_words": ["10gen", "web", "open", "source", "application",
                             "paas", "platform-as-a-service", "technology",
                             "helps", "developers", "focus", "building",
                             "mongodb", "mongo"] * 20
         }


def setup_insert(db, collection, object):
    db.drop_collection(collection)


def insert(db, collection, object):
    for i in range(per_trial):
        to_insert = object.copy()
        to_insert["x"] = i
        db[collection].insert(to_insert)


def insert_batch(db, collection, object):
    for i in range(per_trial / batch_size):
        db[collection].insert([object] * batch_size)


def find_one(db, collection, x):
    for _ in range(per_trial):
        db[collection].find_one({"x": x})


def find(db, collection, x):
    for _ in range(per_trial):
        for _ in db[collection].find({"x": x}):
            pass


def timed(name, function, args=[], setup=None):
    times = []
    for _ in range(trials):
        if setup:
            setup(*args)
        start = time.time()
        function(*args)
        times.append(time.time() - start)
    best_time = min(times)
    print "%s%d" % (name + (60 - len(name)) * ".", per_trial / best_time)
    return best_time


def main():
    c = mongo_client.MongoClient(connectTimeoutMS=60*1000)  # jack up timeout
    c.drop_database("benchmark")
    db = c.benchmark

    timed("insert (small, no index)", insert,
          [db, 'small_none', small], setup_insert)
    timed("insert (medium, no index)", insert,
          [db, 'medium_none', medium], setup_insert)
    timed("insert (large, no index)", insert,
          [db, 'large_none', large], setup_insert)

    db.small_index.create_index("x", ASCENDING)
    timed("insert (small, indexed)", insert, [db, 'small_index', small])
    db.medium_index.create_index("x", ASCENDING)
    timed("insert (medium, indexed)", insert, [db, 'medium_index', medium])
    db.large_index.create_index("x", ASCENDING)
    timed("insert (large, indexed)", insert, [db, 'large_index', large])

    timed("batch insert (small, no index)", insert_batch,
          [db, 'small_bulk', small], setup_insert)
    timed("batch insert (medium, no index)", insert_batch,
          [db, 'medium_bulk', medium], setup_insert)
    timed("batch insert (large, no index)", insert_batch,
          [db, 'large_bulk', large], setup_insert)

    timed("find_one (small, no index)", find_one,
          [db, 'small_none', per_trial / 2])
    timed("find_one (medium, no index)", find_one,
          [db, 'medium_none', per_trial / 2])
    timed("find_one (large, no index)", find_one,
          [db, 'large_none', per_trial / 2])

    timed("find_one (small, indexed)", find_one,
          [db, 'small_index', per_trial / 2])
    timed("find_one (medium, indexed)", find_one,
          [db, 'medium_index', per_trial / 2])
    timed("find_one (large, indexed)", find_one,
          [db, 'large_index', per_trial / 2])

    timed("find (small, no index)", find, [db, 'small_none', per_trial / 2])
    timed("find (medium, no index)", find, [db, 'medium_none', per_trial / 2])
    timed("find (large, no index)", find, [db, 'large_none', per_trial / 2])

    timed("find (small, indexed)", find, [db, 'small_index', per_trial / 2])
    timed("find (medium, indexed)", find, [db, 'medium_index', per_trial / 2])
    timed("find (large, indexed)", find, [db, 'large_index', per_trial / 2])

#     timed("find range (small, no index)", find,
#           [db, 'small_none',
#            {"$gt": per_trial / 4, "$lt": 3 * per_trial / 4}])
#     timed("find range (medium, no index)", find,
#           [db, 'medium_none',
#            {"$gt": per_trial / 4, "$lt": 3 * per_trial / 4}])
#     timed("find range (large, no index)", find,
#           [db, 'large_none',
#            {"$gt": per_trial / 4, "$lt": 3 * per_trial / 4}])

    timed("find range (small, indexed)", find,
          [db, 'small_index',
           {"$gt": per_trial / 2, "$lt": per_trial / 2 + batch_size}])
    timed("find range (medium, indexed)", find,
          [db, 'medium_index',
           {"$gt": per_trial / 2, "$lt": per_trial / 2 + batch_size}])
    timed("find range (large, indexed)", find,
          [db, 'large_index',
           {"$gt": per_trial / 2, "$lt": per_trial / 2 + batch_size}])

if __name__ == "__main__":
#    cProfile.run("main()")
    main()
