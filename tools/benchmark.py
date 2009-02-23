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

"""Jim's benchmarking suite
"""

import datetime
import sys
sys.path[0:0] = [""]

from pymongo.connection import Connection
from pymongo import ASCENDING

N = 30000

def timed(function, db):
    before = datetime.datetime.now()
    function(db)
    print "%s%s" % (function.__name__.ljust(15), datetime.datetime.now() - before)

def insert(db):
    for i in range(N):
        db.test.insert({"i": i})

def find_one(db):
    for _ in range(N):
        db.test.find_one()

def find(db):
    for _ in range(N):
        for _ in db.test.find({"i": 3}):
            pass
        for _ in db.test.find({"i": 234}):
            pass
        for _ in db.test.find({"i": 9876}):
            pass

def find_range(db):
    for _ in range(N):
        for _ in db.test.find({"i": {"$gt": 200, "$lt": 200}}):
            pass

def main():
    db = Connection().benchmark
    db.drop_collection("test")
    db.test.create_index("i", ASCENDING)

    timed(insert, db)
    timed(find_one, db)
    timed(find, db)
    timed(find_range, db)

if __name__ == "__main__":
    main()
