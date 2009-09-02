#!/usr/bin/env python

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

"""Test runner for the driver tests."""

import sys
import os
import datetime

sys.path[0:0] = [os.path.join(os.getcwd(), "..")]
from pymongo.connection import Connection
from pymongo.errors import CollectionInvalid
from pymongo import OFF, SLOW_ONLY, ALL, ASCENDING
import gridfs

def admin(db, out):
    tester = db.tester
    tester.insert({"test": 1})
    try:
        db.validate_collection(tester)
        print >>out, "true"
    except:
        print >>out, "false"

    try:
        db.validate_collection("system.users")
        print >>out, "true"
    except:
        print >>out, "false"

    try:
        db.validate_collection("$")
        print >>out, "true"
    except:
        print >>out, "false"

    levels = {OFF: "off",
              SLOW_ONLY: "slowOnly",
              ALL: "all"}
    print >>out, levels[db.profiling_level()]
    db.set_profiling_level(OFF)
    print >>out, levels[db.profiling_level()]

def capped(db, out):
    collection = db.create_collection("capped1", {"capped": True, "size": 500})
    collection.insert({"x": 1})
    collection.insert({"x": 2})

    # TODO ignoring $nExtents for now
    collection2 = db.create_collection("capped2", {"capped": True, "size": 1000})
    str = ""
    for _ in range(100):
        collection2.insert({"dashes": str})
        str += "-"

def count1(db, out):
    print >>out, db.test1.find().count()
    print >>out, db.test2.find().count()
    print >>out, db.test3.find({"i": "a"}).count()
    print >>out, db.test3.find({"i": 3}).count()
    print >>out, db.test3.find({"i": {"$gte": 67}}).count()

def dberror(db, out):
    db.reset_error_history()
    print >>out, db.error() is None
    print >>out, db.previous_error() is None
    db._command({"forceerror": 1}, check=False)
    print >>out, db.error() is None
    print >>out, db.previous_error() is None
    db.foo.find_one()
    print >>out, db.error() is None
    print >>out, db.previous_error() is None
    print >>out, db.previous_error()["nPrev"]
    db.reset_error_history()
    print >>out, db.previous_error() is None

def dbs(db, out):
    db.dbs_1.save({"foo": "bar"})
    db.dbs_2.save({"psi": "phi"})
    print >>out, db.name()
    for name in [n for n in sorted(db.collection_names()) if n.startswith("dbs")]:
        print >>out, name
    db.drop_collection(db.dbs_1)
    db.create_collection("dbs_3")
    for name in [n for n in sorted(db.collection_names()) if n.startswith("dbs")]:
        print >>out, name

def find(db, out):
    db.test.insert({"a": 2})

def find1(db, out):
    for doc in db.c.find({"x": 1}).sort("y", ASCENDING).skip(20).limit(10):
        print >>out, doc["z"]

def indices(db, out):
    db.x.drop_index([("field1", ASCENDING)])
    for name in db.x.index_information().keys():
        print >>out, name
    db.y.create_index([("a", ASCENDING),
                       ("b", ASCENDING),
                       ("c", ASCENDING)])
    db.y.create_index("d")
    for name in sorted(db.y.index_information().keys()):
        print >>out, name

def remove(db, out):
    db.remove1.remove({})
    db.remove2.remove({"a": 3})

def stress1(db, out):
    for i in range(50000):
        db.stress1.save({"name": "asdf" + str(i),
                         "date": datetime.datetime.utcnow(),
                         "id": i,
                         "blah": "lksjhasoh1298a1shasoidiohaskjasiouashoasasiugoas" +
                         "lksjhasoh1298a1shasoidiohaskjasiouashoasasiugoas" +
                         "lksjhasoh1298a1shasoidiohaskjasiouashoasasiugoas" +
                         "lksjhasoh1298a1shasoidiohaskjasiouashoasasiugoas" +
                         "lksjhasoh1298a1shasoidiohaskjasiouashoasasiugoas" +
                         "lksjhasoh1298a1shasoidiohaskjasiouashoasasiugoas",
                         "subarray": []})
    for i in range(10000):
        x = db.stress1.find_one({"id": i})
        x["subarray"] = "foo" + str(i)
        db.stress1.save(x)

    db.stress1.create_index("date")

def test1(db, out):
    for i in range(100):
        db.part1.insert({"x": i})

def update(db, out):
    db.foo.update({"x": 1}, {"x": 1, "y": 2})
    db.foo.update({"x": 2}, {"x": 1, "y": 7})
    db.foo.update({"x": 3}, {"x": 4, "y": 1}, upsert=True)

def gridfs_in(db, time, input):
    name = input.name
    fs = gridfs.GridFS(db)
    f = fs.open(name, "w")
    f.write(input.read())
    f.close()

def gridfs_out(db, time, input, output):
    fs = gridfs.GridFS(db)
    f = fs.open(input.name)
    output.write(f.read())
    f.close()

def main(test, time_file, in_file=None, out_file=None):
    db = Connection().driver_test_framework
    try:
        test_function = globals()[test]
    except KeyError:
        return

    time_file = open(time_file, "w")
    args = [db, time_file]
    if in_file:
        in_file = open(in_file, "r")
        args.append(in_file)
    if out_file:
        out_file = open(out_file, "w")
        args.append(out_file)
    try:
        begin = datetime.datetime.now()
        test_function(*args)
        end = datetime.datetime.now()
        exit_status = 0
    except:
        begin = None
        begin = None
        exit_status = 1
        raise

    if begin and end:
        time_file.write("begintime:%s\n" % begin)
        time_file.write("endtime:%s\n" % end)
        time_file.write("totaltime:%s\n" % (end - begin))
    time_file.write("exit_code:%s\n" % exit_status)
    time_file.close()
    if in_file and hasattr(in_file, "close"):
        in_file.close()
    if out_file:
        out_file.close()

if __name__ == "__main__":
    main(*sys.argv[1:])

