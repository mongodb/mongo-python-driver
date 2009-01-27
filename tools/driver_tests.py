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

sys.path.append(os.path.join(os.getcwd(), ".."))
from pymongo.connection import Connection

def test1(db):
    for i in range(100):
        db.part1.save({"x": i})

def remove(db):
    db.remove1.remove({})
    db.remove2.remove({"a": 3})

def find(db):
    db.test.insert({"a": 2})

def circular(db):
    # TODO skipping this test for now because as it's written it *sort of*
    # depends on auto-ref and auto-deref
    pass

def capped(db):
    collection = db.create_collection("capped1", {"capped": True, "size": 500})
    collection.insert({"x": 1})
    collection.insert({"x": 2})

    # TODO ignoring $nExtents for now
    collection2 = db.create_collection("capped2", {"capped": True, "size": 1000})
    str = ""
    for _ in range(100):
        collection2.insert({"dashes": str})
        str += "-"

def main(test, out_file):
    db = Connection()[test]
    test_function = globals()[test]
    try:
        begin = datetime.datetime.now()
        test_function(db)
        end = datetime.datetime.now()
        exit_status = 0
    except:
        begin = None
        begin = None
        exit_status = 1

    f = open(out_file, "w")
    try:
        if begin and end:
            f.write("begintime:%s\n" % begin)
            f.write("endtime:%s\n" % end)
            f.write("totaltime:%s\n" % (end - begin))
        f.write("exit_code:%s\n" % exit_status)
    finally:
        f.close()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

