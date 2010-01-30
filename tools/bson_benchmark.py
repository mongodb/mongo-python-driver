# Copyright 2009-2010 10gen, Inc.
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

"""Benchmarking for the `pymongo.bson` module.

Depends on `simplejson`. Which is really just the Python 2.6 `json` module, so
this should be updated to use that if it exists an `simplejson` otherwise.
"""

import datetime
try:
    import cProfile as profile
except:
    import profile
import sys
sys.path[0:0] = [""]

# TODO try importing json first and do simplejson if that fails
import simplejson as json

from pymongo import bson

trials = 100000

def run(case, function):
    start = datetime.datetime.now()
    for _ in range(trials):
       result = function(case)
    print "took: %s" % (datetime.datetime.now() - start)
    return result

def main():
    test_cases = [{},
                  {"hello": "world"},
                  {"hello": "world",
                   "mike": u"something",
                   "here's": u"an\u8744other"},
                  {"int": 200,
                   "bool": True,
                   "an int": 20,
                   "a bool": False},
                  {"this": 5,
                   "is": {"a": True},
                   "big": [True, 5.5],
                   "object": None}]

    for case in test_cases:
        print "case: %r" % case
        print "enc bson",
        enc_bson = run(case, bson.BSON.from_dict)
        print "enc json",
        enc_json = run(case, json.dumps)
        print "dec bson",
        assert case == run(enc_bson, bson._to_dict)
        print "dec json",
        assert case == run(enc_json, json.loads)

if __name__ == "__main__":
    profile.run("main()")
