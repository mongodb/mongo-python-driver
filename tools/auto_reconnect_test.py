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

"""Simple script to help test auto-reconnection."""

import threading
import time

from pymongo.errors import ConnectionFailure
from pymongo.connection import Connection

db = Connection.paired(("localhost", 27018)).test
db.test.remove({})

class Something(threading.Thread):
    def run(self):
        while True:
            time.sleep(1)
            try:
                id = db.test.save({"x": 1})
                assert db.test.find_one(id)["x"] == 1
                db.test.remove(id)
                db.connection.end_request()
                print "Y"
            except ConnectionFailure, e:
                print e
                print "N"

for _ in range(1):
    t = Something()
    t.start()
    time.sleep(1)
