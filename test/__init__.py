# Copyright 2010-2014 MongoDB, Inc.
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

"""Clean up databases after running `nosetests`.
"""

import os
import warnings

import pymongo
from pymongo.errors import ConnectionFailure

# hostnames retrieved by MongoReplicaSetClient from isMaster will be of unicode
# type in Python 2, so ensure these hostnames are unicodes, too. It makes tests
# like `test_repr` predictable.
host = unicode(os.environ.get("DB_IP", 'localhost'))
port = int(os.environ.get("DB_PORT", 27017))
pair = '%s:%d' % (host, port)

host2 = unicode(os.environ.get("DB_IP2", 'localhost'))
port2 = int(os.environ.get("DB_PORT2", 27018))

host3 = unicode(os.environ.get("DB_IP3", 'localhost'))
port3 = int(os.environ.get("DB_PORT3", 27019))

# Make sure warnings are always raised, regardless of
# python version.
def setup():
    warnings.resetwarnings()
    warnings.simplefilter("always")


def teardown():
    try:
        c = pymongo.MongoClient(host, port)
    except ConnectionFailure:
        # Tests where ssl=True can cause connection failures here.
        # Ignore and continue.
        return

    c.drop_database("pymongo-pooling-tests")
    c.drop_database("pymongo_test")
    c.drop_database("pymongo_test1")
    c.drop_database("pymongo_test2")
    c.drop_database("pymongo_test_mike")
    c.drop_database("pymongo_test_bernie")
