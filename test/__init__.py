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

"""Test suite for pymongo, bson, and gridfs.
"""

import os
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
    from unittest2 import SkipTest
else:
    import unittest
    from unittest import SkipTest
import warnings

import pymongo

from bson.py3compat import _unicode

# hostnames retrieved by MongoReplicaSetClient from isMaster will be of unicode
# type in Python 2, so ensure these hostnames are unicodes, too. It makes tests
# like `test_repr` predictable.
host = _unicode(os.environ.get("DB_IP", 'localhost'))
port = int(os.environ.get("DB_PORT", 27017))
pair = '%s:%d' % (host, port)

host2 = _unicode(os.environ.get("DB_IP2", 'localhost'))
port2 = int(os.environ.get("DB_PORT2", 27018))

host3 = _unicode(os.environ.get("DB_IP3", 'localhost'))
port3 = int(os.environ.get("DB_PORT3", 27019))


def setup():
    warnings.resetwarnings()
    warnings.simplefilter("always")


def teardown():
    try:
        c = pymongo.MongoClient(host, port)
    except pymongo.errors.ConnectionFailure:
        # Tests where ssl=True can cause connection failures here.
        # Ignore and continue.
        return

    c.drop_database("pymongo-pooling-tests")
    c.drop_database("pymongo_test")
    c.drop_database("pymongo_test1")
    c.drop_database("pymongo_test2")
    c.drop_database("pymongo_test_mike")
    c.drop_database("pymongo_test_bernie")


class PymongoTestSuite(unittest.TestSuite):
    """Run package-level setup and teardown functions.

    This functionality was built into nose, but doesn't exist in
    unittest.
    """
    def run(self, result):
        setup()
        super(PymongoTestSuite, self).run(result)
        teardown()


class PymongoTestLoader(unittest.TestLoader):
    suiteClass = PymongoTestSuite
