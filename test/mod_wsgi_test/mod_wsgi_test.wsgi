# Copyright 2012-2015 MongoDB, Inc.
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

"""Minimal test of PyMongo in a WSGI application, see bug PYTHON-353
"""

import os
import sys

this_path = os.path.dirname(os.path.join(os.getcwd(), __file__))

# Location of PyMongo checkout
repository_path = os.path.normpath(os.path.join(this_path, '..', '..'))
sys.path.insert(0, repository_path)

import pymongo
from pymongo.hello import HelloCompat
from pymongo.mongo_client import MongoClient

client = MongoClient()
collection = client.test.test
ndocs = 20
collection.drop()
collection.insert_many([{'i': i} for i in range(ndocs)])
client.close() # Discard main thread's request socket.
client = MongoClient()
collection = client.test.test

try:
    from mod_wsgi import version as mod_wsgi_version
except:
    mod_wsgi_version = None


def application(environ, start_response):
    results = list(collection.find().batch_size(10))
    assert len(results) == ndocs
    output = ' python %s, mod_wsgi %s, pymongo %s ' % (
        sys.version, mod_wsgi_version, pymongo.version)
    response_headers = [('Content-Length', str(len(output)))]
    start_response('200 OK', response_headers)
    return [output.encode('ascii')]
