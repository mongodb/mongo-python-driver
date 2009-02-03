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

"""A simple demo of gridfs

To run this, make sure that you are running an instance of Mongo on
localhost:27017 and that you have the PyMongo distribution installed:

$ easy_install pymongo

Then do:

$ python gridfs_demo.py
"""

import sys

from pymongo.connection import Connection
from pymongo.errors import ConnectionFailure
import gridfs

# Make a connection to Mongo.
try:
    connection = Connection("localhost", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

# We need a database for GridFS to use, but first make sure it's clean.
connection.drop_database("pymongo_examples")
db = connection["pymongo_examples"]

# Create our GridFS instance
fs = gridfs.GridFS(db)

# Open a GridFile for writing, and write some content
f = fs.open("hello.txt", "w")
f.write("hello ")
f.write("world")
f.close()

# Now print the contents of the file
g = fs.open("hello.txt")
print g.read()
