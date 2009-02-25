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

"""A simple demo of pymongo - the Python Mongo driver.

To run this, make sure that you are running an instance of Mongo on
localhost:27017 and that you have the pymongo package installed:

$ easy_install pymongo

Then do:

$ python simple_demo.py

You should see a friendly message!
"""
import sys
import datetime

from pymongo.connection import Connection
from pymongo.errors import ConnectionFailure

# Make a connection to Mongo.
try:
    connection = Connection("localhost", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

# Get the database "pymongo_examples".
db = connection["pymongo_examples"]

# Drop the collection just to be sure we are starting from a clean state.
db.drop_collection("pymongo")

# Now insert some documents into the "pymongo" collection.
db.pymongo.insert({"x": 1})

# Mongo is schema-free, so we can just add more fields on the fly.
db.pymongo.insert({"x": 20, "y": 100})

# We can also save other cool things, like booleans, strings
db.pymongo.insert({"greeting": True, "hello": u"world"})

# floats and dates.
db.pymongo.insert({"name": u"mike", "account balance": 0.01, "time": datetime.datetime.utcnow()})

# The find_one method returns a single document, or None.
doc = db.pymongo.find_one({"x": 20})
assert doc["y"] == 100

# The find method returns an iterable pymongo.cursor.Cursor over all the results.
all_docs = db.pymongo.find()
sum = 0
for doc in all_docs:
    sum += doc.get("x", 0)
assert sum == 21

# Get the first greeting and print it!
key = "hello"
print "%s, %s!" % (key, db.pymongo.find_one({"greeting": True})[key])

print """\
There is a lot more that you can do with Mongo and pymongo.
Check out http://www.mongodb.org for more!
"""
