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

"""A quick tour of Mongo, using the pymongo driver.

This aims to be a more or less complete introduction to the features of Mongo.
For a quicker and more basic introduction, see "simple_demo.py".
"""

# First we get a connection to Mongo.
# Our Mongo instance must be running locally on port 27017.
from pymongo.connection import Connection
# Each of these are equivalent, the first two make use of default arguments.
connection = Connection()
connection = Connection("localhost")
connection = Connection("localhost", 27017)

# Now we get a database on that connection - again these are equivalent.
db = connection.mydb
db = connection["mydb"]

# We can optionally authenticate. If Mongo is running in auth mode, this is
# required.
#db.authenticate("username", "password")

# Get a list of collections on this database and print them.
for collection in db.collection_names():
    print collection

# This is how we get a collection on the database (they are equivalent).
collection = db.testCollection
collection = db["testCollection"]

# We insert a single document into our collection.
doc = {"name": "MongoDB",
       "type": "database",
       "count": 1,
       "info": {"x": 203,
                "y": 102
                }
       }
collection.insert(doc)

# Now we can use find_one to retrieve the first document in the collection
# (or None if there are no documents).
print collection.find_one()

# Insert more documents into the database. Note how these documents are of a
# completely different "shape" than the previous one. Mongo is schema-free.
for i in range(100):
    collection.insert({"i": i})

# Get the total number of documents in the collection - this will be 101.
print collection.count()

# Get a cursor and iterate over it, printing all documents in the collection.
cursor = collection.find()
for d in cursor:
    print d

# New we use a query specifier to only find specific documents (in this case,
# just the single document with i = 71.
query = {"i": 71}
for d in collection.find(query):
    print d

# We can also use special query specifiers to find all documents with i > 50,
# or 20 < i <= 30.
for d in collection.find({"i": {"$gt": 50}}):
    print d
for d in collection.find({"i": {"$gt": 20, "$lte": 30}}):
    print d

# Finally, we can create an index on i, and print out the collection's
# dictionary of indexes.
collection.create_index("i")
print collection.index_information()
