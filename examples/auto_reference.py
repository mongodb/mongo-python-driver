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

"""Demo of auto-referencing and auto-dereferencing functionality.

To run this, make sure that you are running an instance of MongoDB on
localhost:27017 and that you have the PyMongo distribution installed:

$ easy_install pymongo

Then do:

$ python auto_reference.py
"""
from pymongo.connection import Connection
from pymongo.son_manipulator import AutoReference, NamespaceInjector

# Make a connection to Mongo.
try:
    connection = Connection("localhost", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

# We need a database to use, but first make sure it's clean.
connection.drop_database("pymongo_examples")
db = connection["pymongo_examples"]

# Now we need to add the NamespaceInjector and AutoReference manipulators.
# These are what actually handle translating documents as the enter and exit
# MongoDB.
db.add_son_manipulator(NamespaceInjector()) # inject _ns
db.add_son_manipulator(AutoReference(db))

# Save a message to the database.
message = {"title": "foo"}
db.messages.save(message)

# Now save a user - the message will get automatically referenced (converted
# to a DBRef) since we've already saved it to the database.
user = {"name": "hello", "message": message}
db.users.save(user)

# Get a message (there is only one) from the messages collection, edit it, and
# save it back.
m = db.messages.find_one()
m["title"] = "world"
db.messages.save(m)

# When we get the user from the database the "message" field is automatically
# dereferenced. As a result, the change in message title is evident in the
# retrieved document.
u = db.users.find_one()
print "%s %s" % (u["name"], u["message"]["title"])
