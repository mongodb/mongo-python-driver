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

"""An example of using a custom type with PyMongo.

To run this, make sure that you are running an instance of Mongo on
localhost:27017 and that you have the PyMongo distribution installed:

$ easy_install pymongo

Then do:

$ python custom_type.py

NOTE: The APIs used in this example are provisional. That means that
if you think something looks really terrible, or could be better, you should
contact us! We'd love suggestions.
"""

import sys

from pymongo.connection import Connection
from pymongo.errors import ConnectionFailure, InvalidDocument
from pymongo.son import SON
from pymongo.son_manipulator import SONManipulator
from pymongo.binary import Binary

# Make a connection to Mongo.
try:
    connection = Connection("localhost", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

# Make sure our database is clean
connection.drop_database("pymongo_examples")
db = connection["pymongo_examples"]


# Here is our custom class that we want to be able to use seamlessly with Mongo.
class Custom(object):
    """Class that we want to be able to save in Mongo.
    """
    def __init__(self, x):
        self.__x = x

    def x(self):
        return self.__x


# If we try to save a document containing an instance of Custom pymongo will
# raise an exception:
document = {"custom": Custom(5)}
try:
    db.test.insert(document)
    print "we won't get this far..."
except InvalidDocument:
    pass


# One way to work around this is to manipulate our instance into something we
# *can* save in Mongo:
def encode_custom(custom):
    return {"_type": "custom", "x": custom.x()}
def decode_custom(document):
    assert document["_type"] == "custom"
    return Custom(document["x"])

document = {"custom": encode_custom(Custom(5))}
db.test.insert(document)
result = db.test.find_one()
custom_field = decode_custom(result["custom"])
assert custom_field.x() == 5


# Needless to say, that was a little unwieldy. Let's make this a bit more
# seamless by creating a new SONManipulator. SONManipulators allow you to
# specify transformations to be applied automatically by the database.
class Transform(SONManipulator):
    def transform_incoming(self, son, collection):
        """Find any Custom instances, and encode them.
        """
        for (key, value) in son.items():
            if isinstance(value, Custom):
                son[key] = encode_custom(value)
            elif isinstance(value, dict): # Make sure we recurse into sub-docs
                son[key] = self.transform_incoming(value, collection)
        return son

    def transform_outgoing(self, son, collection):
        """Find anything that looks like we encoded it, and decode.
        """
        for (key, value) in son.items():
            if isinstance(value, dict):
                if "_type" in value and value["_type"] == "custom":
                    son[key] = decode_custom(value)
                else: # Again, make sure to recurse into sub-docs
                    son[key] = self.transform_outgoing(value, collection)
        return son

# Now add our manipulator to the db:
db.add_son_manipulator(Transform())

# Now we can save and restore Custom instances seamlessly:
db.test.remove({}) # clear whatever is already there
document = {"custom": Custom(5)}
db.test.insert(document)
assert db.test.find_one()["custom"].x() == 5


# We can take this one step further by encoding to binary, using a user defined
# subtype. This allows us to identify what to decode without resorting to tricks
# like the "_type" field used above.
# NOTE: you could just pickle the instance and save that. What we do here is a
# little more lightweight...
def to_binary(custom):
    return Binary(str(custom.x()), 128)
def from_binary(binary):
    return Custom(int(binary))

class TransformToBinary(SONManipulator):
    def transform_incoming(self, son, collection):
        for (key, value) in son.items():
            if isinstance(value, Custom):
                son[key] = to_binary(custom)
            elif isinstance(value, dict):
                son[key] = self.transform_incoming(value, collection)
        return son

    def transform_outgoing(self, son, collection):
        for (key, value) in son.items():
            if isinstance(value, Binary) and value.subtype == 128:
                son[key] = from_binary(value)
            elif isinstance(value, dict):
                son[key] = self.transform_outgoing(value, collection)
        return son

# We'll need to get a new instance of Database to clear the SONManipulator
# we already added
db = connection["pymongo_examples"]
db.test.remove({})
db.add_son_manipulator(Transform())
document = {"custom": Custom(5)}
db.test.insert(document)
assert db.test.find_one()["custom"].x() == 5
