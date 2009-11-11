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

"""Tools for using Python's :mod:`json` module with MongoDB documents.

This module provides two methods: `object_hook` and `default`. These names are
pretty terrible, but match the names used in Python's `json library
<http://docs.python.org/library/json.html>`_. They allow for specialized
encoding and decoding of MongoDB documents into `Mongo Extended JSON
<http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON>`_'s *Strict* mode.
This lets you encode / decode MongoDB documents to JSON even when they use
special PyMongo types.

Example usage (serialization)::

>>> json.dumps(..., default=json_util.default)

Example usage (deserialization)::

>>> json.loads(..., object_hook=json_util.object_hook)

Currently this only handles special encoding and decoding for ObjectId and
DBRef instancs.
"""

from objectid import ObjectId
from dbref import DBRef

# TODO support other types, like Binary, Code, datetime & regex
# Binary and Code are tricky because they subclass str so json thinks it can
# handle them. Not sure what the proper way to get around this is...

def object_hook(dct):
    if "$oid" in dct:
        return ObjectId(str(dct["$oid"]))
    if "$ref" in dct:
        return DBRef(dct["$ref"], dct["$id"], dct.get("$db", None))
    return dct

def default(obj):
    if isinstance(obj, ObjectId):
        return {"$oid": str(obj)}
    if isinstance(obj, DBRef):
        return obj.as_doc()
    raise TypeError("%r is not JSON serializable" % obj)
