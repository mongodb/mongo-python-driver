# Copyright 2009-2012 10gen, Inc.
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

"""Tools for using Python's :mod:`json` module with BSON documents.

This module provides two methods: `object_hook` and `default`. These
names are pretty terrible, but match the names used in Python's `json
library <http://docs.python.org/library/json.html>`_. They allow for
specialized encoding and decoding of BSON documents into `Mongo
Extended JSON
<http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON>`_'s *Strict*
mode.  This lets you encode / decode BSON documents to JSON even when
they use special BSON types.

Example usage (serialization)::

>>> json.dumps(..., default=json_util.default)

Example usage (deserialization)::

>>> json.loads(..., object_hook=json_util.object_hook)

Currently this does not handle special encoding and decoding for
:class:`~bson.binary.Binary` and :class:`~bson.code.Code` instances.

.. versionchanged:: 1.9
   Handle :class:`uuid.UUID` instances, whenever possible.

.. versionchanged:: 1.8
   Handle timezone aware datetime instances on encode, decode to
   timezone aware datetime instances.

.. versionchanged:: 1.8
   Added support for encoding/decoding :class:`~bson.max_key.MaxKey`
   and :class:`~bson.min_key.MinKey`, and for encoding
   :class:`~bson.timestamp.Timestamp`.

.. versionchanged:: 1.2
   Added support for encoding/decoding datetimes and regular expressions.
"""

import calendar
import datetime
import re
try:
    import uuid
    _use_uuid = True
except ImportError:
    _use_uuid = False

from bson import EPOCH_AWARE
from bson.dbref import DBRef
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from bson.tz_util import utc

# TODO support Binary and Code
# Binary and Code are tricky because they subclass str so json thinks it can
# handle them. Not sure what the proper way to get around this is...
#
# One option is to just add some other method that users need to call _before_
# calling json.dumps or json.loads. That is pretty terrible though...

# TODO share this with bson.py?
_RE_TYPE = type(re.compile("foo"))


def object_hook(dct):
    if "$oid" in dct:
        return ObjectId(str(dct["$oid"]))
    if "$ref" in dct:
        return DBRef(dct["$ref"], dct["$id"], dct.get("$db", None))
    if "$date" in dct:
        secs = float(dct["$date"]) / 1000.0
        return EPOCH_AWARE + datetime.timedelta(seconds=secs)
    if "$regex" in dct:
        flags = 0
        if "i" in dct["$options"]:
            flags |= re.IGNORECASE
        if "m" in dct["$options"]:
            flags |= re.MULTILINE
        return re.compile(dct["$regex"], flags)
    if "$minKey" in dct:
        return MinKey()
    if "$maxKey" in dct:
        return MaxKey()
    if _use_uuid and "$uuid" in dct:
        return uuid.UUID(dct["$uuid"])
    return dct


def default(obj):
    if isinstance(obj, ObjectId):
        return {"$oid": str(obj)}
    if isinstance(obj, DBRef):
        return obj.as_doc()
    if isinstance(obj, datetime.datetime):
        # TODO share this code w/ bson.py?
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
        millis = int(calendar.timegm(obj.timetuple()) * 1000 +
                     obj.microsecond / 1000)
        return {"$date": millis}
    if isinstance(obj, _RE_TYPE):
        flags = ""
        if obj.flags & re.IGNORECASE:
            flags += "i"
        if obj.flags & re.MULTILINE:
            flags += "m"
        return {"$regex": obj.pattern,
                "$options": flags}
    if isinstance(obj, MinKey):
        return {"$minKey": 1}
    if isinstance(obj, MaxKey):
        return {"$maxKey": 1}
    if isinstance(obj, Timestamp):
        return {"t": obj.time, "i": obj.inc}
    if _use_uuid and isinstance(obj, uuid.UUID):
        return {"$uuid": obj.hex}
    raise TypeError("%r is not JSON serializable" % obj)
