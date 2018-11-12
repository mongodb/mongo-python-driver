# -*- coding: utf-8 -*-
#
# Copyright 2018-present MongoDB, Inc.
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

"""Tools for specifying BSON codecs for custom types."""

from bson.objectid import ObjectId


class BSONCodecBase(object):
    def to_bson(self, obj, writer):
        raise NotImplementedError

    def from_bson(self, reader):
        raise NotImplementedError

    def generate_id(self, obj):
        # Generate id, set it on the object, and return it.
        generated_id = ObjectId()
        obj._id = generated_id
        return generated_id

    def has_id(self, obj):
        # Return boolean indicating whether `_id` is set on the object.
        # Defaults to returning boolean of `get_id()` return value.
        # User can override this behavior to exclude other inadmissible
        # values for `_id`.
        return hasattr(obj, '_id') and bool(obj._id)

    def get_id(self, obj):
        # Returns current value of `_id` for the given object.
        return obj._id