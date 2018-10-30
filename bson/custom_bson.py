# Copyright 2014-present MongoDB, Inc.
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

"""Tools for specifying BSON codec options."""

import datetime

from collections import namedtuple

from bson.py3compat import abc, string_type
from bson.binary import (ALL_UUID_REPRESENTATIONS,
                         PYTHON_LEGACY,
                         UUID_REPRESENTATION_NAMES)


class DocumentWranglerABC(object):
    """Abstract class to define a standard interface for dealing with docs."""
    cls = None


class MappingDocumentWrangler(DocumentWranglerABC):
    """Wrangler implementation for mappings."""
    def get_id(self, doc):
        return doc["_id"]

    def set_id(self, doc, _id):
        doc['_id'] = _id

    def has_id(self, doc):
        return '_id' in doc


class RawBSONDocumentWrangler(DocumentWranglerABC):
    """Wrangler implementation for RawBSONDocument."""
    def get_id(self, doc):
        raise RuntimeError("unsupported for raw docs")

    def set_id(self, doc, _id):
        raise RuntimeError("unsupported for raw docs")

    def has_id(self, doc):
        return True


class CustomBSONTypeABC(object):
    def to_bson(self, key, writer):
        raise NotImplementedError


class CustomDocumentClassABC(object):
    def to_bson(self, writer):
        raise NotImplementedError

    @classmethod
    def from_bson(cls, reader):
        result = {}
        reader.start_document()
        for entry in reader:
            result[entry.name] = entry.value
        reader.end_document()
        return result

    @property
    def _id(self):
        raise NotImplementedError