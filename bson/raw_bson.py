# Copyright 2015-present MongoDB, Inc.
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

"""Tools for representing raw BSON documents.
"""

import collections

from bson import _UNPACK_INT, _iterate_elements
from bson.py3compat import iteritems
from bson.codec_options import (
    DEFAULT_CODEC_OPTIONS as DEFAULT, _RAW_BSON_DOCUMENT_MARKER)
from bson.errors import InvalidBSON


class RawBSONDocument(collections.Mapping):
    """Representation for a MongoDB document that provides access to the raw
    BSON bytes that compose it.

    Only when a field is accessed or modified within the document does
    RawBSONDocument decode its bytes.
    """

    __slots__ = ('__raw', '__inflated_doc', '__codec_options')
    _type_marker = _RAW_BSON_DOCUMENT_MARKER

    def __init__(self, bson_bytes, codec_options=None):
        """Create a new :class:`RawBSONDocument`.

        :Parameters:
          - `bson_bytes`: the BSON bytes that compose this document
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.5
          If a :class:`~bson.codec_options.CodecOptions` is passed in, its
          `document_class` must be :class:`RawBSONDocument`.
        """
        self.__raw = bson_bytes
        self.__inflated_doc = None
        # Can't default codec_options to DEFAULT_RAW_BSON_OPTIONS in signature,
        # it refers to this class RawBSONDocument.
        if codec_options is None:
            codec_options = DEFAULT_RAW_BSON_OPTIONS
        elif codec_options.document_class is not RawBSONDocument:
            raise TypeError(
                "RawBSONDocument cannot use CodecOptions with document "
                "class %s" % (codec_options.document_class, ))
        self.__codec_options = codec_options

    @property
    def raw(self):
        """The raw BSON bytes composing this document."""
        return self.__raw

    def items(self):
        """Lazily decode and iterate elements in this document."""
        return iteritems(self.__inflated)

    @property
    def __inflated(self):
        if self.__inflated_doc is None:
            # We already validated the object's size when this document was
            # created, so no need to do that again. We still need to check the
            # size of all the elements and compare to the document size.
            object_size = _UNPACK_INT(self.__raw[:4])[0] - 1
            position = 0
            self.__inflated_doc = {}
            for key, value, position in _iterate_elements(
                    self.__raw, 4, object_size, self.__codec_options):
                self.__inflated_doc[key] = value
            if position != object_size:
                raise InvalidBSON('bad object or element length')
        return self.__inflated_doc

    def __getitem__(self, item):
        return self.__inflated[item]

    def __iter__(self):
        return iter(self.__inflated)

    def __len__(self):
        return len(self.__inflated)

    def __eq__(self, other):
        if isinstance(other, RawBSONDocument):
            return self.__raw == other.raw
        return NotImplemented

    def __repr__(self):
        return ("RawBSONDocument(%r, codec_options=%r)"
                % (self.raw, self.__codec_options))


DEFAULT_RAW_BSON_OPTIONS = DEFAULT.with_options(document_class=RawBSONDocument)
