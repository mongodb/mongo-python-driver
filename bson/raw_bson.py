# Copyright 2015 MongoDB, Inc.
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
    CodecOptions, DEFAULT_CODEC_OPTIONS, _RAW_BSON_DOCUMENT_MARKER)


class RawBSONDocument(collections.Mapping):
    """Representation for a MongoDB document that provides access to the raw
    BSON bytes that compose it.

    Only when a field is accessed or modified within the document does
    RawBSONDocument decode its bytes.
    """

    __slots__ = ('__raw', '__inflated_doc', '__codec_options')
    _type_marker = _RAW_BSON_DOCUMENT_MARKER

    def __init__(self, bson_bytes, codec_options=DEFAULT_CODEC_OPTIONS):
        """Create a new :class:`RawBSONDocument`.

        :Parameters:
          - `bson_bytes`: the BSON bytes that compose this document
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.
        """
        self.__raw = bson_bytes
        self.__inflated_doc = None
        # Always decode documents to their lazy representations.
        co = codec_options
        self.__codec_options = CodecOptions(
            tz_aware=co.tz_aware,
            document_class=RawBSONDocument,
            uuid_representation=co.uuid_representation,
            unicode_decode_error_handler=co.unicode_decode_error_handler,
            tzinfo=co.tzinfo)

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
            # created, so no need to do that again.
            self.__inflated_doc = dict(
                element for element in _iterate_elements(
                    self.__raw, 4, _UNPACK_INT(self.__raw[:4])[0] - 1,
                    self.__codec_options))
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
