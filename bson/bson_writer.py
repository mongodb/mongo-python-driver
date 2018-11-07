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

"""BSON (Binary JSON) encoding.

"""

from collections import deque
from enum import Enum
from io import BytesIO

from bson import _encode_int, _make_name, _name_value_to_bson, _PACK_INT
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.py3compat import text_type


class BSONWriterException(BaseException):
    pass


class BSONTypes(Enum):
    X00 = b"\x00"
    FLOAT = b"\x01"
    STRING = b"\x02"
    DOCUMENT = b"\x03"
    ARRAY = b"\x04"
    BINARY = b"\x05"
    UNDEFINED = b"\x06"
    OBJECT_ID = b"\x07"
    BOOLEAN = b"\x08"
    UTCDATETIME = b"\x09"
    NULL = b"\x0A"
    INT = b"\x10"
    TIMESTAMP = b"\x11"
    LONG = b"\x12"
    DECIMAL128 = b"\x13"


class _BSONWriterState(Enum):
    INITIAL = 0
    TOP_LEVEL = 1
    DOCUMENT = 2
    ARRAY = 3
    END = 4


class BSONWriter(object):
    def __init__(self, check_keys=False, codec_options=None):
        self._check_keys = check_keys
        self._codec_options = codec_options or DEFAULT_CODEC_OPTIONS
        self.initialize()

    def initialize(self):
        # Writer state tracker. Starts with INITIAL. Ends with INITIAL, END.
        self._states = deque()
        self._states.append(_BSONWriterState.INITIAL)

        # Bytestream in which to place BSON encoded bytes.
        # Deque used to track streams for nested entities.
        self._streams = deque()

        # Size of document in bytes.
        # Deque used to track sizes for nested entities.
        self._sizes = deque()

        # Container type to which we are currently writing.
        # Can be a document or an array.
        # Deque used to track nesting of containers.
        self._containers = deque()

        # Array index at which we are currently writing.
        # Deque used to track nesting of arrays.
        self._array_indices = deque()

        # Cached element name.
        self.__name = None

    def as_bytes(self):
        return self._stream.getvalue()

    @property
    def _name(self):
        if self._current_container == BSONTypes.ARRAY:
            name = text_type(self._get_array_index())
            self._advance_array_index()
            return name
        elif self.__name is not None:
            return self.__name
        else:
            return None

    @_name.setter
    def _name(self, new_name):
        self.__name = new_name

    @property
    def _state(self):
        return self._states[-1]

    @property
    def _nesting_level(self):
        # Nesting level of document currently being populated.
        # Top level document is level 1.
        return len(self._sizes)

    @property
    def _stream(self):
        # Bytestream corresponding to current document.
        return self._streams[-1]

    @property
    def _size(self):
        # Size of current document.
        return self._sizes[-1]

    def _get_array_index(self):
        # Index of current array element.
        return self._array_indices[-1]

    def _advance_array_index(self):
        self._array_indices[-1] += 1

    @property
    def _current_container(self):
        # Pointer to container (document/array) being currently written to.
        return self._containers[-1]

    def _insert_bytes(self, b):
        # Insert given bytes into current document.
        self._sizes[-1] += self._stream.write(b)

    def _finalize(self):
        # Insert null to mark document end.
        self._insert_bytes(b"\x00")

        # Update the corresponding size bytes.
        self._stream.seek(0)
        self._insert_bytes(_PACK_INT(self._size))

    def _write_start_container(self, name_bytes, container_context):
        # TODO: Some container context validation?
        # ...

        # Insert element type marker and name.
        if name_bytes is not None:
            self._insert_bytes(container_context.value + name_bytes)

        # Create stream for new container.
        self._streams.append(BytesIO())

        # Create size counter for new container.
        self._sizes.append(0)

        # Update container tracking.
        self._containers.append(container_context)

        # Add a placeholder for size.
        self._insert_bytes(_PACK_INT(0))

    def _write_end_container(self):
        # End current container.
        self._finalize()

        # If not at top level document, render this container
        # and concatenate it to the higher level container.
        if self._state != _BSONWriterState.TOP_LEVEL:
            bstream = self._stream.getvalue()
            self._containers.pop()
            self._sizes.pop()
            self._streams.pop()
            self._insert_bytes(bstream)

    def start_document(self, name=None):
        if self._state == _BSONWriterState.INITIAL:
            self._write_start_container(None, BSONTypes.DOCUMENT)
            self._states[-1] = _BSONWriterState.TOP_LEVEL
        else:
            name = name or self._name
            self._write_start_container(_make_name(name), BSONTypes.DOCUMENT)
            self._states.append(_BSONWriterState.DOCUMENT)

    def end_document(self):
        if self._state == _BSONWriterState.TOP_LEVEL:
            self._write_end_container()
            self._states[-1] = _BSONWriterState.END
            return

        if self._state == _BSONWriterState.DOCUMENT:
            self._write_end_container()
            self._states.pop()
            return

        raise BSONWriterException("cannot end non-document or finished document.")

    def start_array(self, name=None):
        name = name or self._name
        if name is None:
            raise BSONWriterException("cannot create unnamed array")
        self._write_start_container(_make_name(name), BSONTypes.ARRAY)
        self._states.append(_BSONWriterState.ARRAY)
        self._array_indices.append(0)

    def end_array(self):
        if self._state == _BSONWriterState.ARRAY:
            self._write_end_container()
            self._states.pop()
            self._array_indices.pop()
            return

        raise BSONWriterException("cannot end non-array.")

    def write_name(self, name):
        self._name = name

    def write_value(self, value):
        name = self._name
        if name is None:
            raise BSONWriterException("no cached name and none provided")
        self._insert_bytes(_name_value_to_bson(
            _make_name(name), value, self._check_keys,
            self._codec_options))
        self._name = None

    def write_name_value(self, name, value):
        self._insert_bytes(_name_value_to_bson(
            _make_name(name), value, self._check_keys, self._codec_options))