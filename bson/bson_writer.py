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
    FINAL = 4


class BSONWriter(object):
    def __init__(self, check_keys=False, codec_options=None):
        self._check_keys = check_keys
        self._codec_options = codec_options or DEFAULT_CODEC_OPTIONS
        self.initialize()

    def initialize(self):
        # Writer state tracker.
        self._states = deque()
        self._states.append(_BSONWriterState.INITIAL)

        # Bytestream in which to place BSON encoded bytes.
        # Deque used to track streams for nested entities.
        self._streams = deque()

        # Size of document in bytes.
        # Deque used to track sizes for nested entities.
        self._sizes = deque()

        # Array index at which we are currently writing.
        # Deque used to track nesting of arrays.
        self._array_indices = deque()

        # Initialize element name tracking.
        self._next_name = None

    def as_bytes(self):
        if self._state == _BSONWriterState.FINAL:
            return self._stream.getvalue()
        raise BSONWriterException("cannot render incomplete document")

    @property
    def _state(self):
        return self._states[-1]

    @property
    def _stream(self):
        # Bytestream corresponding to current document.
        return self._streams[-1]

    @property
    def _size(self):
        # Size of current document.
        return self._sizes[-1]

    def _get_array_index(self):
        idx = self._array_indices[-1]
        self._array_indices[-1] += 1
        return idx

    def _set_field_name(self, name):
        self._next_name = name or self._next_name

    def _get_field_name(self):
        if self._state == _BSONWriterState.ARRAY:
            name = text_type(self._get_array_index())
        else:
            name = self._next_name
            self._next_name = None
        if name is None:
            raise BSONWriterException("no cached name and none provided")
        return name

    def _insert_bytes(self, b):
        # Insert given bytes into current document.
        self._sizes[-1] += self._stream.write(b)

    def _finalize(self):
        # Insert null to mark document end.
        self._insert_bytes(b"\x00")

        # Update the corresponding size bytes.
        self._stream.seek(0)
        self._insert_bytes(_PACK_INT(self._size))

    def _write_start_container(self, container_type):
        # Insert element type marker and name.
        if self._state != _BSONWriterState.INITIAL:
            name = self._get_field_name()
            self._insert_bytes(container_type.value + _make_name(name))

        # Create stream for new container.
        self._streams.append(BytesIO())

        # Create size counter for new container.
        self._sizes.append(0)

        # Add a placeholder for size.
        self._insert_bytes(_PACK_INT(0))

    def _write_end_container(self):
        # End current container.
        self._finalize()

        # If not at top level document, render this container
        # and concatenate it to the higher level container.
        if self._state != _BSONWriterState.TOP_LEVEL:
            bstream = self._stream.getvalue()
            self._sizes.pop()
            self._streams.pop()
            self._insert_bytes(bstream)

    def start_document(self, name=None):
        if self._state == _BSONWriterState.INITIAL:
            self._write_start_container(BSONTypes.DOCUMENT)
            self._states[-1] = _BSONWriterState.TOP_LEVEL
        else:
            self._set_field_name(name)
            self._write_start_container(BSONTypes.DOCUMENT)
            self._states.append(_BSONWriterState.DOCUMENT)

    def end_document(self):
        if self._state == _BSONWriterState.TOP_LEVEL:
            self._write_end_container()
            self._states[-1] = _BSONWriterState.FINAL
            return

        if self._state == _BSONWriterState.DOCUMENT:
            self._write_end_container()
            self._states.pop()
            return

        raise BSONWriterException("cannot end non-document or finished document.")

    def start_array(self, name=None):
        self._set_field_name(name)
        self._write_start_container(BSONTypes.ARRAY)
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
        self._set_field_name(name)

    def write_value(self, value):
        name = self._get_field_name()
        self._insert_bytes(_name_value_to_bson(
            _make_name(name), value, self._check_keys, self._codec_options))

    def write_name_value(self, name, value):
        self.write_name(name)
        self.write_value(value)


class ImplicitIDBSONWriter(BSONWriter):
    def __init__(self, implicit_id=True, id_generator=None, check_keys=False,
                 codec_options=None):
        if implicit_id and id_generator is None:
            raise BSONWriterException("must supply id generator callable")
        if not callable(id_generator):
            raise BSONWriterException("provided id generator is not callable")
        self._implicit_id = implicit_id
        self._id_generator = id_generator
        super(ImplicitIDBSONWriter, self).__init__(check_keys, codec_options)

    def initialize(self):
        # Flag that tracks whether we have written the `_id`.
        self._id_written = None
        super(ImplicitIDBSONWriter, self).initialize()

    def _insert_bytes(self, b):
        if self._state != _BSONWriterState.TOP_LEVEL:
            return super(ImplicitIDBSONWriter, self)._insert_bytes(b)

        # Error out if attempting to write a second `_id` field.
        # if self._id_written and

        # We are in the top-level document. If we are writing the first
        # key, value pair, we must set the `_id` appropriately.



    def write_name(self, name):
        # Error out if re-writing `_id` in the top-level doc.
        if (name == "_id" and self._id_written and
                self._state == _BSONWriterState.TOP_LEVEL):
            raise BSONWriterException("_id must be first entry in document")
        super(ImplicitIDBSONWriter, self).write_name(name)

    def start_document(self, name=None):
        if self._state == _BSONWriterState.INITIAL:
            self._id_written = False
        super(ImplicitIDBSONWriter, self).start_document(name)