# Copyright 2014-2015 MongoDB, Inc.
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

from bson.binary import (ALL_UUID_REPRESENTATIONS,
                         PYTHON_LEGACY,
                         UUID_REPRESENTATION_NAMES)


class CodecOptions(tuple):
    """Encapsulates BSON options used in CRUD operations.

    :Parameters:
      - `document_class`: BSON documents returned in queries will be decoded
        to an instance of this class.
      - `tz_aware`: If ``True``, BSON datetimes will be decoded to timezone
        aware instances of :class:`~datetime.datetime`. Otherwise they will be
        naive. Defaults to ``False``.
      - `uuid_representation`: The BSON representation to use when encoding
        and decoding instances of :class:`~uuid.UUID`. Defaults to
        :data:`~bson.binary.PYTHON_LEGACY`.
    """

    __slots__ = ()

    def __new__(cls, document_class=dict,
                tz_aware=False, uuid_representation=PYTHON_LEGACY):
        if not isinstance(tz_aware, bool):
            raise TypeError("tz_aware must be True or False")
        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError("uuid_representation must be a value "
                             "from bson.binary.ALL_UUID_REPRESENTATIONS")

        return tuple.__new__(
            cls, (document_class, tz_aware, uuid_representation))

    def __repr__(self):
        if self.document_class is dict:
            document_class_repr = 'dict'
        else:
            document_class_repr = repr(self.document_class)

        uuid_rep_repr = UUID_REPRESENTATION_NAMES.get(self.uuid_representation,
                                                      self.uuid_representation)

        return (
            'CodecOptions(document_class=%s, tz_aware=%r, uuid_representation='
            '%s)' % (document_class_repr, self.tz_aware, uuid_rep_repr))

    def __getnewargs__(self):
        return tuple(self)

    @property
    def document_class(self):
        return self[0]

    @property
    def tz_aware(self):
        return self[1]

    @property
    def uuid_representation(self):
        return self[2]

DEFAULT_CODEC_OPTIONS = CodecOptions()
