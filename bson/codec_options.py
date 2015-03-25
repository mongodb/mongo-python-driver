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

from collections import MutableMapping, namedtuple

from bson.binary import (ALL_UUID_REPRESENTATIONS,
                         PYTHON_LEGACY,
                         UUID_REPRESENTATION_NAMES)


_options_base = namedtuple(
    'CodecOptions', ('document_class', 'tz_aware', 'uuid_representation'))


class CodecOptions(_options_base):
    """Encapsulates BSON options used in CRUD operations.

    :Parameters:
      - `document_class`: BSON documents returned in queries will be decoded
        to an instance of this class. Must be a subclass of
        :class:`~collections.MutableMapping`. Defaults to :class:`dict`.
      - `tz_aware`: If ``True``, BSON datetimes will be decoded to timezone
        aware instances of :class:`~datetime.datetime`. Otherwise they will be
        naive. Defaults to ``False``.
      - `uuid_representation`: The BSON representation to use when encoding
        and decoding instances of :class:`~uuid.UUID`. Defaults to
        :data:`~bson.binary.PYTHON_LEGACY`.
    """

    def __new__(cls, document_class=dict,
                tz_aware=False, uuid_representation=PYTHON_LEGACY):
        if not issubclass(document_class, MutableMapping):
            raise TypeError("document_class must be dict, bson.son.SON, or "
                            "another subclass of collections.MutableMapping")
        if not isinstance(tz_aware, bool):
            raise TypeError("tz_aware must be True or False")
        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError("uuid_representation must be a value "
                             "from bson.binary.ALL_UUID_REPRESENTATIONS")

        return tuple.__new__(
            cls, (document_class, tz_aware, uuid_representation))

    def __repr__(self):
        document_class_repr = (
            'dict' if self.document_class is dict
            else repr(self.document_class))

        uuid_rep_repr = UUID_REPRESENTATION_NAMES.get(self.uuid_representation,
                                                      self.uuid_representation)

        return (
            'CodecOptions(document_class=%s, tz_aware=%r, uuid_representation='
            '%s)' % (document_class_repr, self.tz_aware, uuid_rep_repr))


DEFAULT_CODEC_OPTIONS = CodecOptions()


def _parse_codec_options(options):
    """Parse BSON codec options."""
    return CodecOptions(
        document_class=options.get(
            'document_class', DEFAULT_CODEC_OPTIONS.document_class),
        tz_aware=options.get(
            'tz_aware', DEFAULT_CODEC_OPTIONS.tz_aware),
        uuid_representation=options.get(
            'uuidrepresentation', DEFAULT_CODEC_OPTIONS.uuid_representation))
