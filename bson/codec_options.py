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
from bson.custom_bson import CustomDocumentClassABC

_RAW_BSON_DOCUMENT_MARKER = 101


def _raw_document_class(document_class):
    """Determine if a document_class is a RawBSONDocument class."""
    marker = getattr(document_class, '_type_marker', None)
    return marker == _RAW_BSON_DOCUMENT_MARKER


_options_base = namedtuple(
    'CodecOptions',
    ('document_class', 'tz_aware', 'uuid_representation',
     'unicode_decode_error_handler', 'tzinfo', 'custom_codec_map',))


class CodecOptions(_options_base):
    """Encapsulates options used encoding and / or decoding BSON.

    The `document_class` option is used to define a custom type for use
    decoding BSON documents. Access to the underlying raw BSON bytes for
    a document is available using the :class:`~bson.raw_bson.RawBSONDocument`
    type::

      >>> from bson.raw_bson import RawBSONDocument
      >>> from bson.codec_options import CodecOptions
      >>> codec_options = CodecOptions(document_class=RawBSONDocument)
      >>> coll = db.get_collection('test', codec_options=codec_options)
      >>> doc = coll.find_one()
      >>> doc.raw
      '\\x16\\x00\\x00\\x00\\x07_id\\x00[0\\x165\\x91\\x10\\xea\\x14\\xe8\\xc5\\x8b\\x93\\x00'

    The document class can be any type that inherits from
    :class:`~collections.MutableMapping`::

      >>> class AttributeDict(dict):
      ...     # A dict that supports attribute access.
      ...     def __getattr__(self, key):
      ...         return self[key]
      ...     def __setattr__(self, key, value):
      ...         self[key] = value
      ...
      >>> codec_options = CodecOptions(document_class=AttributeDict)
      >>> coll = db.get_collection('test', codec_options=codec_options)
      >>> doc = coll.find_one()
      >>> doc._id
      ObjectId('5b3016359110ea14e8c58b93')

    See :doc:`/examples/datetimes` for examples using the `tz_aware` and
    `tzinfo` options.

    See :class:`~bson.binary.UUIDLegacy` for examples using the
    `uuid_representation` option.

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
      - `unicode_decode_error_handler`: The error handler to apply when
        a Unicode-related error occurs during BSON decoding that would
        otherwise raise :exc:`UnicodeDecodeError`. Valid options include
        'strict', 'replace', and 'ignore'. Defaults to 'strict'.
      - `tzinfo`: A :class:`~datetime.tzinfo` subclass that specifies the
        timezone to/from which :class:`~datetime.datetime` objects should be
        encoded/decoded.

    .. warning:: Care must be taken when changing
       `unicode_decode_error_handler` from its default value ('strict').
       The 'replace' and 'ignore' modes should not be used when documents
       retrieved from the server will be modified in the client application
       and stored back to the server.
    """

    def __new__(cls, document_class=dict,
                tz_aware=False, uuid_representation=PYTHON_LEGACY,
                unicode_decode_error_handler="strict", custom_codec_map=None,
                tzinfo=None,):
        if not (issubclass(document_class, (abc.MutableMapping, CustomDocumentClassABC)) or
                _raw_document_class(document_class)):
            raise TypeError("document_class must be dict, bson.son.SON, "
                            "bson.raw_bson.RawBSONDocument, or a "
                            "sublass of collections.MutableMapping")
        if not isinstance(tz_aware, bool):
            raise TypeError("tz_aware must be True or False")
        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError("uuid_representation must be a value "
                             "from bson.binary.ALL_UUID_REPRESENTATIONS")
        if not isinstance(unicode_decode_error_handler, (string_type, None)):
            raise ValueError("unicode_decode_error_handler must be a string "
                             "or None")
        if tzinfo is not None:
            if not isinstance(tzinfo, datetime.tzinfo):
                raise TypeError(
                    "tzinfo must be an instance of datetime.tzinfo")
            if not tz_aware:
                raise ValueError(
                    "cannot specify tzinfo without also setting tz_aware=True")

        if custom_codec_map is None:
            custom_codec_map = {}

        return tuple.__new__(
            cls, (document_class, tz_aware, uuid_representation,
                  unicode_decode_error_handler, tzinfo, custom_codec_map,))

    def _arguments_repr(self):
        """Representation of the arguments used to create this object."""
        document_class_repr = (
            'dict' if self.document_class is dict
            else repr(self.document_class))

        uuid_rep_repr = UUID_REPRESENTATION_NAMES.get(self.uuid_representation,
                                                      self.uuid_representation)

        return ('document_class=%s, tz_aware=%r, uuid_representation='
                '%s, unicode_decode_error_handler=%r, tzinfo=%r' %
                (document_class_repr, self.tz_aware, uuid_rep_repr,
                 self.unicode_decode_error_handler, self.tzinfo))

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._arguments_repr())

    def with_options(self, **kwargs):
        """Make a copy of this CodecOptions, overriding some options::

            >>> from bson.codec_options import DEFAULT_CODEC_OPTIONS
            >>> DEFAULT_CODEC_OPTIONS.tz_aware
            False
            >>> options = DEFAULT_CODEC_OPTIONS.with_options(tz_aware=True)
            >>> options.tz_aware
            True

        .. versionadded:: 3.5
        """
        return CodecOptions(
            kwargs.get('document_class', self.document_class),
            kwargs.get('tz_aware', self.tz_aware),
            kwargs.get('uuid_representation', self.uuid_representation),
            kwargs.get('unicode_decode_error_handler',
                       self.unicode_decode_error_handler),
            kwargs.get('tzinfo', self.tzinfo))

    def register_codec(self, type_to_encode, codec):
        self.custom_codec_map[type_to_encode] = codec

    def get_codec_for_type(self, type_to_encode):
        codec = self.custom_codec_map.get(type_to_encode)
        if codec is not None:
            return codec
        for base in self.custom_codec_map:
            if isinstance(type_to_encode, base):
                codec = self.custom_codec_map[base]
                self.custom_codec_map[type_to_encode] = codec
                return codec
        raise TypeError("no known codec for type {!r}".format(
            type(type_to_encode)))

    def set_custom_document_class(self, doc_cls, doc_cls_codec):
        self.document_class = doc_cls
        self.document_class_codec = doc_cls_codec(doc_cls)


DEFAULT_CODEC_OPTIONS = CodecOptions()


def _parse_codec_options(options):
    """Parse BSON codec options."""
    return CodecOptions(
        document_class=options.get(
            'document_class', DEFAULT_CODEC_OPTIONS.document_class),
        tz_aware=options.get(
            'tz_aware', DEFAULT_CODEC_OPTIONS.tz_aware),
        uuid_representation=options.get(
            'uuidrepresentation', DEFAULT_CODEC_OPTIONS.uuid_representation),
        unicode_decode_error_handler=options.get(
            'unicode_decode_error_handler',
            DEFAULT_CODEC_OPTIONS.unicode_decode_error_handler),
        tzinfo=options.get('tzinfo', DEFAULT_CODEC_OPTIONS.tzinfo))
