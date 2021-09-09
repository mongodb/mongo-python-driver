# Copyright 2016-present MongoDB, Inc.
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

"""Run the BSON corpus specification tests."""

import binascii
import codecs
import functools
import glob
import json
import os
import sys

from decimal import DecimalException

sys.path[0:0] = [""]

from bson import decode, encode, json_util
from bson.binary import STANDARD
from bson.codec_options import CodecOptions
from bson.decimal128 import Decimal128
from bson.dbref import DBRef
from bson.errors import InvalidBSON, InvalidId
from bson.json_util import JSONMode
from bson.son import SON

from test import unittest

_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'bson_corpus')

_TESTS_TO_SKIP = {
    # Python cannot decode dates after year 9999.
    'Y10K',
}

_NON_PARSE_ERRORS = {
    # {"$date": <number>} is our legacy format which we still need to parse.
    'Bad $date (number, not string or hash)',
    # This variant of $numberLong may have been generated by an old version
    # of mongoexport.
    'Bad $numberLong (number, not string)',
    # Python's UUID constructor is very permissive.
    '$uuid invalid value--misplaced hyphens',
}

_IMPLCIT_LOSSY_TESTS = {
    # JSON decodes top-level $ref+$id as a DBRef but BSON doesn't.
    'Document with key names similar to those of a DBRef'
}

_DEPRECATED_BSON_TYPES = {
    # Symbol
    '0x0E': str,
    # Undefined
    '0x06': type(None),
    # DBPointer
    '0x0C': DBRef
}


# Need to set tz_aware=True in order to use "strict" dates in extended JSON.
codec_options = CodecOptions(tz_aware=True, document_class=SON)
# We normally encode UUID as binary subtype 0x03,
# but we'll need to encode to subtype 0x04 for one of the tests.
codec_options_uuid_04 = codec_options._replace(uuid_representation=STANDARD)
json_options_uuid_04 = json_util.JSONOptions(json_mode=JSONMode.CANONICAL,
                                             uuid_representation=STANDARD)
json_options_iso8601 = json_util.JSONOptions(
    datetime_representation=json_util.DatetimeRepresentation.ISO8601,
    json_mode=JSONMode.LEGACY)
to_extjson = functools.partial(json_util.dumps,
                               json_options=json_util.CANONICAL_JSON_OPTIONS)
to_extjson_uuid_04 = functools.partial(json_util.dumps,
                                       json_options=json_options_uuid_04)
to_extjson_iso8601 = functools.partial(json_util.dumps,
                                       json_options=json_options_iso8601)
to_relaxed_extjson = functools.partial(
    json_util.dumps, json_options=json_util.RELAXED_JSON_OPTIONS)
to_bson_uuid_04 = functools.partial(encode,
                                    codec_options=codec_options_uuid_04)
to_bson = functools.partial(encode, codec_options=codec_options)
decode_bson = lambda bbytes: decode(bbytes, codec_options=codec_options)
decode_extjson = functools.partial(
    json_util.loads,
    json_options=json_util.JSONOptions(json_mode=JSONMode.CANONICAL,
                                       document_class=SON))
loads = functools.partial(json.loads, object_pairs_hook=SON)


class TestBSONCorpus(unittest.TestCase):
    def assertJsonEqual(self, first, second, msg=None):
        """Fail if the two json strings are unequal.

        Normalize json by parsing it with the built-in json library. This
        accounts for discrepancies in spacing.
        """
        self.assertEqual(loads(first), loads(second), msg=msg)


def create_test(case_spec):
    bson_type = case_spec['bson_type']
    # Test key is absent when testing top-level documents.
    test_key = case_spec.get('test_key')
    deprecated = case_spec.get('deprecated')

    def run_test(self):
        for valid_case in case_spec.get('valid', []):
            description = valid_case['description']
            if description in _TESTS_TO_SKIP:
                continue

            # Special case for testing encoding UUID as binary subtype 0x04.
            if description.startswith('subtype 0x04'):
                encode_extjson = to_extjson_uuid_04
                encode_bson = to_bson_uuid_04
            else:
                encode_extjson = to_extjson
                encode_bson = to_bson

            cB = binascii.unhexlify(valid_case['canonical_bson'].encode('utf8'))
            cEJ = valid_case['canonical_extjson']
            rEJ = valid_case.get('relaxed_extjson')
            dEJ = valid_case.get('degenerate_extjson')
            if description in _IMPLCIT_LOSSY_TESTS:
                valid_case.setdefault('lossy', True)
            lossy = valid_case.get('lossy')

            # BSON double, use lowercase 'e+' to match Python's encoding
            if bson_type == '0x01':
                cEJ = cEJ.replace('E+', 'e+')

            decoded_bson = decode_bson(cB)

            if not lossy:
                # Make sure we can parse the legacy (default) JSON format.
                legacy_json = json_util.dumps(
                    decoded_bson, json_options=json_util.LEGACY_JSON_OPTIONS)
                self.assertEqual(
                    decode_extjson(legacy_json), decoded_bson, description)

            if deprecated:
                if 'converted_bson' in valid_case:
                    converted_bson = binascii.unhexlify(
                        valid_case['converted_bson'].encode('utf8'))
                    self.assertEqual(encode_bson(decoded_bson), converted_bson)
                    self.assertJsonEqual(
                        encode_extjson(decode_bson(converted_bson)),
                        valid_case['converted_extjson'])
                # Make sure we can decode the type.
                self.assertEqual(decoded_bson, decode_extjson(cEJ))
                if test_key is not None:
                    self.assertIsInstance(decoded_bson[test_key],
                                          _DEPRECATED_BSON_TYPES[bson_type])
                continue

            # Jython can't handle NaN with a payload from
            # struct.(un)pack if endianness is specified in the format string.
            if not (sys.platform.startswith("java") and
                    description == 'NaN with payload'):
                # Test round-tripping canonical bson.
                self.assertEqual(encode_bson(decoded_bson), cB, description)
            self.assertJsonEqual(encode_extjson(decoded_bson), cEJ)

            # Test round-tripping canonical extended json.
            decoded_json = decode_extjson(cEJ)
            self.assertJsonEqual(encode_extjson(decoded_json), cEJ)
            if not lossy:
                self.assertEqual(encode_bson(decoded_json), cB)

            # Test round-tripping degenerate bson.
            if 'degenerate_bson' in valid_case:
                dB = binascii.unhexlify(valid_case['degenerate_bson'].encode('utf8'))
                self.assertEqual(encode_bson(decode_bson(dB)), cB)

            # Test round-tripping degenerate extended json.
            if dEJ is not None:
                decoded_json = decode_extjson(dEJ)
                self.assertJsonEqual(encode_extjson(decoded_json), cEJ)
                if not lossy:
                    self.assertEqual(encode_bson(decoded_json), cB)

            # Test round-tripping relaxed extended json.
            if rEJ is not None:
                self.assertJsonEqual(to_relaxed_extjson(decoded_bson), rEJ)
                decoded_json = decode_extjson(rEJ)
                self.assertJsonEqual(to_relaxed_extjson(decoded_json), rEJ)

        for decode_error_case in case_spec.get('decodeErrors', []):
            with self.assertRaises(InvalidBSON):
                decode_bson(
                    binascii.unhexlify(decode_error_case['bson'].encode('utf8')))

        for parse_error_case in case_spec.get('parseErrors', []):
            description = parse_error_case['description']
            if description in _NON_PARSE_ERRORS:
                decode_extjson(parse_error_case['string'])
                continue
            if bson_type == '0x13':
                self.assertRaises(
                    DecimalException, Decimal128, parse_error_case['string'])
            elif bson_type == '0x00':
                try:
                    decode_extjson(parse_error_case['string'])
                    raise AssertionError('exception not raised for test '
                                         'case: ' + description)
                except (ValueError, KeyError, TypeError, InvalidId):
                    pass
            elif bson_type == '0x05':
                try:
                    decode_extjson(parse_error_case['string'])
                    raise AssertionError('exception not raised for test '
                                         'case: ' + description)
                except (TypeError, ValueError):
                    pass
            else:
                raise AssertionError('cannot test parseErrors for type ' +
                                     bson_type)
    return run_test


def create_tests():
    for filename in glob.glob(os.path.join(_TEST_PATH, '*.json')):
        test_suffix, _ = os.path.splitext(os.path.basename(filename))
        with codecs.open(filename, encoding='utf-8') as bson_test_file:
            test_method = create_test(json.load(bson_test_file))
        setattr(TestBSONCorpus, 'test_' + test_suffix, test_method)


create_tests()


if __name__ == '__main__':
    unittest.main()
