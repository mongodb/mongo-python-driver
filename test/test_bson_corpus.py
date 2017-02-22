# Copyright 2016 MongoDB, Inc.
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
import functools
import glob
import os
import sys

if sys.version_info[:2] == (2, 6):
    try:
        import simplejson as json
    except ImportError:
        import json
else:
    import json

from bson import BSON, EPOCH_AWARE, json_util
from bson.binary import STANDARD
from bson.codec_options import CodecOptions
from bson.dbref import DBRef
from bson.errors import InvalidBSON
from bson.py3compat import text_type, b
from bson.son import SON

from test import unittest


_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'bson_corpus')


_DEPRECATED_BSON_TYPES = {
    # Symbol
    '0x0E': text_type,
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
json_options_uuid_04 = json_util.JSONOptions(
        strict_number_long=True,
        strict_uuid=True,
        datetime_representation=json_util.DatetimeRepresentation.NUMBERLONG,
        uuid_representation=STANDARD)
json_options_iso8601 = json_util.JSONOptions(
    datetime_representation=json_util.DatetimeRepresentation.ISO8601)
to_extjson = functools.partial(
    json_util.dumps, json_options=json_util.CANONICAL_JSON_OPTIONS)
to_extjson_uuid_04 = functools.partial(json_util.dumps,
                                       json_options=json_options_uuid_04)
to_extjson_iso8601 = functools.partial(json_util.dumps,
                                       json_options=json_options_iso8601)
to_bson_uuid_04 = functools.partial(BSON.encode,
                                    codec_options=codec_options_uuid_04)
to_bson = functools.partial(BSON.encode, codec_options=codec_options)
decode_bson = lambda bbytes: BSON(bbytes).decode(codec_options=codec_options)
if json_util._HAS_OBJECT_PAIRS_HOOK:
    decode_extjson = functools.partial(
        json_util.loads,
        json_options=json_util.JSONOptions(canonical_extended_json=True,
                                           document_class=SON))
    loads = functools.partial(json.loads, object_pairs_hook=SON)
else:
    decode_extjson = functools.partial(
        json_util.loads,
        json_options=json_util.CANONICAL_JSON_OPTIONS)
    loads = json.loads


class TestBSONCorpus(unittest.TestCase):
    def test_all_bson_types(self):
        # Because we can't round-trip all BSON types (see _DEPRECATED_BSON_TYPES
        # above for how these are handled), make this test a special case,
        # instead of mangling our create_test function below.
        with open(os.path.join(_TEST_PATH, 'multi-type.json')) as spec_file:
            case_spec = json.load(spec_file)
        for valid_case in case_spec.get('valid', []):
            B = binascii.unhexlify(b(valid_case['bson']))
            E = valid_case['extjson']

            # Make sure that the BSON and JSON decode to the same document.
            self.assertEqual(
                json_util.loads(
                    E, json_options=json_util.CANONICAL_JSON_OPTIONS),
                BSON(B).decode(
                    codec_options=CodecOptions(
                        document_class=SON, tz_aware=True)))


def create_test(case_spec):
    bson_type = case_spec['bson_type']
    # Test key is absent when testing top-level documents.
    test_key = case_spec.get('test_key')

    def run_test(self):
        for valid_case in case_spec.get('valid', []):
            # Special case for testing encoding UUID as binary subtype 0x04.
            if valid_case['description'] == 'subtype 0x04':
                encode_extjson = to_extjson_uuid_04
                encode_bson = to_bson_uuid_04
            else:
                encode_extjson = to_extjson
                encode_bson = to_bson

            B = binascii.unhexlify(b(valid_case['bson']))

            if 'canonical_bson' in valid_case:
                cB = binascii.unhexlify(b(valid_case['canonical_bson']))
            else:
                cB = B

            if bson_type in _DEPRECATED_BSON_TYPES:
                # Just make sure we can decode the type.
                self.assertIsInstance(
                    decode_bson(B)[test_key], _DEPRECATED_BSON_TYPES[bson_type])
                if B != cB:
                    self.assertIsInstance(
                        decode_bson(cB)[test_key],
                        _DEPRECATED_BSON_TYPES[bson_type])
            # PyPy3 and Jython can't handle NaN with a payload from
            # struct.(un)pack if endianness is specified in the format string.
            elif not ((('PyPy' in sys.version and
                        sys.version_info[:2] < (3, 3)) or
                       sys.platform.startswith("java")) and
                      valid_case['description'] == 'NaN with payload'):
                # Test round-tripping encoding/decoding the type.
                self.assertEqual(encode_bson(decode_bson(B)), cB)

                if B != cB:
                    self.assertEqual(
                        encode_bson(decode_bson(cB)), cB)

            if 'extjson' in valid_case:
                E = valid_case['extjson']
                cE = valid_case.get('canonical_extjson', E)

                if bson_type in _DEPRECATED_BSON_TYPES:
                    # Just make sure that we can parse the extended JSON.
                    self.assertIsInstance(
                        decode_extjson(E)[test_key],
                        _DEPRECATED_BSON_TYPES[bson_type])
                    if E != cE:
                        self.assertIsInstance(
                            decode_extjson(cE)[test_key],
                            _DEPRECATED_BSON_TYPES[bson_type])
                    continue

                # Normalize extended json by parsing it with the built-in
                # json library. This accounts for discrepancies in spacing.
                # Key ordering is preserved when possible.
                normalized_cE = loads(cE)

                self.assertEqual(
                    loads(encode_extjson(decode_bson(B))),
                    normalized_cE)

                self.assertEqual(
                    loads(encode_extjson(decode_extjson(E))),
                    normalized_cE)

                if bson_type == '0x09':
                    # Test datetime can output ISO8601 to match extjson or
                    # $numberLong to match canonical_extjson if the datetime
                    # is pre-epoch.
                    if decode_extjson(E)[test_key] >= EPOCH_AWARE:
                        normalized_date = loads(E)
                    else:
                        normalized_date = normalized_cE
                    self.assertEqual(
                        loads(to_extjson_iso8601(decode_extjson(cE))),
                        normalized_date)

                if B != cB:
                    self.assertEqual(
                        loads(encode_extjson(decode_bson(cB))),
                        normalized_cE)

                if E != cE:
                    self.assertEqual(
                        loads(encode_extjson(decode_extjson(cE))),
                        normalized_cE)

                if 'lossy' not in valid_case:
                    # Skip tests for document type in Python 2.6 that have
                    # multiple keys, since we can't control key ordering when
                    # parsing JSON.
                    if json_util._HAS_OBJECT_PAIRS_HOOK or not (
                            sys.version_info[:2] == (2, 6) and
                            bson_type in ('0x03', '0x00') and
                            len(decode_extjson(E)) > 1):
                        self.assertEqual(encode_bson(decode_extjson(E)), cB)

                        if E != cE:
                            self.assertEqual(
                                encode_bson(decode_extjson(cE)),
                                cB)

        for decode_error_case in case_spec.get('decodeErrors', []):
            with self.assertRaises(InvalidBSON):
                decode_bson(
                    binascii.unhexlify(b(decode_error_case['bson'])))

    return run_test


def create_tests():
    for filename in glob.glob(os.path.join(_TEST_PATH, '*.json')):
        test_suffix, _ = os.path.splitext(os.path.basename(filename))
        if test_suffix == 'multi-type':
            # Special case in TestBSONCorpus.
            continue
        with open(filename) as bson_test_file:
            test_method = create_test(json.load(bson_test_file))
        setattr(TestBSONCorpus, 'test_' + test_suffix, test_method)


create_tests()


if __name__ == '__main__':
    unittest.main()
