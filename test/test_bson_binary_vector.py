# Copyright 2024-present MongoDB, Inc.
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

from __future__ import annotations

import binascii
import codecs
import json
import struct
from pathlib import Path
from test import unittest

from bson import decode, encode
from bson.binary import Binary, BinaryVectorDtype

_TEST_PATH = Path(__file__).parent / "bson_binary_vector"


class TestBSONBinaryVector(unittest.TestCase):
    """Runs Binary Vector subtype tests.

    Follows the style of the BSON corpus specification tests.
    Tests are automatically generated on import
    from json files in _TEST_PATH via `create_tests`.
    The actual tests are defined in the inner function `run_test`
    of the test generator `create_test`."""


def create_test(case_spec):
    """Create standard test given specification in json.

    We use the naming convention expected (exp) and observed (obj)
    to differentiate what is in the json (expected or suffix _exp)
    from what is produced by the API (observed or suffix _obs)
    """
    test_key = case_spec.get("test_key")

    def run_test(self):
        for test_case in case_spec.get("tests", []):
            description = test_case["description"]
            vector_exp = test_case["vector"]
            dtype_hex_exp = test_case["dtype_hex"]
            dtype_alias_exp = test_case.get("dtype_alias")
            padding_exp = test_case.get("padding", 0)
            canonical_bson_exp = test_case.get("canonical_bson")
            # Convert dtype hex string into bytes
            dtype_exp = BinaryVectorDtype(int(dtype_hex_exp, 16).to_bytes(1, byteorder="little"))

            if test_case["valid"]:
                # Convert bson string to bytes
                cB_exp = binascii.unhexlify(canonical_bson_exp.encode("utf8"))
                decoded_doc = decode(cB_exp)
                binary_obs = decoded_doc[test_key]
                # Handle special float cases like '-inf'
                if dtype_exp in [BinaryVectorDtype.FLOAT32]:
                    vector_exp = [float(x) for x in vector_exp]

                # Test round-tripping canonical bson.
                self.assertEqual(encode(decoded_doc), cB_exp, description)

                # Test BSON to Binary Vector
                vector_obs = binary_obs.as_vector()
                self.assertEqual(vector_obs.dtype, dtype_exp, description)
                if dtype_alias_exp:
                    self.assertEqual(
                        vector_obs.dtype, BinaryVectorDtype[dtype_alias_exp], description
                    )
                self.assertEqual(vector_obs.data, vector_exp, description)
                self.assertEqual(vector_obs.padding, padding_exp, description)

                # Test Binary Vector to BSON
                vector_exp = Binary.from_vector(vector_exp, dtype_exp, padding_exp)
                cB_obs = binascii.hexlify(encode({test_key: vector_exp})).decode().upper()
                self.assertEqual(cB_obs, canonical_bson_exp, description)

            else:
                with self.assertRaises((struct.error, ValueError), msg=description):
                    Binary.from_vector(vector_exp, dtype_exp, padding_exp)

    return run_test


def create_tests():
    for filename in _TEST_PATH.glob("*.json"):
        with codecs.open(str(filename), encoding="utf-8") as test_file:
            test_method = create_test(json.load(test_file))
        setattr(TestBSONBinaryVector, "test_" + filename.stem, test_method)


create_tests()


if __name__ == "__main__":
    unittest.main()
