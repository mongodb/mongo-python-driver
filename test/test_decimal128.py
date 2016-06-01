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

"""Tests for Decimal128."""

import codecs
import glob
import json
import os.path
import pickle
import sys

from binascii import unhexlify
from decimal import Decimal, DecimalException

sys.path[0:0] = [""]

from bson import BSON
from bson.decimal128 import Decimal128, create_decimal128_context
from bson.json_util import dumps, loads
from bson.py3compat import b
from test import client_context, unittest

class TestDecimal128(unittest.TestCase):

    def test_round_trip(self):
        if not client_context.version.at_least(3, 3, 6):
            raise unittest.SkipTest(
                'Round trip test requires MongoDB >= 3.3.6')

        coll = client_context.client.pymongo_test.test
        coll.drop()

        dec128 = Decimal128.from_bid(
            b'\x00@cR\xbf\xc6\x01\x00\x00\x00\x00\x00\x00\x00\x1c0')
        coll.insert_one({'dec128': dec128})
        doc = coll.find_one({'dec128': dec128})
        self.assertIsNotNone(doc)
        self.assertEqual(doc['dec128'], dec128)

    def test_pickle(self):
        dec128 = Decimal128.from_bid(
            b'\x00@cR\xbf\xc6\x01\x00\x00\x00\x00\x00\x00\x00\x1c0')
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            pkl = pickle.dumps(dec128, protocol=protocol)
            self.assertEqual(dec128, pickle.loads(pkl))

    def test_special(self):
        dnan = Decimal('NaN')
        dnnan = Decimal('-NaN')
        dsnan = Decimal('sNaN')
        dnsnan = Decimal('-sNaN')
        dnan128 = Decimal128(dnan)
        dnnan128 = Decimal128(dnnan)
        dsnan128 = Decimal128(dsnan)
        dnsnan128 = Decimal128(dnsnan)

        # Due to the comparison rules for decimal.Decimal we have to
        # compare strings.
        self.assertEqual(str(dnan), str(dnan128.to_decimal()))
        self.assertEqual(str(dnnan), str(dnnan128.to_decimal()))
        self.assertEqual(str(dsnan), str(dsnan128.to_decimal()))
        self.assertEqual(str(dnsnan), str(dnsnan128.to_decimal()))

    def test_decimal128_context(self):
        ctx = create_decimal128_context()
        self.assertEqual("NaN", str(ctx.copy().create_decimal(".13.1")))
        self.assertEqual("Infinity", str(ctx.copy().create_decimal("1E6145")))
        self.assertEqual("0E-6176", str(ctx.copy().create_decimal("1E-6177")))

    def test_spec(self):
        for path in glob.glob(os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "decimal",
                "decimal128*")):
            with codecs.open(path, "r", "utf-8-sig") as fp:
                suite = json.load(fp)

            for case in suite.get("valid", []):
                B = unhexlify(b(case["bson"]))
                E = case["extjson"].replace(" ", "")

                if "canonical_bson" in case:
                    cB = unhexlify(b(case["canonical_bson"]))
                else:
                    cB = B

                if "canonical_extjson" in case:
                    cE = case["canonical_extjson"].replace(" ", "")
                else:
                    cE = E

                self.assertEqual(BSON().encode(BSON(B).decode()), cB)

                if B != cB:
                    self.assertEqual(BSON().encode(BSON(cB).decode()), cB)

                self.assertEqual(
                    dumps(BSON(B).decode()).replace(" ", ""), cE)
                self.assertEqual(
                    dumps(loads(E)).replace(" ", ""), cE)

                if B != cB:
                    self.assertEqual(
                        dumps(BSON(cB).decode()).replace(" ", ""), cE)

                if E != cE:
                    self.assertEqual(
                        dumps(loads(cE)).replace(" ", ""), cE)

                if "lossy" not in case:
                    self.assertEqual(BSON().encode(loads(E)), cB)

                    if E != cE:
                        self.assertEqual(BSON().encode(loads(cE)), cB)

            for test in suite.get("parseErrors", []):
                self.assertRaises(
                    DecimalException, Decimal128, test["string"])

