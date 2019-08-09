import sys

sys.path[0:0] = [""]

from bson import encode
from bson.errors import InvalidStringData
from bson.py3compat import PY3
from test import unittest

class TestUTF8(unittest.TestCase):

    # Verify that python and bson have the same understanding of
    # legal utf-8 if the first byte is 0xf4 (244)
    def _assert_same_utf8_validation(self, data):
        try:
            data.decode('utf-8')
            py_is_legal = True 
        except UnicodeDecodeError:
            py_is_legal = False

        try:
            encode({'x': data})
            bson_is_legal = True 
        except InvalidStringData:
            bson_is_legal = False

        self.assertEqual(py_is_legal, bson_is_legal, data)

    @unittest.skipIf(PY3, "python3 has strong separation between bytes/unicode")
    def test_legal_utf8_full_coverage(self):
        # This test takes 400 seconds. Which is too long to run each time.
        # However it is the only one which covers all possible bit combinations
        # in the 244 space.
        b1 = chr(0xf4)

        for b2 in map(chr, range(255)):
            m2 = b1 + b2 
            self._assert_same_utf8_validation(m2)

            for b3 in map(chr, range(255)):
                m3 = m2 + b3 
                self._assert_same_utf8_validation(m3)

                for b4 in map(chr, range(255)):
                    m4 = m3 + b4 
                    self._assert_same_utf8_validation(m4)

    # In python3:
    #  - 'bytes' are not checked with isLegalutf
    #  - 'unicode' We cannot create unicode objects with invalid utf8, since it
    #    would result in non valid code-points.
    @unittest.skipIf(PY3, "python3 has strong separation between bytes/unicode")
    def test_legal_utf8_few_samples(self):
        good_samples = [
            '\xf4\x80\x80\x80',
            '\xf4\x8a\x80\x80',
            '\xf4\x8e\x80\x80',
            '\xf4\x81\x80\x80',
        ]

        for data in good_samples:
            self._assert_same_utf8_validation(data)

        bad_samples = [
            '\xf4\x00\x80\x80',
            '\xf4\x3a\x80\x80',
            '\xf4\x7f\x80\x80',
            '\xf4\x90\x80\x80',
            '\xf4\xff\x80\x80',
        ]

        for data in bad_samples:
            self._assert_same_utf8_validation(data)

if __name__ == "__main__":
    unittest.main()
