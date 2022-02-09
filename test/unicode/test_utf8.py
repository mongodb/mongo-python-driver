import sys

sys.path[0:0] = [""]

from test import unittest

from bson import encode
from bson.errors import InvalidStringData


class TestUTF8(unittest.TestCase):

    # Verify that python and bson have the same understanding of
    # legal utf-8 if the first byte is 0xf4 (244)
    def _assert_same_utf8_validation(self, data):
        try:
            data.decode("utf-8")
            py_is_legal = True
        except UnicodeDecodeError:
            py_is_legal = False

        try:
            encode({"x": data})
            bson_is_legal = True
        except InvalidStringData:
            bson_is_legal = False

        self.assertEqual(py_is_legal, bson_is_legal, data)


if __name__ == "__main__":
    unittest.main()
