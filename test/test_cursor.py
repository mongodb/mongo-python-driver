"""Test the cursor module."""
import unittest

from cursor import Cursor
from database import Database
from test_connection import get_connection

class TestCursor(unittest.TestCase):
    def setUp(self):
        self.db = Database(get_connection(), "test")

    def test_explain(self):
        a = self.db.test.find()
        explanation = a.explain()
        for _ in a:
            break
        self.assertEqual(a.explain(), explanation)
        self.assertTrue("cursor" in explanation)

if __name__ == "__main__":
    unittest.main()
