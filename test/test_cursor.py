"""Test the cursor module."""
import unittest

from cursor import Cursor
from database import Database
from test_connection import get_connection

class TestCursor(unittest.TestCase):
    def setUp(self):
        self.db = Database(get_connection(), "test")

# TODO are there any tests that belong here? a lot of cursor stuff is hard to
# test seperately...

if __name__ == "__main__":
    unittest.main()
