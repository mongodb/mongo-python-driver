import unittest

from pymongo.cursor import Cursor
from pymongo.database import Database
from test_connection import get_connection


class CustomCursor(Cursor):
    def custom_method(self):
        return "kinda workds"


class TestCustomCursors(unittest.TestCase):

    def setUp(self):
        self.db = Database(get_connection(), "pymongo_test")

    def testBasics(self):
        # it is not persisstent now, so we need to set it for every collection instance
        collection = self.db.custom_cursor
        self.assertEquals(collection.find().__class__, Cursor)
        self.assertRaises(TypeError, setattr, collection, 'cursor_class', unicode)
        collection.cursor_class = CustomCursor
        self.assertEquals(collection.find().__class__, CustomCursor)
        self.assertEquals(collection.find().custom_method(), "kinda workds")


if __name__ == "__main__":
    unittest.main()
