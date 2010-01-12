# -*- coding: utf-8 -*-

"""Test the errors module."""

import unittest

import pymongo
from pymongo.errors import BaseMongoDBException


class TestErrors(unittest.TestCase):
    def test_base_exception(self):
        self.assertRaises(BaseMongoDBException, pymongo.Connection, port=0)

if __name__ == '__main__':
    unittest.main()
