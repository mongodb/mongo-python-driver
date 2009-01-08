#!/usr/bin/env python

import unittest

import bson
import objectid
import dbref
import mongo

def main():
    bson_suite = unittest.TestLoader().loadTestsFromModule(bson)
    objectid_suite = unittest.TestLoader().loadTestsFromModule(objectid)
    dbref_suite = unittest.TestLoader().loadTestsFromModule(dbref)
    mongo_suite = unittest.TestLoader().loadTestsFromModule(mongo)
    alltests = unittest.TestSuite([bson_suite, objectid_suite, dbref_suite])

    unittest.TextTestRunner().run(objectid_suite)
    unittest.TextTestRunner().run(dbref_suite)
    unittest.TextTestRunner().run(bson_suite)
    unittest.TextTestRunner().run(mongo_suite)

if __name__ == "__main__":
    main()
