"""Test the geospatial indexes and queries."""

import unittest

from bson.son import SON
from nose.plugins.skip import SkipTest
from pymongo import ASCENDING, GEO2D, GEOHAYSTACK, GEOSPHERE

from test.test_client import get_client
from test.utils import is_mongos
from test import version


class GeoTestBase(unittest.TestCase):

    def setUp(self):
        self.client = get_client()
        self.db = self.client.pymongo_test
        self.db.test.drop()

        # Create test points (using SON to preserve the right order of long & lat)
        self._id1 = self.db.test.insert({
            "loc": SON([("long", 34.2), ("lat", 33.3)]),
            "type": "restaurant"
        })
        self._id2 = self.db.test.insert({
            "loc": SON([("long", 34.2), ("lat", 37.3)]),
            "type": "restaurant"
        })
        self._id3 = self.db.test.insert({
            "loc": SON([("long", 59.1), ("lat", 87.2)]),
            "type": "office"
        })

    def tearDown(self):
        self.db = None
        self.client = None


class Test2DSphere(GeoTestBase):

    def setUp(self):
        super(Test2DSphere, self).setUp()

        if not version.at_least(self.client, (2, 3, 2)):
            raise SkipTest("2dsphere indexing requires server >=2.3.2.")

        self.assertEqual("loc_2dsphere",
                         self.db.test.create_index([("loc", GEOSPHERE)]))

    def test_geo_within_cursor(self):
        poly = {"type": "Polygon",
                "coordinates": [[[40,5], [40,6], [41,6], [41,5], [40,5]]]}
        query = {"loc": {"$within": {"$geometry": poly}}}

        cursor = self.db.test.find(query).explain()['cursor']
        self.assertTrue('S2Cursor' in cursor or 'loc_2dsphere' in cursor)


class Test2D(GeoTestBase):

    def setUp(self):
        super(Test2D, self).setUp()

        self.assertEqual('loc_2d', self.db.test.create_index([("loc", GEO2D)]))
        index_info = self.db.test.index_information()['loc_2d']
        self.assertEqual([('loc', '2d')], index_info['key'])

    def test_geo_within_box(self):
        results = self.db.test.find({
            'loc': {
                '$geoWithin' : {
                    '$box': [ [0, 0], [100, 100] ]
                }
            }
        })
        self.assertEqual(3, results.count())

        results = self.db.test.find({
            'loc': {
                '$geoWithin' : {
                    '$box': [ [30, 30], [40, 40] ]
                }
            }
        })
        self.assertEqual(2, results.count())
        self.assertEqual(set(res['_id'] for res in results), set([self._id1, self._id2]))

    def test_geo_within_polygon(self):
        results = self.db.test.find({
            'loc': {
                '$geoWithin' : {
                    '$polygon': [ [0, 0], [0, 100], [100, 100], [100, 0] ]
                }
            }
        })
        self.assertEqual(3, results.count())

        results = self.db.test.find({
            'loc': {
                '$geoWithin' : {
                    '$polygon': [ [0, 0], [0, 40], [40, 40], [40, 0] ]
                }
            }
        })
        self.assertEqual(2, results.count())
        self.assertEqual(set(res['_id'] for res in results), set([self._id1, self._id2]))

    def test_geo_within_center(self):
        results = self.db.test.find({
            'loc': {
                '$geoWithin' : {
                    '$center': [ [30, 30], 100 ]
                }
            }
        })
        self.assertEqual(3, results.count())

        results = self.db.test.find({
            'loc': {
                '$geoWithin' : {
                    '$center': [ [30, 30], 10 ]
                }
            }
        })
        self.assertEqual(2, results.count())
        self.assertEqual(set(res['_id'] for res in results), set([self._id1, self._id2]))

    def test_near_sphere(self):
        north_id = self.db.test.insert({
            "loc": SON([("long", -122), ("lat", 38)]),  # Near Concord, CA
            "type": "restaurant"
        })

        south_id = self.db.test.insert({
            "loc": SON([("long", -122), ("lat", 37)]),  # Near Santa Cruz, CA
            "type": "restaurant"
        })

        results = self.db.test.find({
            'loc': {
                '$nearSphere': [-122, 38.5]
            }
        })
        self.assertEqual(5, results.count())
        self.assertEqual(results[0]['_id'], north_id)
        self.assertEqual(results[1]['_id'], south_id)


class TestHaystack(GeoTestBase):

    def setUp(self):
        super(TestHaystack, self).setUp()

        if is_mongos(self.db.connection):
            raise SkipTest("geoSearch is not supported by mongos")

        self.db.test.create_index(
            [("loc", GEOHAYSTACK), ("type", ASCENDING)],
            bucket_size=1
        )

    def test_geo_search_command(self):
        results = self.db.command(SON([
            ("geoSearch", "test"),
            ("near", [33, 33]),
            ("maxDistance", 6),
            ("search", {"type": "restaurant"}),
            ("limit", 30),
        ]))['results']

        self.assertEqual(2, len(results))
        self.assertEqual({
            "_id": self._id1,
            "loc": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        }, results[0])

