# Copyright 2017 MongoDB, Inc.
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

"""MongoDB documentation examples in Python."""

import datetime
import sys
import threading

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import rs_client, wait_until

import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.server_api import ServerApi
from pymongo.write_concern import WriteConcern


class TestSampleShellCommands(IntegrationTest):
    @classmethod
    def setUpClass(cls):
        super(TestSampleShellCommands, cls).setUpClass()
        # Run once before any tests run.
        cls.db.inventory.drop()

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database("pymongo_test")

    def tearDown(self):
        # Run after every test.
        self.db.inventory.drop()

    def test_first_three_examples(self):
        db = self.db

        # Start Example 1
        db.inventory.insert_one(
            {
                "item": "canvas",
                "qty": 100,
                "tags": ["cotton"],
                "size": {"h": 28, "w": 35.5, "uom": "cm"},
            }
        )
        # End Example 1

        self.assertEqual(db.inventory.count_documents({}), 1)

        # Start Example 2
        cursor = db.inventory.find({"item": "canvas"})
        # End Example 2

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 3
        db.inventory.insert_many(
            [
                {
                    "item": "journal",
                    "qty": 25,
                    "tags": ["blank", "red"],
                    "size": {"h": 14, "w": 21, "uom": "cm"},
                },
                {
                    "item": "mat",
                    "qty": 85,
                    "tags": ["gray"],
                    "size": {"h": 27.9, "w": 35.5, "uom": "cm"},
                },
                {
                    "item": "mousepad",
                    "qty": 25,
                    "tags": ["gel", "blue"],
                    "size": {"h": 19, "w": 22.85, "uom": "cm"},
                },
            ]
        )
        # End Example 3

        self.assertEqual(db.inventory.count_documents({}), 4)

    def test_query_top_level_fields(self):
        db = self.db

        # Start Example 6
        db.inventory.insert_many(
            [
                {
                    "item": "journal",
                    "qty": 25,
                    "size": {"h": 14, "w": 21, "uom": "cm"},
                    "status": "A",
                },
                {
                    "item": "notebook",
                    "qty": 50,
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "status": "A",
                },
                {
                    "item": "paper",
                    "qty": 100,
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "status": "D",
                },
                {
                    "item": "planner",
                    "qty": 75,
                    "size": {"h": 22.85, "w": 30, "uom": "cm"},
                    "status": "D",
                },
                {
                    "item": "postcard",
                    "qty": 45,
                    "size": {"h": 10, "w": 15.25, "uom": "cm"},
                    "status": "A",
                },
            ]
        )
        # End Example 6

        self.assertEqual(db.inventory.count_documents({}), 5)

        # Start Example 7
        cursor = db.inventory.find({})
        # End Example 7

        self.assertEqual(len(list(cursor)), 5)

        # Start Example 9
        cursor = db.inventory.find({"status": "D"})
        # End Example 9

        self.assertEqual(len(list(cursor)), 2)

        # Start Example 10
        cursor = db.inventory.find({"status": {"$in": ["A", "D"]}})
        # End Example 10

        self.assertEqual(len(list(cursor)), 5)

        # Start Example 11
        cursor = db.inventory.find({"status": "A", "qty": {"$lt": 30}})
        # End Example 11

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 12
        cursor = db.inventory.find({"$or": [{"status": "A"}, {"qty": {"$lt": 30}}]})
        # End Example 12

        self.assertEqual(len(list(cursor)), 3)

        # Start Example 13
        cursor = db.inventory.find(
            {"status": "A", "$or": [{"qty": {"$lt": 30}}, {"item": {"$regex": "^p"}}]}
        )
        # End Example 13

        self.assertEqual(len(list(cursor)), 2)

    def test_query_embedded_documents(self):
        db = self.db

        # Start Example 14
        # Subdocument key order matters in a few of these examples so we have
        # to use bson.son.SON instead of a Python dict.
        from bson.son import SON

        db.inventory.insert_many(
            [
                {
                    "item": "journal",
                    "qty": 25,
                    "size": SON([("h", 14), ("w", 21), ("uom", "cm")]),
                    "status": "A",
                },
                {
                    "item": "notebook",
                    "qty": 50,
                    "size": SON([("h", 8.5), ("w", 11), ("uom", "in")]),
                    "status": "A",
                },
                {
                    "item": "paper",
                    "qty": 100,
                    "size": SON([("h", 8.5), ("w", 11), ("uom", "in")]),
                    "status": "D",
                },
                {
                    "item": "planner",
                    "qty": 75,
                    "size": SON([("h", 22.85), ("w", 30), ("uom", "cm")]),
                    "status": "D",
                },
                {
                    "item": "postcard",
                    "qty": 45,
                    "size": SON([("h", 10), ("w", 15.25), ("uom", "cm")]),
                    "status": "A",
                },
            ]
        )
        # End Example 14

        # Start Example 15
        cursor = db.inventory.find({"size": SON([("h", 14), ("w", 21), ("uom", "cm")])})
        # End Example 15

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 16
        cursor = db.inventory.find({"size": SON([("w", 21), ("h", 14), ("uom", "cm")])})
        # End Example 16

        self.assertEqual(len(list(cursor)), 0)

        # Start Example 17
        cursor = db.inventory.find({"size.uom": "in"})
        # End Example 17

        self.assertEqual(len(list(cursor)), 2)

        # Start Example 18
        cursor = db.inventory.find({"size.h": {"$lt": 15}})
        # End Example 18

        self.assertEqual(len(list(cursor)), 4)

        # Start Example 19
        cursor = db.inventory.find({"size.h": {"$lt": 15}, "size.uom": "in", "status": "D"})
        # End Example 19

        self.assertEqual(len(list(cursor)), 1)

    def test_query_arrays(self):
        db = self.db

        # Start Example 20
        db.inventory.insert_many(
            [
                {"item": "journal", "qty": 25, "tags": ["blank", "red"], "dim_cm": [14, 21]},
                {"item": "notebook", "qty": 50, "tags": ["red", "blank"], "dim_cm": [14, 21]},
                {
                    "item": "paper",
                    "qty": 100,
                    "tags": ["red", "blank", "plain"],
                    "dim_cm": [14, 21],
                },
                {"item": "planner", "qty": 75, "tags": ["blank", "red"], "dim_cm": [22.85, 30]},
                {"item": "postcard", "qty": 45, "tags": ["blue"], "dim_cm": [10, 15.25]},
            ]
        )
        # End Example 20

        # Start Example 21
        cursor = db.inventory.find({"tags": ["red", "blank"]})
        # End Example 21

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 22
        cursor = db.inventory.find({"tags": {"$all": ["red", "blank"]}})
        # End Example 22

        self.assertEqual(len(list(cursor)), 4)

        # Start Example 23
        cursor = db.inventory.find({"tags": "red"})
        # End Example 23

        self.assertEqual(len(list(cursor)), 4)

        # Start Example 24
        cursor = db.inventory.find({"dim_cm": {"$gt": 25}})
        # End Example 24

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 25
        cursor = db.inventory.find({"dim_cm": {"$gt": 15, "$lt": 20}})
        # End Example 25

        self.assertEqual(len(list(cursor)), 4)

        # Start Example 26
        cursor = db.inventory.find({"dim_cm": {"$elemMatch": {"$gt": 22, "$lt": 30}}})
        # End Example 26

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 27
        cursor = db.inventory.find({"dim_cm.1": {"$gt": 25}})
        # End Example 27

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 28
        cursor = db.inventory.find({"tags": {"$size": 3}})
        # End Example 28

        self.assertEqual(len(list(cursor)), 1)

    def test_query_array_of_documents(self):
        db = self.db

        # Start Example 29
        # Subdocument key order matters in a few of these examples so we have
        # to use bson.son.SON instead of a Python dict.
        from bson.son import SON

        db.inventory.insert_many(
            [
                {
                    "item": "journal",
                    "instock": [
                        SON([("warehouse", "A"), ("qty", 5)]),
                        SON([("warehouse", "C"), ("qty", 15)]),
                    ],
                },
                {"item": "notebook", "instock": [SON([("warehouse", "C"), ("qty", 5)])]},
                {
                    "item": "paper",
                    "instock": [
                        SON([("warehouse", "A"), ("qty", 60)]),
                        SON([("warehouse", "B"), ("qty", 15)]),
                    ],
                },
                {
                    "item": "planner",
                    "instock": [
                        SON([("warehouse", "A"), ("qty", 40)]),
                        SON([("warehouse", "B"), ("qty", 5)]),
                    ],
                },
                {
                    "item": "postcard",
                    "instock": [
                        SON([("warehouse", "B"), ("qty", 15)]),
                        SON([("warehouse", "C"), ("qty", 35)]),
                    ],
                },
            ]
        )
        # End Example 29

        # Start Example 30
        cursor = db.inventory.find({"instock": SON([("warehouse", "A"), ("qty", 5)])})
        # End Example 30

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 31
        cursor = db.inventory.find({"instock": SON([("qty", 5), ("warehouse", "A")])})
        # End Example 31

        self.assertEqual(len(list(cursor)), 0)

        # Start Example 32
        cursor = db.inventory.find({"instock.0.qty": {"$lte": 20}})
        # End Example 32

        self.assertEqual(len(list(cursor)), 3)

        # Start Example 33
        cursor = db.inventory.find({"instock.qty": {"$lte": 20}})
        # End Example 33

        self.assertEqual(len(list(cursor)), 5)

        # Start Example 34
        cursor = db.inventory.find({"instock": {"$elemMatch": {"qty": 5, "warehouse": "A"}}})
        # End Example 34

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 35
        cursor = db.inventory.find({"instock": {"$elemMatch": {"qty": {"$gt": 10, "$lte": 20}}}})
        # End Example 35

        self.assertEqual(len(list(cursor)), 3)

        # Start Example 36
        cursor = db.inventory.find({"instock.qty": {"$gt": 10, "$lte": 20}})
        # End Example 36

        self.assertEqual(len(list(cursor)), 4)

        # Start Example 37
        cursor = db.inventory.find({"instock.qty": 5, "instock.warehouse": "A"})
        # End Example 37

        self.assertEqual(len(list(cursor)), 2)

    def test_query_null(self):
        db = self.db

        # Start Example 38
        db.inventory.insert_many([{"_id": 1, "item": None}, {"_id": 2}])
        # End Example 38

        # Start Example 39
        cursor = db.inventory.find({"item": None})
        # End Example 39

        self.assertEqual(len(list(cursor)), 2)

        # Start Example 40
        cursor = db.inventory.find({"item": {"$type": 10}})
        # End Example 40

        self.assertEqual(len(list(cursor)), 1)

        # Start Example 41
        cursor = db.inventory.find({"item": {"$exists": False}})
        # End Example 41

        self.assertEqual(len(list(cursor)), 1)

    def test_projection(self):
        db = self.db

        # Start Example 42
        db.inventory.insert_many(
            [
                {
                    "item": "journal",
                    "status": "A",
                    "size": {"h": 14, "w": 21, "uom": "cm"},
                    "instock": [{"warehouse": "A", "qty": 5}],
                },
                {
                    "item": "notebook",
                    "status": "A",
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "instock": [{"warehouse": "C", "qty": 5}],
                },
                {
                    "item": "paper",
                    "status": "D",
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "instock": [{"warehouse": "A", "qty": 60}],
                },
                {
                    "item": "planner",
                    "status": "D",
                    "size": {"h": 22.85, "w": 30, "uom": "cm"},
                    "instock": [{"warehouse": "A", "qty": 40}],
                },
                {
                    "item": "postcard",
                    "status": "A",
                    "size": {"h": 10, "w": 15.25, "uom": "cm"},
                    "instock": [{"warehouse": "B", "qty": 15}, {"warehouse": "C", "qty": 35}],
                },
            ]
        )
        # End Example 42

        # Start Example 43
        cursor = db.inventory.find({"status": "A"})
        # End Example 43

        self.assertEqual(len(list(cursor)), 3)

        # Start Example 44
        cursor = db.inventory.find({"status": "A"}, {"item": 1, "status": 1})
        # End Example 44

        for doc in cursor:
            self.assertTrue("_id" in doc)
            self.assertTrue("item" in doc)
            self.assertTrue("status" in doc)
            self.assertFalse("size" in doc)
            self.assertFalse("instock" in doc)

        # Start Example 45
        cursor = db.inventory.find({"status": "A"}, {"item": 1, "status": 1, "_id": 0})
        # End Example 45

        for doc in cursor:
            self.assertFalse("_id" in doc)
            self.assertTrue("item" in doc)
            self.assertTrue("status" in doc)
            self.assertFalse("size" in doc)
            self.assertFalse("instock" in doc)

        # Start Example 46
        cursor = db.inventory.find({"status": "A"}, {"status": 0, "instock": 0})
        # End Example 46

        for doc in cursor:
            self.assertTrue("_id" in doc)
            self.assertTrue("item" in doc)
            self.assertFalse("status" in doc)
            self.assertTrue("size" in doc)
            self.assertFalse("instock" in doc)

        # Start Example 47
        cursor = db.inventory.find({"status": "A"}, {"item": 1, "status": 1, "size.uom": 1})
        # End Example 47

        for doc in cursor:
            self.assertTrue("_id" in doc)
            self.assertTrue("item" in doc)
            self.assertTrue("status" in doc)
            self.assertTrue("size" in doc)
            self.assertFalse("instock" in doc)
            size = doc["size"]
            self.assertTrue("uom" in size)
            self.assertFalse("h" in size)
            self.assertFalse("w" in size)

        # Start Example 48
        cursor = db.inventory.find({"status": "A"}, {"size.uom": 0})
        # End Example 48

        for doc in cursor:
            self.assertTrue("_id" in doc)
            self.assertTrue("item" in doc)
            self.assertTrue("status" in doc)
            self.assertTrue("size" in doc)
            self.assertTrue("instock" in doc)
            size = doc["size"]
            self.assertFalse("uom" in size)
            self.assertTrue("h" in size)
            self.assertTrue("w" in size)

        # Start Example 49
        cursor = db.inventory.find({"status": "A"}, {"item": 1, "status": 1, "instock.qty": 1})
        # End Example 49

        for doc in cursor:
            self.assertTrue("_id" in doc)
            self.assertTrue("item" in doc)
            self.assertTrue("status" in doc)
            self.assertFalse("size" in doc)
            self.assertTrue("instock" in doc)
            for subdoc in doc["instock"]:
                self.assertFalse("warehouse" in subdoc)
                self.assertTrue("qty" in subdoc)

        # Start Example 50
        cursor = db.inventory.find(
            {"status": "A"}, {"item": 1, "status": 1, "instock": {"$slice": -1}}
        )
        # End Example 50

        for doc in cursor:
            self.assertTrue("_id" in doc)
            self.assertTrue("item" in doc)
            self.assertTrue("status" in doc)
            self.assertFalse("size" in doc)
            self.assertTrue("instock" in doc)
            self.assertEqual(len(doc["instock"]), 1)

    def test_update_and_replace(self):
        db = self.db

        # Start Example 51
        db.inventory.insert_many(
            [
                {
                    "item": "canvas",
                    "qty": 100,
                    "size": {"h": 28, "w": 35.5, "uom": "cm"},
                    "status": "A",
                },
                {
                    "item": "journal",
                    "qty": 25,
                    "size": {"h": 14, "w": 21, "uom": "cm"},
                    "status": "A",
                },
                {
                    "item": "mat",
                    "qty": 85,
                    "size": {"h": 27.9, "w": 35.5, "uom": "cm"},
                    "status": "A",
                },
                {
                    "item": "mousepad",
                    "qty": 25,
                    "size": {"h": 19, "w": 22.85, "uom": "cm"},
                    "status": "P",
                },
                {
                    "item": "notebook",
                    "qty": 50,
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "status": "P",
                },
                {
                    "item": "paper",
                    "qty": 100,
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "status": "D",
                },
                {
                    "item": "planner",
                    "qty": 75,
                    "size": {"h": 22.85, "w": 30, "uom": "cm"},
                    "status": "D",
                },
                {
                    "item": "postcard",
                    "qty": 45,
                    "size": {"h": 10, "w": 15.25, "uom": "cm"},
                    "status": "A",
                },
                {
                    "item": "sketchbook",
                    "qty": 80,
                    "size": {"h": 14, "w": 21, "uom": "cm"},
                    "status": "A",
                },
                {
                    "item": "sketch pad",
                    "qty": 95,
                    "size": {"h": 22.85, "w": 30.5, "uom": "cm"},
                    "status": "A",
                },
            ]
        )
        # End Example 51

        # Start Example 52
        db.inventory.update_one(
            {"item": "paper"},
            {"$set": {"size.uom": "cm", "status": "P"}, "$currentDate": {"lastModified": True}},
        )
        # End Example 52

        for doc in db.inventory.find({"item": "paper"}):
            self.assertEqual(doc["size"]["uom"], "cm")
            self.assertEqual(doc["status"], "P")
            self.assertTrue("lastModified" in doc)

        # Start Example 53
        db.inventory.update_many(
            {"qty": {"$lt": 50}},
            {"$set": {"size.uom": "in", "status": "P"}, "$currentDate": {"lastModified": True}},
        )
        # End Example 53

        for doc in db.inventory.find({"qty": {"$lt": 50}}):
            self.assertEqual(doc["size"]["uom"], "in")
            self.assertEqual(doc["status"], "P")
            self.assertTrue("lastModified" in doc)

        # Start Example 54
        db.inventory.replace_one(
            {"item": "paper"},
            {
                "item": "paper",
                "instock": [{"warehouse": "A", "qty": 60}, {"warehouse": "B", "qty": 40}],
            },
        )
        # End Example 54

        for doc in db.inventory.find({"item": "paper"}, {"_id": 0}):
            self.assertEqual(len(doc.keys()), 2)
            self.assertTrue("item" in doc)
            self.assertTrue("instock" in doc)
            self.assertEqual(len(doc["instock"]), 2)

    def test_delete(self):
        db = self.db

        # Start Example 55
        db.inventory.insert_many(
            [
                {
                    "item": "journal",
                    "qty": 25,
                    "size": {"h": 14, "w": 21, "uom": "cm"},
                    "status": "A",
                },
                {
                    "item": "notebook",
                    "qty": 50,
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "status": "P",
                },
                {
                    "item": "paper",
                    "qty": 100,
                    "size": {"h": 8.5, "w": 11, "uom": "in"},
                    "status": "D",
                },
                {
                    "item": "planner",
                    "qty": 75,
                    "size": {"h": 22.85, "w": 30, "uom": "cm"},
                    "status": "D",
                },
                {
                    "item": "postcard",
                    "qty": 45,
                    "size": {"h": 10, "w": 15.25, "uom": "cm"},
                    "status": "A",
                },
            ]
        )
        # End Example 55

        self.assertEqual(db.inventory.count_documents({}), 5)

        # Start Example 57
        db.inventory.delete_many({"status": "A"})
        # End Example 57

        self.assertEqual(db.inventory.count_documents({}), 3)

        # Start Example 58
        db.inventory.delete_one({"status": "D"})
        # End Example 58

        self.assertEqual(db.inventory.count_documents({}), 2)

        # Start Example 56
        db.inventory.delete_many({})
        # End Example 56

        self.assertEqual(db.inventory.count_documents({}), 0)

    @client_context.require_replica_set
    @client_context.require_no_mmap
    def test_change_streams(self):
        db = self.db
        done = False

        def insert_docs():
            while not done:
                db.inventory.insert_one({"username": "alice"})
                db.inventory.delete_one({"username": "alice"})

        t = threading.Thread(target=insert_docs)
        t.start()

        try:
            # 1. The database for reactive, real-time applications
            # Start Changestream Example 1
            cursor = db.inventory.watch()
            document = next(cursor)
            # End Changestream Example 1

            # Start Changestream Example 2
            cursor = db.inventory.watch(full_document="updateLookup")
            document = next(cursor)
            # End Changestream Example 2

            # Start Changestream Example 3
            resume_token = cursor.resume_token
            cursor = db.inventory.watch(resume_after=resume_token)
            document = next(cursor)
            # End Changestream Example 3

            # Start Changestream Example 4
            pipeline = [
                {"$match": {"fullDocument.username": "alice"}},
                {"$addFields": {"newField": "this is an added field!"}},
            ]
            cursor = db.inventory.watch(pipeline=pipeline)
            document = next(cursor)
            # End Changestream Example 4
        finally:
            done = True
            t.join()

    def test_aggregate_examples(self):
        db = self.db

        # Start Aggregation Example 1
        db.sales.aggregate([{"$match": {"items.fruit": "banana"}}, {"$sort": {"date": 1}}])
        # End Aggregation Example 1

        # Start Aggregation Example 2
        db.sales.aggregate(
            [
                {"$unwind": "$items"},
                {"$match": {"items.fruit": "banana"}},
                {
                    "$group": {
                        "_id": {"day": {"$dayOfWeek": "$date"}},
                        "count": {"$sum": "$items.quantity"},
                    }
                },
                {"$project": {"dayOfWeek": "$_id.day", "numberSold": "$count", "_id": 0}},
                {"$sort": {"numberSold": 1}},
            ]
        )
        # End Aggregation Example 2

        # Start Aggregation Example 3
        db.sales.aggregate(
            [
                {"$unwind": "$items"},
                {
                    "$group": {
                        "_id": {"day": {"$dayOfWeek": "$date"}},
                        "items_sold": {"$sum": "$items.quantity"},
                        "revenue": {"$sum": {"$multiply": ["$items.quantity", "$items.price"]}},
                    }
                },
                {
                    "$project": {
                        "day": "$_id.day",
                        "revenue": 1,
                        "items_sold": 1,
                        "discount": {
                            "$cond": {"if": {"$lte": ["$revenue", 250]}, "then": 25, "else": 0}
                        },
                    }
                },
            ]
        )
        # End Aggregation Example 3

        # Start Aggregation Example 4
        db.air_alliances.aggregate(
            [
                {
                    "$lookup": {
                        "from": "air_airlines",
                        "let": {"constituents": "$airlines"},
                        "pipeline": [{"$match": {"$expr": {"$in": ["$name", "$$constituents"]}}}],
                        "as": "airlines",
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "name": 1,
                        "airlines": {
                            "$filter": {
                                "input": "$airlines",
                                "as": "airline",
                                "cond": {"$eq": ["$$airline.country", "Canada"]},
                            }
                        },
                    }
                },
            ]
        )
        # End Aggregation Example 4

    def test_commands(self):
        db = self.db
        db.restaurants.insert_one({})

        # Start runCommand Example 1
        db.command("buildInfo")
        # End runCommand Example 1

        # Start runCommand Example 2
        db.command("collStats", "restaurants")
        # End runCommand Example 2

    def test_index_management(self):
        db = self.db

        # Start Index Example 1
        db.records.create_index("score")
        # End Index Example 1

        # Start Index Example 1
        db.restaurants.create_index(
            [("cuisine", pymongo.ASCENDING), ("name", pymongo.ASCENDING)],
            partialFilterExpression={"rating": {"$gt": 5}},
        )
        # End Index Example 1

    @client_context.require_replica_set
    def test_misc(self):
        # Marketing examples
        client = self.client
        self.addCleanup(client.drop_database, "test")
        self.addCleanup(client.drop_database, "my_database")

        # 2. Tunable consistency controls
        collection = client.my_database.my_collection
        with client.start_session() as session:
            collection.insert_one({"_id": 1}, session=session)
            collection.update_one({"_id": 1}, {"$set": {"a": 1}}, session=session)
            for doc in collection.find({}, session=session):
                pass

        # 3. Exploiting the power of arrays
        collection = client.test.array_updates_test
        collection.update_one({"_id": 1}, {"$set": {"a.$[i].b": 2}}, array_filters=[{"i.b": 0}])


class TestTransactionExamples(IntegrationTest):
    @client_context.require_transactions
    def test_transactions(self):
        # Transaction examples
        client = self.client
        self.addCleanup(client.drop_database, "hr")
        self.addCleanup(client.drop_database, "reporting")

        employees = client.hr.employees
        events = client.reporting.events
        employees.insert_one({"employee": 3, "status": "Active"})
        events.insert_one({"employee": 3, "status": {"new": "Active", "old": None}})

        # Start Transactions Intro Example 1

        def update_employee_info(session):
            employees_coll = session.client.hr.employees
            events_coll = session.client.reporting.events

            with session.start_transaction(
                read_concern=ReadConcern("snapshot"), write_concern=WriteConcern(w="majority")
            ):
                employees_coll.update_one(
                    {"employee": 3}, {"$set": {"status": "Inactive"}}, session=session
                )
                events_coll.insert_one(
                    {"employee": 3, "status": {"new": "Inactive", "old": "Active"}}, session=session
                )

                while True:
                    try:
                        # Commit uses write concern set at transaction start.
                        session.commit_transaction()
                        print("Transaction committed.")
                        break
                    except (ConnectionFailure, OperationFailure) as exc:
                        # Can retry commit
                        if exc.has_error_label("UnknownTransactionCommitResult"):
                            print("UnknownTransactionCommitResult, retrying commit operation ...")
                            continue
                        else:
                            print("Error during commit ...")
                            raise

        # End Transactions Intro Example 1

        with client.start_session() as session:
            update_employee_info(session)

        employee = employees.find_one({"employee": 3})
        assert employee is not None
        self.assertIsNotNone(employee)
        self.assertEqual(employee["status"], "Inactive")

        # Start Transactions Retry Example 1
        def run_transaction_with_retry(txn_func, session):
            while True:
                try:
                    txn_func(session)  # performs transaction
                    break
                except (ConnectionFailure, OperationFailure) as exc:
                    print("Transaction aborted. Caught exception during transaction.")

                    # If transient error, retry the whole transaction
                    if exc.has_error_label("TransientTransactionError"):
                        print("TransientTransactionError, retrying transaction ...")
                        continue
                    else:
                        raise

        # End Transactions Retry Example 1

        with client.start_session() as session:
            run_transaction_with_retry(update_employee_info, session)

        employee = employees.find_one({"employee": 3})
        assert employee is not None
        self.assertIsNotNone(employee)
        self.assertEqual(employee["status"], "Inactive")

        # Start Transactions Retry Example 2
        def commit_with_retry(session):
            while True:
                try:
                    # Commit uses write concern set at transaction start.
                    session.commit_transaction()
                    print("Transaction committed.")
                    break
                except (ConnectionFailure, OperationFailure) as exc:
                    # Can retry commit
                    if exc.has_error_label("UnknownTransactionCommitResult"):
                        print("UnknownTransactionCommitResult, retrying commit operation ...")
                        continue
                    else:
                        print("Error during commit ...")
                        raise

        # End Transactions Retry Example 2

        # Test commit_with_retry from the previous examples
        def _insert_employee_retry_commit(session):
            with session.start_transaction():
                employees.insert_one({"employee": 4, "status": "Active"}, session=session)
                events.insert_one(
                    {"employee": 4, "status": {"new": "Active", "old": None}}, session=session
                )

                commit_with_retry(session)

        with client.start_session() as session:
            run_transaction_with_retry(_insert_employee_retry_commit, session)

        employee = employees.find_one({"employee": 4})
        assert employee is not None
        self.assertIsNotNone(employee)
        self.assertEqual(employee["status"], "Active")

        # Start Transactions Retry Example 3

        def run_transaction_with_retry(txn_func, session):
            while True:
                try:
                    txn_func(session)  # performs transaction
                    break
                except (ConnectionFailure, OperationFailure) as exc:
                    # If transient error, retry the whole transaction
                    if exc.has_error_label("TransientTransactionError"):
                        print("TransientTransactionError, retrying transaction ...")
                        continue
                    else:
                        raise

        def commit_with_retry(session):
            while True:
                try:
                    # Commit uses write concern set at transaction start.
                    session.commit_transaction()
                    print("Transaction committed.")
                    break
                except (ConnectionFailure, OperationFailure) as exc:
                    # Can retry commit
                    if exc.has_error_label("UnknownTransactionCommitResult"):
                        print("UnknownTransactionCommitResult, retrying commit operation ...")
                        continue
                    else:
                        print("Error during commit ...")
                        raise

        # Updates two collections in a transactions

        def update_employee_info(session):
            employees_coll = session.client.hr.employees
            events_coll = session.client.reporting.events

            with session.start_transaction(
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern(w="majority"),
                read_preference=ReadPreference.PRIMARY,
            ):
                employees_coll.update_one(
                    {"employee": 3}, {"$set": {"status": "Inactive"}}, session=session
                )
                events_coll.insert_one(
                    {"employee": 3, "status": {"new": "Inactive", "old": "Active"}}, session=session
                )

                commit_with_retry(session)

        # Start a session.
        with client.start_session() as session:
            try:
                run_transaction_with_retry(update_employee_info, session)
            except Exception as exc:
                # Do something with error.
                raise

        # End Transactions Retry Example 3

        employee = employees.find_one({"employee": 3})
        assert employee is not None
        self.assertIsNotNone(employee)
        self.assertEqual(employee["status"], "Inactive")

        MongoClient = lambda _: rs_client()
        uriString = None

        # Start Transactions withTxn API Example 1

        # For a replica set, include the replica set name and a seedlist of the members in the URI string; e.g.
        # uriString = 'mongodb://mongodb0.example.com:27017,mongodb1.example.com:27017/?replicaSet=myRepl'
        # For a sharded cluster, connect to the mongos instances; e.g.
        # uriString = 'mongodb://mongos0.example.com:27017,mongos1.example.com:27017/'

        client = MongoClient(uriString)
        wc_majority = WriteConcern("majority", wtimeout=1000)

        # Prereq: Create collections.
        client.get_database("mydb1", write_concern=wc_majority).foo.insert_one({"abc": 0})
        client.get_database("mydb2", write_concern=wc_majority).bar.insert_one({"xyz": 0})

        # Step 1: Define the callback that specifies the sequence of operations to perform inside the transactions.
        def callback(session):
            collection_one = session.client.mydb1.foo
            collection_two = session.client.mydb2.bar

            # Important:: You must pass the session to the operations.
            collection_one.insert_one({"abc": 1}, session=session)
            collection_two.insert_one({"xyz": 999}, session=session)

        # Step 2: Start a client session.
        with client.start_session() as session:
            # Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
            session.with_transaction(
                callback,
                read_concern=ReadConcern("local"),
                write_concern=wc_majority,
                read_preference=ReadPreference.PRIMARY,
            )

        # End Transactions withTxn API Example 1


class TestCausalConsistencyExamples(IntegrationTest):
    @client_context.require_secondaries_count(1)
    @client_context.require_no_mmap
    def test_causal_consistency(self):
        # Causal consistency examples
        client = self.client
        self.addCleanup(client.drop_database, "test")
        client.test.drop_collection("items")
        client.test.items.insert_one(
            {"sku": "111", "name": "Peanuts", "start": datetime.datetime.today()}
        )

        # Start Causal Consistency Example 1
        with client.start_session(causal_consistency=True) as s1:
            current_date = datetime.datetime.today()
            items = client.get_database(
                "test",
                read_concern=ReadConcern("majority"),
                write_concern=WriteConcern("majority", wtimeout=1000),
            ).items
            items.update_one(
                {"sku": "111", "end": None}, {"$set": {"end": current_date}}, session=s1
            )
            items.insert_one(
                {"sku": "nuts-111", "name": "Pecans", "start": current_date}, session=s1
            )
        # End Causal Consistency Example 1

        assert s1.cluster_time is not None
        assert s1.operation_time is not None

        # Start Causal Consistency Example 2
        with client.start_session(causal_consistency=True) as s2:
            s2.advance_cluster_time(s1.cluster_time)
            s2.advance_operation_time(s1.operation_time)

            items = client.get_database(
                "test",
                read_preference=ReadPreference.SECONDARY,
                read_concern=ReadConcern("majority"),
                write_concern=WriteConcern("majority", wtimeout=1000),
            ).items
            for item in items.find({"end": None}, session=s2):
                print(item)
        # End Causal Consistency Example 2


class TestVersionedApiExamples(IntegrationTest):
    @client_context.require_version_min(4, 7)
    def test_versioned_api(self):
        # Versioned API examples
        MongoClient = lambda _, server_api: rs_client(server_api=server_api, connect=False)
        uri = None

        # Start Versioned API Example 1
        from pymongo.server_api import ServerApi

        client = MongoClient(uri, server_api=ServerApi("1"))
        # End Versioned API Example 1

        # Start Versioned API Example 2
        client = MongoClient(uri, server_api=ServerApi("1", strict=True))
        # End Versioned API Example 2

        # Start Versioned API Example 3
        client = MongoClient(uri, server_api=ServerApi("1", strict=False))
        # End Versioned API Example 3

        # Start Versioned API Example 4
        client = MongoClient(uri, server_api=ServerApi("1", deprecation_errors=True))
        # End Versioned API Example 4

    @unittest.skip("PYTHON-3167 count has been added to API version 1")
    @client_context.require_version_min(4, 7)
    def test_versioned_api_migration(self):
        # SERVER-58785
        if client_context.is_topology_type(["sharded"]) and not client_context.version.at_least(
            5, 0, 2
        ):
            self.skipTest("This test needs MongoDB 5.0.2 or newer")

        client = rs_client(server_api=ServerApi("1", strict=True))
        client.db.sales.drop()

        # Start Versioned API Example 5
        def strptime(s):
            return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")

        client.db.sales.insert_many(
            [
                {
                    "_id": 1,
                    "item": "abc",
                    "price": 10,
                    "quantity": 2,
                    "date": strptime("2021-01-01T08:00:00Z"),
                },
                {
                    "_id": 2,
                    "item": "jkl",
                    "price": 20,
                    "quantity": 1,
                    "date": strptime("2021-02-03T09:00:00Z"),
                },
                {
                    "_id": 3,
                    "item": "xyz",
                    "price": 5,
                    "quantity": 5,
                    "date": strptime("2021-02-03T09:05:00Z"),
                },
                {
                    "_id": 4,
                    "item": "abc",
                    "price": 10,
                    "quantity": 10,
                    "date": strptime("2021-02-15T08:00:00Z"),
                },
                {
                    "_id": 5,
                    "item": "xyz",
                    "price": 5,
                    "quantity": 10,
                    "date": strptime("2021-02-15T09:05:00Z"),
                },
                {
                    "_id": 6,
                    "item": "xyz",
                    "price": 5,
                    "quantity": 5,
                    "date": strptime("2021-02-15T12:05:10Z"),
                },
                {
                    "_id": 7,
                    "item": "xyz",
                    "price": 5,
                    "quantity": 10,
                    "date": strptime("2021-02-15T14:12:12Z"),
                },
                {
                    "_id": 8,
                    "item": "abc",
                    "price": 10,
                    "quantity": 5,
                    "date": strptime("2021-03-16T20:20:13Z"),
                },
            ]
        )
        # End Versioned API Example 5

        with self.assertRaisesRegex(
            OperationFailure,
            "Provided apiStrict:true, but the command count is not in API Version 1",
        ):
            client.db.command("count", "sales", query={})
        # Start Versioned API Example 6
        # pymongo.errors.OperationFailure: Provided apiStrict:true, but the command count is not in API Version 1, full error: {'ok': 0.0, 'errmsg': 'Provided apiStrict:true, but the command count is not in API Version 1', 'code': 323, 'codeName': 'APIStrictError'}
        # End Versioned API Example 6

        # Start Versioned API Example 7
        client.db.sales.count_documents({})
        # End Versioned API Example 7

        # Start Versioned API Example 8
        # 8
        # End Versioned API Example 8


class TestSnapshotQueryExamples(IntegrationTest):
    @client_context.require_version_min(5, 0)
    def test_snapshot_query(self):
        client = self.client

        if not client_context.is_topology_type(["replicaset", "sharded"]):
            self.skipTest("Must be a sharded or replicaset")

        self.addCleanup(client.drop_database, "pets")
        db = client.pets
        db.drop_collection("cats")
        db.drop_collection("dogs")
        db.cats.insert_one({"name": "Whiskers", "color": "white", "age": 10, "adoptable": True})
        db.dogs.insert_one({"name": "Pebbles", "color": "Brown", "age": 10, "adoptable": True})
        wait_until(lambda: self.check_for_snapshot(db.cats), "success")
        wait_until(lambda: self.check_for_snapshot(db.dogs), "success")

        # Start Snapshot Query Example 1

        db = client.pets
        with client.start_session(snapshot=True) as s:
            adoptablePetsCount = db.cats.aggregate(
                [{"$match": {"adoptable": True}}, {"$count": "adoptableCatsCount"}], session=s
            ).next()["adoptableCatsCount"]

            adoptablePetsCount += db.dogs.aggregate(
                [{"$match": {"adoptable": True}}, {"$count": "adoptableDogsCount"}], session=s
            ).next()["adoptableDogsCount"]

        print(adoptablePetsCount)

        # End Snapshot Query Example 1
        db = client.retail
        self.addCleanup(client.drop_database, "retail")
        db.drop_collection("sales")

        saleDate = datetime.datetime.now()
        db.sales.insert_one({"shoeType": "boot", "price": 30, "saleDate": saleDate})
        wait_until(lambda: self.check_for_snapshot(db.sales), "success")

        # Start Snapshot Query Example 2
        db = client.retail
        with client.start_session(snapshot=True) as s:
            total = db.sales.aggregate(
                [
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    "$saleDate",
                                    {
                                        "$dateSubtract": {
                                            "startDate": "$$NOW",
                                            "unit": "day",
                                            "amount": 1,
                                        }
                                    },
                                ]
                            }
                        }
                    },
                    {"$count": "totalDailySales"},
                ],
                session=s,
            ).next()["totalDailySales"]

        # End Snapshot Query Example 2

    def check_for_snapshot(self, collection):
        """Wait for snapshot reads to become available to prevent this error:
        [246:SnapshotUnavailable]: Unable to read from a snapshot due to pending collection catalog changes; please retry the operation. Snapshot timestamp is Timestamp(1646666892, 4). Collection minimum is Timestamp(1646666892, 5) (on localhost:27017, modern retry, attempt 1)
        From https://github.com/mongodb/mongo-ruby-driver/commit/7c4117b58e3d12e237f7536f7521e18fc15f79ac
        """
        with self.client.start_session(snapshot=True) as s:
            try:
                with collection.aggregate([], session=s):
                    pass
                return True
            except OperationFailure as e:
                # Retry them as the server demands...
                if e.code == 246:  # SnapshotUnavailable
                    return False
                raise


if __name__ == "__main__":
    unittest.main()
