# Copyright 2023-present MongoDB, Inc.
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

"""Test typings in strict mode."""
from __future__ import annotations

import unittest
from typing import TYPE_CHECKING, Any, Dict

import pymongo
from pymongo.collection import Collection
from pymongo.database import Database


def test_generic_arguments() -> None:
    """Ensure known usages of generic arguments pass strict typing"""
    if not TYPE_CHECKING:
        raise unittest.SkipTest("Used for Type Checking Only")
    mongo_client: pymongo.MongoClient[Dict[str, Any]] = pymongo.MongoClient()
    mongo_client.drop_database("foo")
    mongo_client.get_default_database()
    db = mongo_client.get_database("test_db")
    db = Database(mongo_client, "test_db")
    db.with_options()
    db.validate_collection("py_test")
    col = db.get_collection("py_test")
    col.insert_one({"abc": 123})
    col = Collection(db, "py_test")
    col.with_options()
