# Copyright 2016-present MongoDB, Inc.
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

"""Test the collation module."""
from __future__ import annotations

import functools
import warnings
from test.conftest import rs_or_single_client
from test.utils import OvertCommandListener

import pytest

from pymongo.collation import (
    Collation,
    CollationAlternate,
    CollationCaseFirst,
    CollationMaxVariable,
    CollationStrength,
)
from pymongo.errors import ConfigurationError
from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    IndexModel,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
from pymongo.synchronous.helpers import next
from pymongo.write_concern import WriteConcern

_IS_SYNC = True


def test_constructor():
    with pytest.raises(TypeError):
        Collation(locale=42)
    # Fill in a locale to test the other options.
    _Collation = functools.partial(Collation, "en_US")
    # No error.
    _Collation(caseFirst=CollationCaseFirst.UPPER)
    with pytest.raises(TypeError):
        _Collation(caseLevel="true")
    with pytest.raises(ValueError):
        _Collation(strength="six")
    with pytest.raises(TypeError):
        _Collation(numericOrdering="true")
    with pytest.raises(TypeError):
        _Collation(alternate=5)
    with pytest.raises(TypeError):
        _Collation(maxVariable=2)
    with pytest.raises(TypeError):
        _Collation(normalization="false")
    with pytest.raises(TypeError):
        _Collation(backwards="true")
    # No errors.
    Collation("en_US", future_option="bar", another_option=42)
    collation = Collation(
        "en_US",
        caseLevel=True,
        caseFirst=CollationCaseFirst.UPPER,
        strength=CollationStrength.QUATERNARY,
        numericOrdering=True,
        alternate=CollationAlternate.SHIFTED,
        maxVariable=CollationMaxVariable.SPACE,
        normalization=True,
        backwards=True,
    )

    expected_document = {
        "locale": "en_US",
        "caseLevel": True,
        "caseFirst": "upper",
        "strength": 4,
        "numericOrdering": True,
        "alternate": "shifted",
        "maxVariable": "space",
        "normalization": True,
        "backwards": True,
    }
    assert expected_document == collation.document

    assert {"locale": "en_US", "backwards": True} == Collation("en_US",
                                                               backwards=True).document


# Fixture for setup and teardown
@pytest.fixture(loop_scope="function")
# @client_context_fixture.require_connection
def async_client(client_context_fixture):
    listener = OvertCommandListener()
    client = rs_or_single_client(client_context_fixture,
                                             event_listeners=[listener])
    db = client.pymongo_test
    collation = Collation("en_US")
    warn_context = warnings.catch_warnings()
    warn_context.__enter__()

    yield db, collation, listener, warn_context
    warn_context.__exit__(None, None, None)
    warn_context = None
    listener.reset()


@pytest.mark.asyncio
def test_create_collection(async_client):
    db, collation, listener, _ = async_client
    db.test.drop()
    db.create_collection("test", collation=collation)
    assert collation.document == listener.started_events[-1].command[
        "collation"]
    # Test passing collation as dict
    db.test.drop()
    listener.reset()
    db.create_collection("test", collation=collation.document)
    assert collation.document == listener.started_events[-1].command[
        "collation"]


def test_index_model():
    model = IndexModel([("a", 1), ("b", -1)], collation=Collation("en_US"))
    assert Collation("en_US").document == model.document["collation"]


@pytest.mark.asyncio
def test_create_index(async_client):
    db, collation, listener, _ = async_client
    db.test.create_index("foo", collation=collation)
    ci_cmd = listener.started_events[0].command
    assert collation.document == ci_cmd["indexes"][0]["collation"]


@pytest.mark.asyncio
def test_aggregate(async_client):
    db, collation, listener, _ = async_client
    db.test.aggregate([{"$group": {"_id": 42}}], collation=collation)
    assert collation.document == listener.started_events[-1].command[
        "collation"]


@pytest.mark.asyncio
def test_count_documents(async_client):
    db, collation, listener, _ = async_client
    db.test.count_documents({}, collation=collation)
    assert collation.document == listener.started_events[-1].command[
        "collation"]


@pytest.mark.asyncio
def test_distinct(async_client):
    db, collation, listener, _ = async_client
    db.test.distinct("foo", collation=collation)
    assert collation.document == listener.started_events[-1].command[
        "collation"]

    listener.reset()
    db.test.find(collation=collation).distinct("foo")
    assert collation.document == listener.started_events[-1].command[
        "collation"]


@pytest.mark.asyncio
def test_find_command(async_client):
    db, collation, listener, _ = async_client
    db.test.insert_one({"is this thing on?": True})
    listener.reset()
    next(db.test.find(collation=collation))
    assert collation.document == listener.started_events[-1].command[
        "collation"]


@pytest.mark.asyncio
def test_explain_command(async_client):
    db, collation, listener, _ = async_client
    listener.reset()
    db.test.find(collation=collation).explain()
    # The collation should be part of the explained command.
    assert collation.document == listener.started_events[-1].command["explain"][
        "collation"]


@pytest.mark.asyncio
def test_delete(async_client):
    db, collation, listener, _ = async_client
    db.test.delete_one({"foo": 42}, collation=collation)
    command = listener.started_events[0].command
    assert collation.document == command["deletes"][0]["collation"]

    listener.reset()
    db.test.delete_many({"foo": 42}, collation=collation)
    command = listener.started_events[0].command
    assert collation.document == command["deletes"][0]["collation"]


@pytest.mark.asyncio
def test_update(async_client):
    db, collation, listener, _ = async_client
    db.test.replace_one({"foo": 42}, {"foo": 43}, collation=collation)
    command = listener.started_events[0].command
    assert collation.document == command["updates"][0]["collation"]

    listener.reset()
    db.test.update_one({"foo": 42}, {"$set": {"foo": 43}},
                             collation=collation)
    command = listener.started_events[0].command
    assert collation.document == command["updates"][0]["collation"]

    listener.reset()
    db.test.update_many({"foo": 42}, {"$set": {"foo": 43}},
                              collation=collation)
    command = listener.started_events[0].command
    assert collation.document == command["updates"][0]["collation"]


@pytest.mark.asyncio
def test_find_and(async_client):
    db, collation, listener, _ = async_client
    db.test.find_one_and_delete({"foo": 42}, collation=collation)
    assert collation.document == listener.started_events[-1].command[
        "collation"]

    listener.reset()
    db.test.find_one_and_update({"foo": 42}, {"$set": {"foo": 43}},
                                      collation=collation)
    assert collation.document == listener.started_events[-1].command[
        "collation"]

    listener.reset()
    db.test.find_one_and_replace({"foo": 42}, {"foo": 43},
                                       collation=collation)
    assert collation.document == listener.started_events[-1].command[
        "collation"]


@pytest.mark.asyncio
def test_bulk_write(async_client):
    db, collation, listener, _ = async_client
    db.test.collection.bulk_write(
        [
            DeleteOne({"noCollation": 42}),
            DeleteMany({"noCollation": 42}),
            DeleteOne({"foo": 42}, collation=collation),
            DeleteMany({"foo": 42}, collation=collation),
            ReplaceOne({"noCollation": 24}, {"bar": 42}),
            UpdateOne({"noCollation": 84}, {"$set": {"bar": 10}}, upsert=True),
            UpdateMany({"noCollation": 45}, {"$set": {"bar": 42}}),
            ReplaceOne({"foo": 24}, {"foo": 42}, collation=collation),
            UpdateOne({"foo": 84}, {"$set": {"foo": 10}}, upsert=True,
                      collation=collation),
            UpdateMany({"foo": 45}, {"$set": {"foo": 42}}, collation=collation),
        ]
    )
    delete_cmd = listener.started_events[0].command
    update_cmd = listener.started_events[1].command

    def check_ops(ops):
        for op in ops:
            if "noCollation" in op["q"]:
                assert "collation" not in op
            else:
                assert collation.document == op["collation"]

    check_ops(delete_cmd["deletes"])
    check_ops(update_cmd["updates"])


@pytest.mark.asyncio
def test_indexes_same_keys_different_collations(async_client):
    db, _, _, _ = async_client
    db.test.drop()
    usa_collation = Collation("en_US")
    ja_collation = Collation("ja")
    db.test.create_indexes(
        [
            IndexModel("fieldname", collation=usa_collation),
            IndexModel("fieldname", name="japanese_version",
                       collation=ja_collation),
            IndexModel("fieldname", name="simple"),
        ]
    )
    indexes = db.test.index_information()
    assert usa_collation.document["locale"] == \
           indexes["fieldname_1"]["collation"]["locale"]
    assert ja_collation.document["locale"] == \
           indexes["japanese_version"]["collation"]["locale"]
    assert "collation" not in indexes["simple"]
    db.test.drop_index("fieldname_1")
    indexes = db.test.index_information()
    assert "japanese_version" in indexes
    assert "simple" in indexes
    assert "fieldname" not in indexes


@pytest.mark.asyncio
def test_unacknowledged_write(async_client):
    db, collation, _, _ = async_client
    unacknowledged = WriteConcern(w=0)
    collection = db.get_collection("test", write_concern=unacknowledged)
    with pytest.raises(ConfigurationError):
        collection.update_one(
            {"hello": "world"}, {"$set": {"hello": "moon"}}, collation=collation
        )
    update_one = UpdateOne(
        {"hello": "world"}, {"$set": {"hello": "moon"}}, collation=collation
    )
    with pytest.raises(ConfigurationError):
        collection.bulk_write([update_one])


@pytest.mark.asyncio
def test_cursor_collation(async_client):
    db, collation, listener, _ = async_client
    db.test.insert_one({"hello": "world"})
    next(db.test.find().collation(collation))
    assert collation.document == listener.started_events[-1].command[
        "collation"]
