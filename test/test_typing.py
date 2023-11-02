# Copyright 2020-present MongoDB, Inc.
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

"""Test that each file in mypy_fails/ actually fails mypy, and test some
sample client code that uses PyMongo typings.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    TypeVar,
    Union,
    cast,
)

try:
    from typing_extensions import NotRequired, TypedDict

    from bson import ObjectId

    class Movie(TypedDict):
        name: str
        year: int

    class MovieWithId(TypedDict):
        _id: ObjectId
        name: str
        year: int

    class ImplicitMovie(TypedDict):
        _id: NotRequired[ObjectId]  # pyright: ignore[reportGeneralTypeIssues]
        name: str
        year: int

except ImportError:
    Movie = dict  # type:ignore[misc,assignment]
    ImplicitMovie = dict  # type: ignore[assignment,misc]
    MovieWithId = dict  # type: ignore[assignment,misc]
    TypedDict = None
    NotRequired = None  # type: ignore[assignment]


try:
    from mypy import api
except ImportError:
    api = None  # type: ignore[assignment]

sys.path[0:0] = [""]

from test import IntegrationTest, client_context
from test.utils import rs_or_single_client

from bson import CodecOptions, decode, decode_all, decode_file_iter, decode_iter, encode
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.operations import DeleteOne, InsertOne, ReplaceOne
from pymongo.read_preferences import ReadPreference

TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "mypy_fails")


def get_tests() -> Iterable[str]:
    for dirpath, _, filenames in os.walk(TEST_PATH):
        for filename in filenames:
            yield os.path.join(dirpath, filename)


FuncT = TypeVar("FuncT", bound=Callable[..., None])


def only_type_check(func: FuncT) -> FuncT:
    def inner(*args, **kwargs):
        if not TYPE_CHECKING:
            raise unittest.SkipTest("Used for Type Checking Only")
        func(*args, **kwargs)

    return cast(FuncT, inner)


class TestMypyFails(unittest.TestCase):
    def ensure_mypy_fails(self, filename: str) -> None:
        if api is None:
            raise unittest.SkipTest("Mypy is not installed")
        stdout, stderr, exit_status = api.run([filename])
        self.assertTrue(exit_status, msg=stdout)

    def test_mypy_failures(self) -> None:
        for filename in get_tests():
            if filename == "typeddict_client.py" and TypedDict is None:
                continue
            with self.subTest(filename=filename):
                self.ensure_mypy_fails(filename)


class TestPymongo(IntegrationTest):
    coll: Collection

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.coll = cls.client.test.test

    def test_insert_find(self) -> None:
        doc = {"my": "doc"}
        coll2 = self.client.test.test2
        result = self.coll.insert_one(doc)
        self.assertEqual(result.inserted_id, doc["_id"])
        retrieved = self.coll.find_one({"_id": doc["_id"]})
        if retrieved:
            # Documents returned from find are mutable.
            retrieved["new_field"] = 1
            result2 = coll2.insert_one(retrieved)
            self.assertEqual(result2.inserted_id, result.inserted_id)

    def test_cursor_iterable(self) -> None:
        def to_list(iterable: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
            return list(iterable)

        self.coll.insert_one({})
        cursor = self.coll.find()
        docs = to_list(cursor)
        self.assertTrue(docs)

    @only_type_check
    def test_bulk_write(self) -> None:
        self.coll.insert_one({})
        coll: Collection[Movie] = self.coll
        requests: List[InsertOne[Movie]] = [InsertOne(Movie(name="American Graffiti", year=1973))]
        self.assertTrue(coll.bulk_write(requests).acknowledged)
        new_requests: List[Union[InsertOne[Movie], ReplaceOne[Movie]]] = []
        input_list: List[Union[InsertOne[Movie], ReplaceOne[Movie]]] = [
            InsertOne(Movie(name="American Graffiti", year=1973)),
            ReplaceOne({}, Movie(name="American Graffiti", year=1973)),
        ]
        for i in input_list:
            new_requests.append(i)
        self.assertTrue(coll.bulk_write(new_requests).acknowledged)

    # Because ReplaceOne is not generic, type checking is not enforced for ReplaceOne in the first example.
    @only_type_check
    def test_bulk_write_heterogeneous(self):
        coll: Collection[Movie] = self.coll
        requests: List[Union[InsertOne[Movie], ReplaceOne, DeleteOne]] = [
            InsertOne(Movie(name="American Graffiti", year=1973)),
            ReplaceOne({}, {"name": "American Graffiti", "year": "WRONG_TYPE"}),
            DeleteOne({}),
        ]
        self.assertTrue(coll.bulk_write(requests).acknowledged)
        requests_two: List[Union[InsertOne[Movie], ReplaceOne[Movie], DeleteOne]] = [
            InsertOne(Movie(name="American Graffiti", year=1973)),
            ReplaceOne(
                {},
                {"name": "American Graffiti", "year": "WRONG_TYPE"},  # type:ignore[typeddict-item]
            ),
            DeleteOne({}),
        ]
        self.assertTrue(coll.bulk_write(requests_two).acknowledged)

    def test_command(self) -> None:
        result: Dict = self.client.admin.command("ping")
        result.items()

    def test_list_collections(self) -> None:
        cursor = self.client.test.list_collections()
        value = cursor.next()
        value.items()

    def test_list_databases(self) -> None:
        cursor = self.client.list_databases()
        value = cursor.next()
        value.items()

    def test_default_document_type(self) -> None:
        client = rs_or_single_client()
        self.addCleanup(client.close)
        coll = client.test.test
        doc = {"my": "doc"}
        coll.insert_one(doc)
        retrieved = coll.find_one({"_id": doc["_id"]})
        assert retrieved is not None
        retrieved["a"] = 1

    def test_aggregate_pipeline(self) -> None:
        coll3 = self.client.test.test3
        coll3.insert_many(
            [
                {"x": 1, "tags": ["dog", "cat"]},
                {"x": 2, "tags": ["cat"]},
                {"x": 2, "tags": ["mouse", "cat", "dog"]},
                {"x": 3, "tags": []},
            ]
        )

        class mydict(Dict[str, Any]):
            pass

        result = coll3.aggregate(
            [
                mydict({"$unwind": "$tags"}),
                {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
                {"$sort": SON([("count", -1), ("_id", -1)])},
            ]
        )
        self.assertTrue(len(list(result)))

    def test_with_transaction(self) -> None:
        def execute_transaction(session):
            pass

        with self.client.start_session() as session:
            return session.with_transaction(
                execute_transaction, read_preference=ReadPreference.PRIMARY
            )


class TestDecode(unittest.TestCase):
    def test_bson_decode(self) -> None:
        doc = {"_id": 1}
        bsonbytes = encode(doc)
        rt_document: Dict[str, Any] = decode(bsonbytes)
        assert rt_document["_id"] == 1
        rt_document["foo"] = "bar"

        class MyDict(Dict[str, Any]):
            def foo(self):
                return "bar"

        codec_options = CodecOptions(document_class=MyDict)
        bsonbytes2 = encode(doc, codec_options=codec_options)
        rt_document2 = decode(bsonbytes2, codec_options=codec_options)
        assert rt_document2.foo() == "bar"

        codec_options2 = CodecOptions(document_class=RawBSONDocument)
        encode(doc, codec_options=codec_options2)
        rt_document3 = decode(bsonbytes2, codec_options=codec_options2)
        assert rt_document3.raw

    def test_bson_decode_no_codec_option(self) -> None:
        doc = decode_all(encode({"a": 1}))
        assert doc
        doc[0]["a"] = 2

    def test_bson_decode_all(self) -> None:
        doc = {"_id": 1}
        bsonbytes = encode(doc)
        bsonbytes += encode(doc)
        rt_documents: List[Dict[str, Any]] = decode_all(bsonbytes)
        assert rt_documents[0]["_id"] == 1
        rt_documents[0]["foo"] = "bar"

        class MyDict(Dict[str, Any]):
            def foo(self):
                return "bar"

        codec_options2 = CodecOptions(MyDict)
        bsonbytes2 = encode(doc, codec_options=codec_options2)
        bsonbytes2 += encode(doc, codec_options=codec_options2)
        rt_documents2 = decode_all(bsonbytes2, codec_options2)
        assert rt_documents2[0].foo() == "bar"

        codec_options3 = CodecOptions(RawBSONDocument)
        bsonbytes3 = encode(doc, codec_options=codec_options3)
        bsonbytes3 += encode(doc, codec_options=codec_options3)
        rt_documents3 = decode_all(bsonbytes3, codec_options3)
        assert rt_documents3[0].raw

    def test_bson_decode_all_no_codec_option(self) -> None:
        docs = decode_all(b"")
        docs.append({"new": 1})

        docs = decode_all(encode({"a": 1}))
        assert docs
        docs[0]["a"] = 2
        docs.append({"new": 1})

    def test_bson_decode_iter(self) -> None:
        doc = {"_id": 1}
        bsonbytes = encode(doc)
        bsonbytes += encode(doc)
        rt_documents: Iterator[Dict[str, Any]] = decode_iter(bsonbytes)
        assert next(rt_documents)["_id"] == 1
        next(rt_documents)["foo"] = "bar"

        class MyDict(Dict[str, Any]):
            def foo(self):
                return "bar"

        codec_options2 = CodecOptions(MyDict)
        bsonbytes2 = encode(doc, codec_options=codec_options2)
        bsonbytes2 += encode(doc, codec_options=codec_options2)
        rt_documents2 = decode_iter(bsonbytes2, codec_options2)
        assert next(rt_documents2).foo() == "bar"

        codec_options3 = CodecOptions(RawBSONDocument)
        bsonbytes3 = encode(doc, codec_options=codec_options3)
        bsonbytes3 += encode(doc, codec_options=codec_options3)
        rt_documents3 = decode_iter(bsonbytes3, codec_options3)
        assert next(rt_documents3).raw

    def test_bson_decode_iter_no_codec_option(self) -> None:
        doc = next(decode_iter(encode({"a": 1})))
        assert doc
        doc["a"] = 2

    def make_tempfile(self, content: bytes) -> Any:
        fileobj = tempfile.TemporaryFile()
        fileobj.write(content)
        fileobj.seek(0)
        self.addCleanup(fileobj.close)
        return fileobj

    def test_bson_decode_file_iter(self) -> None:
        doc = {"_id": 1}
        bsonbytes = encode(doc)
        bsonbytes += encode(doc)
        fileobj = self.make_tempfile(bsonbytes)
        rt_documents: Iterator[Dict[str, Any]] = decode_file_iter(fileobj)
        assert next(rt_documents)["_id"] == 1
        next(rt_documents)["foo"] = "bar"

        class MyDict(Dict[str, Any]):
            def foo(self):
                return "bar"

        codec_options2 = CodecOptions(MyDict)
        bsonbytes2 = encode(doc, codec_options=codec_options2)
        bsonbytes2 += encode(doc, codec_options=codec_options2)
        fileobj2 = self.make_tempfile(bsonbytes2)
        rt_documents2 = decode_file_iter(fileobj2, codec_options2)
        assert next(rt_documents2).foo() == "bar"

        codec_options3 = CodecOptions(RawBSONDocument)
        bsonbytes3 = encode(doc, codec_options=codec_options3)
        bsonbytes3 += encode(doc, codec_options=codec_options3)
        fileobj3 = self.make_tempfile(bsonbytes3)
        rt_documents3 = decode_file_iter(fileobj3, codec_options3)
        assert next(rt_documents3).raw

    def test_bson_decode_file_iter_none_codec_option(self) -> None:
        fileobj = self.make_tempfile(encode({"new": 1}))
        doc = next(decode_file_iter(fileobj))
        assert doc
        doc["a"] = 2


class TestDocumentType(unittest.TestCase):
    @only_type_check
    def test_default(self) -> None:
        client: MongoClient = MongoClient()
        coll = client.test.test
        retrieved = coll.find_one({"_id": "foo"})
        assert retrieved is not None
        retrieved["a"] = 1

    @only_type_check
    def test_explicit_document_type(self) -> None:
        client: MongoClient[Dict[str, Any]] = MongoClient()
        coll = client.test.test
        retrieved = coll.find_one({"_id": "foo"})
        assert retrieved is not None
        retrieved["a"] = 1

    @only_type_check
    def test_typeddict_document_type(self) -> None:
        client: MongoClient[Movie] = MongoClient()
        coll = client.test.test
        retrieved = coll.find_one({"_id": "foo"})
        assert retrieved is not None
        assert retrieved["year"] == 1
        assert retrieved["name"] == "a"

    @only_type_check
    def test_typeddict_document_type_insertion(self) -> None:
        client: MongoClient[Movie] = MongoClient()
        coll = client.test.test
        mov = {"name": "THX-1138", "year": 1971}
        movie = Movie(name="THX-1138", year=1971)
        coll.insert_one(mov)  # type: ignore[arg-type]
        coll.insert_one({"name": "THX-1138", "year": 1971})  # This will work because it is in-line.
        coll.insert_one(movie)
        coll.insert_many([mov])  # type: ignore[list-item]
        coll.insert_many([movie])
        bad_mov = {"name": "THX-1138", "year": "WRONG TYPE"}
        bad_movie = Movie(name="THX-1138", year="WRONG TYPE")  # type: ignore[typeddict-item]
        coll.insert_one(bad_mov)  # type:ignore[arg-type]
        coll.insert_one({"name": "THX-1138", "year": "WRONG TYPE"})  # type: ignore[typeddict-item]
        coll.insert_one(bad_movie)
        coll.insert_many([bad_mov])  # type: ignore[list-item]
        coll.insert_many(
            [{"name": "THX-1138", "year": "WRONG TYPE"}]  # type: ignore[typeddict-item]
        )
        coll.insert_many([bad_movie])

    @only_type_check
    def test_bulk_write_document_type_insertion(self):
        client: MongoClient[MovieWithId] = MongoClient()
        coll: Collection[MovieWithId] = client.test.test
        coll.bulk_write(
            [InsertOne(Movie({"name": "THX-1138", "year": 1971}))]  # type:ignore[arg-type]
        )
        mov_dict = {"_id": ObjectId(), "name": "THX-1138", "year": 1971}
        coll.bulk_write(
            [InsertOne(mov_dict)]  # type:ignore[arg-type]
        )
        coll.bulk_write(
            [
                InsertOne({"_id": ObjectId(), "name": "THX-1138", "year": 1971})
            ]  # No error because it is in-line.
        )

    @only_type_check
    def test_bulk_write_document_type_replacement(self):
        client: MongoClient[MovieWithId] = MongoClient()
        coll: Collection[MovieWithId] = client.test.test
        coll.bulk_write(
            [ReplaceOne({}, Movie({"name": "THX-1138", "year": 1971}))]  # type:ignore[arg-type]
        )
        mov_dict = {"_id": ObjectId(), "name": "THX-1138", "year": 1971}
        coll.bulk_write(
            [ReplaceOne({}, mov_dict)]  # type:ignore[arg-type]
        )
        coll.bulk_write(
            [
                ReplaceOne({}, {"_id": ObjectId(), "name": "THX-1138", "year": 1971})
            ]  # No error because it is in-line.
        )

    @only_type_check
    def test_typeddict_explicit_document_type(self) -> None:
        out = MovieWithId(_id=ObjectId(), name="THX-1138", year=1971)
        assert out is not None
        # This should fail because the output is a Movie.
        assert out["foo"]  # type:ignore[typeddict-item]
        assert out["_id"]

    # This should work the same as the test above, but this time using NotRequired to allow
    # automatic insertion of the _id field by insert_one.
    @only_type_check
    def test_typeddict_not_required_document_type(self) -> None:
        out = ImplicitMovie(name="THX-1138", year=1971)
        assert out is not None
        # This should fail because the output is a Movie.
        assert out["foo"]  # type:ignore[typeddict-item]
        # pyright gives reportTypedDictNotRequiredAccess for the following:
        assert out["_id"]  # type:ignore

    @only_type_check
    def test_typeddict_empty_document_type(self) -> None:
        out = Movie(name="THX-1138", year=1971)
        assert out is not None
        # This should fail because the output is a Movie.
        assert out["foo"]  # type:ignore[typeddict-item]
        # This should fail because _id is not included in our TypedDict definition.
        assert out["_id"]  # type:ignore[typeddict-item]

    @client_context.require_connection
    def test_typeddict_find_notrequired(self):
        if NotRequired is None or ImplicitMovie is None:
            raise unittest.SkipTest("Python 3.11+ is required to use NotRequired.")
        client: MongoClient[ImplicitMovie] = rs_or_single_client()
        coll = client.test.test
        coll.insert_one(ImplicitMovie(name="THX-1138", year=1971))
        out = coll.find_one({})
        assert out is not None
        # pyright gives reportTypedDictNotRequiredAccess for the following:
        assert out["_id"]  # type:ignore

    @only_type_check
    def test_raw_bson_document_type(self) -> None:
        client = MongoClient(document_class=RawBSONDocument)
        coll = client.test.test
        retrieved = coll.find_one({"_id": "foo"})
        assert retrieved is not None
        assert len(retrieved.raw) > 0

    @only_type_check
    def test_son_document_type(self) -> None:
        client = MongoClient(document_class=SON[str, Any])
        coll = client.test.test
        retrieved = coll.find_one({"_id": "foo"})
        assert retrieved is not None
        retrieved["a"] = 1

    def test_son_document_type_runtime(self) -> None:
        MongoClient(document_class=SON[str, Any], connect=False)

    @only_type_check
    def test_create_index(self) -> None:
        client: MongoClient[Dict[str, str]] = MongoClient("test")
        db = client.test
        with client.start_session() as session:
            index = db.test.create_index([("user_id", ASCENDING)], unique=True, session=session)
            assert isinstance(index, str)


class TestCommandDocumentType(unittest.TestCase):
    @only_type_check
    def test_default(self) -> None:
        client: MongoClient = MongoClient()
        result: Dict = client.admin.command("ping")
        result["a"] = 1

    @only_type_check
    def test_explicit_document_type(self) -> None:
        client: MongoClient = MongoClient()
        codec_options: CodecOptions[Dict[str, Any]] = CodecOptions()
        result = client.admin.command("ping", codec_options=codec_options)
        result["a"] = 1

    @only_type_check
    def test_typeddict_document_type(self) -> None:
        client: MongoClient = MongoClient()
        codec_options: CodecOptions[Movie] = CodecOptions()
        result = client.admin.command("ping", codec_options=codec_options)
        assert result["year"] == 1
        assert result["name"] == "a"

    @only_type_check
    def test_raw_bson_document_type(self) -> None:
        client: MongoClient = MongoClient()
        codec_options = CodecOptions(RawBSONDocument)
        result = client.admin.command("ping", codec_options=codec_options)
        assert len(result.raw) > 0

    @only_type_check
    def test_son_document_type(self) -> None:
        client = MongoClient(document_class=SON[str, Any])
        codec_options = CodecOptions(SON[str, Any])
        result = client.admin.command("ping", codec_options=codec_options)
        result["a"] = 1


class TestCodecOptionsDocumentType(unittest.TestCase):
    def test_default(self) -> None:
        options: CodecOptions = CodecOptions()
        obj = options.document_class()
        obj["a"] = 1

    def test_explicit_document_type(self) -> None:
        options: CodecOptions[Dict[str, Any]] = CodecOptions()
        obj = options.document_class()
        obj["a"] = 1

    def test_typeddict_document_type(self) -> None:
        options: CodecOptions[Movie] = CodecOptions()
        # Suppress: Cannot instantiate type "Type[Movie]".
        obj = options.document_class(name="a", year=1)  # type: ignore[misc]
        assert obj["year"] == 1
        assert obj["name"] == "a"

    def test_raw_bson_document_type(self) -> None:
        options = CodecOptions(RawBSONDocument)
        doc_bson = b"\x10\x00\x00\x00\x11a\x00\xff\xff\xff\xff\xff\xff\xff\xff\x00"
        obj = options.document_class(doc_bson)
        assert len(obj.raw) > 0

    def test_son_document_type(self) -> None:
        options = CodecOptions(SON[str, Any])
        obj = options.document_class()
        obj["a"] = 1


if __name__ == "__main__":
    unittest.main()
