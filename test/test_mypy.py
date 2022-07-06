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
sample client code that uses PyMongo typings."""

import os
import tempfile
import unittest
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List

try:
    from typing import TypedDict  # type: ignore[attr-defined]

    # Not available in Python 3.7
    class Movie(TypedDict):  # type: ignore[misc]
        name: str
        year: int

except ImportError:
    TypeDict = None


try:
    from mypy import api
except ImportError:
    api = None  # type: ignore[assignment]

from test import IntegrationTest
from test.utils import rs_or_single_client

from bson import CodecOptions, decode, decode_all, decode_file_iter, decode_iter, encode
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.operations import InsertOne
from pymongo.read_preferences import ReadPreference

TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "mypy_fails")


def get_tests() -> Iterable[str]:
    for dirpath, _, filenames in os.walk(TEST_PATH):
        for filename in filenames:
            yield os.path.join(dirpath, filename)


def only_type_check(func):
    def inner(*args, **kwargs):
        if not TYPE_CHECKING:
            raise unittest.SkipTest("Used for Type Checking Only")
        func(*args, **kwargs)

    return inner


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
        retreived = self.coll.find_one({"_id": doc["_id"]})
        if retreived:
            # Documents returned from find are mutable.
            retreived["new_field"] = 1
            result2 = coll2.insert_one(retreived)
            self.assertEqual(result2.inserted_id, result.inserted_id)

    def test_cursor_iterable(self) -> None:
        def to_list(iterable: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
            return list(iterable)

        self.coll.insert_one({})
        cursor = self.coll.find()
        docs = to_list(cursor)
        self.assertTrue(docs)

    def test_bulk_write(self) -> None:
        self.coll.insert_one({})
        requests = [InsertOne({})]
        result = self.coll.bulk_write(requests)
        self.assertTrue(result.acknowledged)

    def test_command(self) -> None:
        result: Dict = self.client.admin.command("ping")
        items = result.items()

    def test_list_collections(self) -> None:
        cursor = self.client.test.list_collections()
        value = cursor.next()
        items = value.items()

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
        retreived = coll.find_one({"_id": doc["_id"]})
        assert retreived is not None
        retreived["a"] = 1

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
        bsonbytes3 = encode(doc, codec_options=codec_options2)
        rt_document3 = decode(bsonbytes2, codec_options=codec_options2)
        assert rt_document3.raw

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


class TestDocumentType(unittest.TestCase):
    @only_type_check
    def test_default(self) -> None:
        client: MongoClient = MongoClient()
        coll = client.test.test
        retreived = coll.find_one({"_id": "foo"})
        assert retreived is not None
        retreived["a"] = 1

    @only_type_check
    def test_explicit_document_type(self) -> None:
        client: MongoClient[Dict[str, Any]] = MongoClient()
        coll = client.test.test
        retreived = coll.find_one({"_id": "foo"})
        assert retreived is not None
        retreived["a"] = 1

    @only_type_check
    def test_typeddict_document_type(self) -> None:
        client: MongoClient[Movie] = MongoClient()
        coll = client.test.test
        retreived = coll.find_one({"_id": "foo"})
        assert retreived is not None
        assert retreived["year"] == 1
        assert retreived["name"] == "a"

    @only_type_check
    def test_raw_bson_document_type(self) -> None:
        client = MongoClient(document_class=RawBSONDocument)
        coll = client.test.test
        retreived = coll.find_one({"_id": "foo"})
        assert retreived is not None
        assert len(retreived.raw) > 0

    @only_type_check
    def test_son_document_type(self) -> None:
        client = MongoClient(document_class=SON[str, Any])
        coll = client.test.test
        retreived = coll.find_one({"_id": "foo"})
        assert retreived is not None
        retreived["a"] = 1

    def test_son_document_type_runtime(self) -> None:
        client = MongoClient(document_class=SON[str, Any], connect=False)

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
