# Copyright 2012-present MongoDB, Inc.
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

"""Minimal test of PyMongo in a WSGI application, see bug PYTHON-353
"""
from __future__ import annotations

import datetime
import os
import re
import sys
import uuid

this_path = os.path.dirname(os.path.join(os.getcwd(), __file__))

# Location of PyMongo checkout
repository_path = os.path.normpath(os.path.join(this_path, "..", ".."))
sys.path.insert(0, repository_path)

import bson
import pymongo
from bson.binary import STANDARD, Binary
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.datetime_ms import DatetimeConversion, DatetimeMS
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.regex import Regex
from pymongo.mongo_client import MongoClient

# Ensure the C extensions are installed.
assert bson.has_c()
assert pymongo.has_c()

OPTS: CodecOptions[dict] = CodecOptions(
    uuid_representation=STANDARD, datetime_conversion=DatetimeConversion.DATETIME_AUTO
)
client: MongoClient[dict] = MongoClient()
# Use a unique collection name for each process:
coll_name = f"test-{uuid.uuid4()}"
collection = client.test.get_collection(coll_name, codec_options=OPTS)
ndocs = 20
collection.drop()
doc = {
    "int32": 2 << 15,
    "int64": 2 << 50,
    "null": None,
    "bool": True,
    "float": 1.5,
    "str": "string",
    "list": [1, 2, 3],
    "dict": {"a": 1, "b": 2, "c": 3},
    "datetime": datetime.datetime.fromtimestamp(1690328577.446),
    "datetime_ms_out_of_range": DatetimeMS(-2 << 60),
    "regex_native": re.compile("regex*"),
    "regex_pymongo": Regex("regex*"),
    "binary": Binary(b"bytes", 128),
    "oid": ObjectId(),
    "dbref": DBRef("test", 1),
    "code": Code("function(){ return true; }"),
    "code_w_scope": Code("return function(){ return x; }", scope={"x": False}),
    "bytes": b"bytes",
    "uuid": uuid.uuid4(),
}
collection.insert_many([dict(i=i, **doc) for i in range(ndocs)])
client.close()  # Discard main thread's request socket.
client = MongoClient()
collection = client.test.get_collection(coll_name, codec_options=OPTS)

try:
    from mod_wsgi import version as mod_wsgi_version  # type: ignore[import]
except:
    mod_wsgi_version = None


def application(environ, start_response):
    results = list(collection.find().batch_size(10))
    assert len(results) == ndocs, f"n_actual={len(results)} n_expected={ndocs}"
    # Test encoding and decoding works (for sub interpreter support).
    decoded = bson.decode(bson.encode(doc, codec_options=OPTS), codec_options=OPTS)
    for key, value in doc.items():
        # Native regex objects are decoded as bson Regex.
        if isinstance(value, re.Pattern):
            value = Regex.from_native(value)
        assert decoded[key] == value, f"failed on doc[{key!r}]: {decoded[key]!r} != {value!r}"
        assert isinstance(
            decoded[key], type(value)
        ), f"failed on doc[{key}]: {decoded[key]!r} is not an instance of {type(value)}"

    output = (
        f" python {sys.version}, mod_wsgi {mod_wsgi_version},"
        f" pymongo {pymongo.version},"
        f' mod_wsgi.process_group = {environ["mod_wsgi.process_group"]!r}'
        f' mod_wsgi.application_group = {environ["mod_wsgi.application_group"]!r}'
        f' wsgi.multithread = {environ["wsgi.multithread"]!r}'
        "\n"
    )
    response_headers = [("Content-Length", str(len(output)))]
    start_response("200 OK", response_headers)
    return [output.encode("ascii")]
