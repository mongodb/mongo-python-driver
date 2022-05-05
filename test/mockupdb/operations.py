# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"),;
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

from collections import namedtuple

from mockupdb import OpMsgReply, OpReply

from pymongo import ReadPreference

__all__ = ["operations", "upgrades"]


Operation = namedtuple("Operation", ["name", "function", "reply", "op_type", "not_master"])
"""Client operations on MongoDB.

Each has a human-readable name, a function that actually executes a test, and
a type that maps to one of the types in the Server Selection Spec:
'may-use-secondary', 'must-use-primary', etc.

The special type 'always-use-secondary' applies to an operation with an explicit
read mode, like the operation "command('c', read_preference=SECONDARY)".

The not-master response is how a secondary responds to a must-use-primary op,
or how a recovering member responds to a may-use-secondary op.

Example uses:

We can use "find_one" to validate that the SlaveOk bit is set when querying a
standalone, even with mode PRIMARY, but that it isn't set when sent to a mongos
with mode PRIMARY. Or it can validate that "$readPreference" is included in
mongos queries except with mode PRIMARY or SECONDARY_PREFERRED (PYTHON-865).

We can use "options_old" and "options_new" to test that the driver queries an
old server's system.namespaces collection, but uses the listCollections command
on a new server (PYTHON-857).

"secondary command" is good to test that the client can direct reads to
secondaries in a replica set, or select a mongos for secondary reads in a
sharded cluster (PYTHON-868).
"""

not_master_reply = OpMsgReply(ok=0, errmsg="not master")

operations = [
    Operation(
        "find_one",
        lambda client: client.db.collection.find_one(),
        reply={"cursor": {"id": 0, "firstBatch": []}},
        op_type="may-use-secondary",
        not_master=not_master_reply,
    ),
    Operation(
        "count_documents",
        lambda client: client.db.collection.count_documents({}),
        reply={"n": 1},
        op_type="may-use-secondary",
        not_master=not_master_reply,
    ),
    Operation(
        "estimated_document_count",
        lambda client: client.db.collection.estimated_document_count(),
        reply={"n": 1},
        op_type="may-use-secondary",
        not_master=not_master_reply,
    ),
    Operation(
        "aggregate",
        lambda client: client.db.collection.aggregate([]),
        reply={"cursor": {"id": 0, "firstBatch": []}},
        op_type="may-use-secondary",
        not_master=not_master_reply,
    ),
    Operation(
        "options",
        lambda client: client.db.collection.options(),
        reply={"cursor": {"id": 0, "firstBatch": []}},
        op_type="must-use-primary",
        not_master=not_master_reply,
    ),
    Operation(
        "command",
        lambda client: client.db.command("foo"),
        reply={"ok": 1},
        op_type="must-use-primary",  # Ignores client's read preference.
        not_master=not_master_reply,
    ),
    Operation(
        "secondary command",
        lambda client: client.db.command("foo", read_preference=ReadPreference.SECONDARY),
        reply={"ok": 1},
        op_type="always-use-secondary",
        not_master=OpReply(ok=0, errmsg="node is recovering"),
    ),
    Operation(
        "listIndexes",
        lambda client: client.db.collection.index_information(),
        reply={"cursor": {"id": 0, "firstBatch": []}},
        op_type="must-use-primary",
        not_master=not_master_reply,
    ),
]


_ops_by_name = dict([(op.name, op) for op in operations])

Upgrade = namedtuple("Upgrade", ["name", "function", "old", "new", "wire_version"])

upgrades = []
