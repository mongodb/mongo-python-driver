# Copyright 2022-present MongoDB, Inc.
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

"""Test the default exports of the top level packages."""
from __future__ import annotations

import inspect
import unittest

import bson
import gridfs
import pymongo

BSON_IGNORE = []
GRIDFS_IGNORE = [
    "ASCENDING",
    "DESCENDING",
    "AsyncClientSession",
    "Collection",
    "ObjectId",
    "validate_string",
    "Database",
    "ConfigurationError",
    "WriteConcern",
]
PYMONGO_IGNORE = []
GLOBAL_INGORE = ["TYPE_CHECKING", "annotations"]


class TestDefaultExports(unittest.TestCase):
    def check_module(self, mod, ignores):
        names = dir(mod)
        names.remove("__all__")
        for name in mod.__all__:
            if name not in names and name not in ignores:
                self.fail(f"{name} was included in {mod}.__all__ but is not a valid symbol")

        for name in names:
            if name not in mod.__all__ and name not in ignores:
                if name in GLOBAL_INGORE:
                    continue
                value = getattr(mod, name)
                if inspect.ismodule(value):
                    continue
                if getattr(value, "__module__", None) == "typing":
                    continue
                if not name.startswith("_"):
                    self.fail(f"{name} was not included in {mod}.__all__")

    def test_pymongo(self):
        self.check_module(pymongo, PYMONGO_IGNORE)

    def test_gridfs(self):
        self.check_module(gridfs, GRIDFS_IGNORE)

    def test_bson(self):
        self.check_module(bson, BSON_IGNORE)

    def test_pymongo_imports(self):
        import pymongo
        from pymongo.auth import MECHANISMS
        from pymongo.auth_oidc import (
            OIDCCallback,
            OIDCCallbackContext,
            OIDCCallbackResult,
            OIDCIdPInfo,
        )
        from pymongo.change_stream import (
            ChangeStream,
            ClusterChangeStream,
            CollectionChangeStream,
            DatabaseChangeStream,
        )
        from pymongo.client_options import ClientOptions
        from pymongo.client_session import ClientSession, SessionOptions, TransactionOptions
        from pymongo.collation import (
            Collation,
            CollationAlternate,
            CollationCaseFirst,
            CollationMaxVariable,
            CollationStrength,
            validate_collation_or_none,
        )
        from pymongo.collection import Collection, ReturnDocument
        from pymongo.command_cursor import CommandCursor, RawBatchCommandCursor
        from pymongo.cursor import Cursor, RawBatchCursor
        from pymongo.database import Database
        from pymongo.driver_info import DriverInfo
        from pymongo.encryption import (
            Algorithm,
            ClientEncryption,
            QueryType,
            RewrapManyDataKeyResult,
        )
        from pymongo.encryption_options import AutoEncryptionOpts, RangeOpts
        from pymongo.errors import (
            AutoReconnect,
            BulkWriteError,
            CollectionInvalid,
            ConfigurationError,
            ConnectionFailure,
            CursorNotFound,
            DocumentTooLarge,
            DuplicateKeyError,
            EncryptedCollectionError,
            EncryptionError,
            ExecutionTimeout,
            InvalidName,
            InvalidOperation,
            NetworkTimeout,
            NotPrimaryError,
            OperationFailure,
            ProtocolError,
            PyMongoError,
            ServerSelectionTimeoutError,
            WaitQueueTimeoutError,
            WriteConcernError,
            WriteError,
            WTimeoutError,
        )
        from pymongo.event_loggers import (
            CommandLogger,
            ConnectionPoolLogger,
            HeartbeatLogger,
            ServerLogger,
            TopologyLogger,
        )
        from pymongo.mongo_client import MongoClient
        from pymongo.monitoring import (
            CommandFailedEvent,
            CommandListener,
            CommandStartedEvent,
            CommandSucceededEvent,
            ConnectionCheckedInEvent,
            ConnectionCheckedOutEvent,
            ConnectionCheckOutFailedEvent,
            ConnectionCheckOutFailedReason,
            ConnectionCheckOutStartedEvent,
            ConnectionClosedEvent,
            ConnectionClosedReason,
            ConnectionCreatedEvent,
            ConnectionPoolListener,
            ConnectionReadyEvent,
            PoolClearedEvent,
            PoolClosedEvent,
            PoolCreatedEvent,
            PoolReadyEvent,
            ServerClosedEvent,
            ServerDescriptionChangedEvent,
            ServerHeartbeatFailedEvent,
            ServerHeartbeatListener,
            ServerHeartbeatStartedEvent,
            ServerHeartbeatSucceededEvent,
            ServerListener,
            ServerOpeningEvent,
            TopologyClosedEvent,
            TopologyDescriptionChangedEvent,
            TopologyEvent,
            TopologyListener,
            TopologyOpenedEvent,
            register,
        )
        from pymongo.operations import (
            DeleteMany,
            DeleteOne,
            IndexModel,
            SearchIndexModel,
            UpdateMany,
            UpdateOne,
        )
        from pymongo.pool import PoolOptions
        from pymongo.read_concern import ReadConcern
        from pymongo.read_preferences import (
            Nearest,
            Primary,
            PrimaryPreferred,
            ReadPreference,
            SecondaryPreferred,
        )
        from pymongo.results import (
            BulkWriteResult,
            DeleteResult,
            InsertManyResult,
            InsertOneResult,
            UpdateResult,
        )
        from pymongo.server_api import ServerApi, ServerApiVersion
        from pymongo.server_description import ServerDescription
        from pymongo.topology_description import TopologyDescription
        from pymongo.uri_parser import (
            parse_host,
            parse_ipv6_literal_host,
            parse_uri,
            parse_userinfo,
            split_hosts,
            split_options,
            validate_options,
        )
        from pymongo.write_concern import WriteConcern, validate_boolean

    def test_pymongo_submodule_attributes(self):
        import pymongo

        self.assertTrue(hasattr(pymongo, "uri_parser"))
        self.assertTrue(pymongo.uri_parser)
        self.assertTrue(pymongo.uri_parser.parse_uri)
        self.assertTrue(pymongo.change_stream)
        self.assertTrue(pymongo.client_session)
        self.assertTrue(pymongo.collection)
        self.assertTrue(pymongo.cursor)
        self.assertTrue(pymongo.command_cursor)
        self.assertTrue(pymongo.database)

    def test_gridfs_imports(self):
        import gridfs
        from gridfs.errors import CorruptGridFile, FileExists, GridFSError, NoFile
        from gridfs.grid_file import (
            GridFS,
            GridFSBucket,
            GridIn,
            GridOut,
            GridOutChunkIterator,
            GridOutCursor,
            GridOutIterator,
        )


if __name__ == "__main__":
    unittest.main()
