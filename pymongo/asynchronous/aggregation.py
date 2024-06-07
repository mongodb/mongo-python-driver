# Copyright 2019-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Perform aggregation operations on a collection or database."""
from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, Optional, Union

from pymongo.asynchronous import common
from pymongo.asynchronous.collation import validate_collation_or_none
from pymongo.asynchronous.read_preferences import ReadPreference, _AggWritePref
from pymongo.errors import ConfigurationError

if TYPE_CHECKING:
    from pymongo.asynchronous.client_session import ClientSession
    from pymongo.asynchronous.collection import AsyncCollection
    from pymongo.asynchronous.command_cursor import AsyncCommandCursor
    from pymongo.asynchronous.database import AsyncDatabase
    from pymongo.asynchronous.pool import Connection
    from pymongo.asynchronous.read_preferences import _ServerMode
    from pymongo.asynchronous.server import Server
    from pymongo.asynchronous.typings import _DocumentType, _Pipeline

_IS_SYNC = False


class _AggregationCommand:
    """The internal abstract base class for aggregation cursors.

    Should not be called directly by application developers. Use
    :meth:`pymongo.collection.AsyncCollection.aggregate`, or
    :meth:`pymongo.database.AsyncDatabase.aggregate` instead.
    """

    def __init__(
        self,
        target: Union[AsyncDatabase, AsyncCollection],
        cursor_class: type[AsyncCommandCursor],
        pipeline: _Pipeline,
        options: MutableMapping[str, Any],
        explicit_session: bool,
        let: Optional[Mapping[str, Any]] = None,
        user_fields: Optional[MutableMapping[str, Any]] = None,
        result_processor: Optional[Callable[[Mapping[str, Any], Connection], None]] = None,
        comment: Any = None,
    ) -> None:
        if "explain" in options:
            raise ConfigurationError(
                "The explain option is not supported. Use AsyncDatabase.command instead."
            )

        self._target = target

        pipeline = common.validate_list("pipeline", pipeline)
        self._pipeline = pipeline
        self._performs_write = False
        if pipeline and ("$out" in pipeline[-1] or "$merge" in pipeline[-1]):
            self._performs_write = True

        common.validate_is_mapping("options", options)
        if let is not None:
            common.validate_is_mapping("let", let)
            options["let"] = let
        if comment is not None:
            options["comment"] = comment

        self._options = options

        # This is the batchSize that will be used for setting the initial
        # batchSize for the cursor, as well as the subsequent getMores.
        self._batch_size = common.validate_non_negative_integer_or_none(
            "batchSize", self._options.pop("batchSize", None)
        )

        # If the cursor option is already specified, avoid overriding it.
        self._options.setdefault("cursor", {})
        # If the pipeline performs a write, we ignore the initial batchSize
        # since the server doesn't return results in this case.
        if self._batch_size is not None and not self._performs_write:
            self._options["cursor"]["batchSize"] = self._batch_size

        self._cursor_class = cursor_class
        self._explicit_session = explicit_session
        self._user_fields = user_fields
        self._result_processor = result_processor

        self._collation = validate_collation_or_none(options.pop("collation", None))

        self._max_await_time_ms = options.pop("maxAwaitTimeMS", None)
        self._write_preference: Optional[_AggWritePref] = None

    @property
    def _aggregation_target(self) -> Union[str, int]:
        """The argument to pass to the aggregate command."""
        raise NotImplementedError

    @property
    def _cursor_namespace(self) -> str:
        """The namespace in which the aggregate command is run."""
        raise NotImplementedError

    def _cursor_collection(self, cursor_doc: Mapping[str, Any]) -> AsyncCollection:
        """The AsyncCollection used for the aggregate command cursor."""
        raise NotImplementedError

    @property
    def _database(self) -> AsyncDatabase:
        """The database against which the aggregation command is run."""
        raise NotImplementedError

    def get_read_preference(
        self, session: Optional[ClientSession]
    ) -> Union[_AggWritePref, _ServerMode]:
        if self._write_preference:
            return self._write_preference
        pref = self._target._read_preference_for(session)
        if self._performs_write and pref != ReadPreference.PRIMARY:
            self._write_preference = pref = _AggWritePref(pref)  # type: ignore[assignment]
        return pref

    async def get_cursor(
        self,
        session: Optional[ClientSession],
        server: Server,
        conn: Connection,
        read_preference: _ServerMode,
    ) -> AsyncCommandCursor[_DocumentType]:
        # Serialize command.
        cmd = {"aggregate": self._aggregation_target, "pipeline": self._pipeline}
        cmd.update(self._options)

        # Apply this target's read concern if:
        # readConcern has not been specified as a kwarg and either
        # - server version is >= 4.2 or
        # - server version is >= 3.2 and pipeline doesn't use $out
        if ("readConcern" not in cmd) and (
            not self._performs_write or (conn.max_wire_version >= 8)
        ):
            read_concern = self._target.read_concern
        else:
            read_concern = None

        # Apply this target's write concern if:
        # writeConcern has not been specified as a kwarg and pipeline doesn't
        # perform a write operation
        if "writeConcern" not in cmd and self._performs_write:
            write_concern = self._target._write_concern_for(session)
        else:
            write_concern = None

        # Run command.
        result = await conn.command(
            self._database.name,
            cmd,
            read_preference,
            self._target.codec_options,
            parse_write_concern_error=True,
            read_concern=read_concern,
            write_concern=write_concern,
            collation=self._collation,
            session=session,
            client=self._database.client,
            user_fields=self._user_fields,
        )

        if self._result_processor:
            self._result_processor(result, conn)

        # Extract cursor from result or mock/fake one if necessary.
        if "cursor" in result:
            cursor = result["cursor"]
        else:
            # Unacknowledged $out/$merge write. Fake a cursor.
            cursor = {
                "id": 0,
                "firstBatch": result.get("result", []),
                "ns": self._cursor_namespace,
            }

        # Create and return cursor instance.
        cmd_cursor = self._cursor_class(
            self._cursor_collection(cursor),
            cursor,
            conn.address,
            batch_size=self._batch_size or 0,
            max_await_time_ms=self._max_await_time_ms,
            session=session,
            explicit_session=self._explicit_session,
            comment=self._options.get("comment"),
        )
        await cmd_cursor._maybe_pin_connection(conn)
        return cmd_cursor


class _CollectionAggregationCommand(_AggregationCommand):
    _target: AsyncCollection

    @property
    def _aggregation_target(self) -> str:
        return self._target.name

    @property
    def _cursor_namespace(self) -> str:
        return self._target.full_name

    def _cursor_collection(self, cursor: Mapping[str, Any]) -> AsyncCollection:
        """The AsyncCollection used for the aggregate command cursor."""
        return self._target

    @property
    def _database(self) -> AsyncDatabase:
        return self._target.database


class _CollectionRawAggregationCommand(_CollectionAggregationCommand):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # For raw-batches, we set the initial batchSize for the cursor to 0.
        if not self._performs_write:
            self._options["cursor"]["batchSize"] = 0


class _DatabaseAggregationCommand(_AggregationCommand):
    _target: AsyncDatabase

    @property
    def _aggregation_target(self) -> int:
        return 1

    @property
    def _cursor_namespace(self) -> str:
        return f"{self._target.name}.$cmd.aggregate"

    @property
    def _database(self) -> AsyncDatabase:
        return self._target

    def _cursor_collection(self, cursor: Mapping[str, Any]) -> AsyncCollection:
        """The AsyncCollection used for the aggregate command cursor."""
        # AsyncCollection level aggregate may not always return the "ns" field
        # according to our MockupDB tests. Let's handle that case for db level
        # aggregate too by defaulting to the <db>.$cmd.aggregate namespace.
        _, collname = cursor.get("ns", self._cursor_namespace).split(".", 1)
        return self._database[collname]
