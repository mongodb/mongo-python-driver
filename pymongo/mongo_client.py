# Copyright 2009-present MongoDB, Inc.
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

"""Tools for connecting to MongoDB.

.. seealso:: :doc:`/examples/high_availability` for examples of connecting
   to replica sets or sets of mongos servers.

To get a :class:`~pymongo.database.Database` instance from a
:class:`MongoClient` use either dictionary-style or attribute-style
access:

.. doctest::

  >>> from pymongo import MongoClient
  >>> c = MongoClient()
  >>> c.test_database
  Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'test_database')
  >>> c["test-database"]
  Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'test-database')
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import weakref
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    FrozenSet,
    Generic,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

import bson
from bson.codec_options import DEFAULT_CODEC_OPTIONS, TypeRegistry
from bson.timestamp import Timestamp
from pymongo import (
    _csot,
    client_session,
    common,
    database,
    helpers,
    message,
    periodic_executor,
    uri_parser,
)
from pymongo.asynchronous import schedule_task, synchronize
from pymongo.change_stream import ChangeStream, ClusterChangeStream
from pymongo.client_options import ClientOptions
from pymongo.client_session import _EmptyServerSession
from pymongo.command_cursor import CommandCursor
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    ConfigurationError,
    ConnectionFailure,
    InvalidOperation,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
    WaitQueueTimeoutError,
    WriteConcernError,
)
from pymongo.lock import _HAS_REGISTER_AT_FORK, _ALock, _create_lock, _release_locks
from pymongo.logger import _CLIENT_LOGGER, _log_or_warn
from pymongo.monitoring import ConnectionClosedReason
from pymongo.operations import _Op
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.server_selectors import writable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.settings import TopologySettings
from pymongo.topology import Topology, _ErrorContext
from pymongo.topology_description import TOPOLOGY_TYPE, TopologyDescription
from pymongo.typings import (
    ClusterTime,
    _Address,
    _CollationIn,
    _DocumentType,
    _DocumentTypeArg,
    _Pipeline,
)
from pymongo.uri_parser import (
    _check_options,
    _handle_option_deprecations,
    _handle_security_options,
    _normalize_options,
)
from pymongo.write_concern import DEFAULT_WRITE_CONCERN, WriteConcern

if TYPE_CHECKING:
    import sys
    from types import TracebackType

    from bson.objectid import ObjectId
    from pymongo.bulk import _Bulk
    from pymongo.client_session import ClientSession, _ServerSession
    from pymongo.cursor import _ConnectionManager
    from pymongo.database import Database
    from pymongo.message import _CursorAddress, _GetMore, _Query
    from pymongo.pool import Connection
    from pymongo.read_concern import ReadConcern
    from pymongo.response import Response
    from pymongo.server import Server
    from pymongo.server_selectors import Selection

    if sys.version_info[:2] >= (3, 9):
        from collections.abc import Generator
    else:
        # Deprecated since version 3.9: collections.abc.Generator now supports [].
        from typing import Generator

T = TypeVar("T")

_WriteCall = Callable[[Optional["ClientSession"], "Connection", bool], T]
_ReadCall = Callable[[Optional["ClientSession"], "Server", "Connection", _ServerMode], T]


class BaseMongoClient(common.BaseObject, Generic[_DocumentType]):
    HOST = "localhost"
    PORT = 27017
    # Define order to retrieve options from ClientOptions for __repr__.
    # No host/port; these are retrieved from TopologySettings.
    _constructor_args = ("document_class", "tz_aware", "connect")
    _clients: weakref.WeakValueDictionary = weakref.WeakValueDictionary()

    def __init__(
        self,
        host: Optional[Union[str, Sequence[str]]] = None,
        port: Optional[int] = None,
        document_class: Optional[Type[_DocumentType]] = None,
        tz_aware: Optional[bool] = None,
        connect: Optional[bool] = None,
        type_registry: Optional[TypeRegistry] = None,
        **kwargs: Any,
    ) -> None:
        doc_class = document_class or dict
        self._init_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "document_class": doc_class,
            "tz_aware": tz_aware,
            "connect": connect,
            "type_registry": type_registry,
            **kwargs,
        }

        if host is None:
            host = self.HOST
        if isinstance(host, str):
            host = [host]
        if port is None:
            port = self.PORT
        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        # _pool_class, _monitor_class, and _condition_class are for deep
        # customization of PyMongo, e.g. Motor.
        pool_class = kwargs.pop("_pool_class", None)
        monitor_class = kwargs.pop("_monitor_class", None)
        condition_class = kwargs.pop("_condition_class", None)

        # Parse options passed as kwargs.
        keyword_opts = common._CaseInsensitiveDictionary(kwargs)
        keyword_opts["document_class"] = doc_class

        seeds = set()
        username = None
        password = None
        dbase = None
        opts = common._CaseInsensitiveDictionary()
        fqdn = None
        srv_service_name = keyword_opts.get("srvservicename")
        srv_max_hosts = keyword_opts.get("srvmaxhosts")
        if len([h for h in host if "/" in h]) > 1:
            raise ConfigurationError("host must not contain multiple MongoDB URIs")
        for entity in host:
            # A hostname can only include a-z, 0-9, '-' and '.'. If we find a '/'
            # it must be a URI,
            # https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names
            if "/" in entity:
                # Determine connection timeout from kwargs.
                timeout = keyword_opts.get("connecttimeoutms")
                if timeout is not None:
                    timeout = common.validate_timeout_or_none_or_zero(
                        keyword_opts.cased_key("connecttimeoutms"), timeout
                    )
                res = uri_parser.parse_uri(
                    entity,
                    port,
                    validate=True,
                    warn=True,
                    normalize=False,
                    connect_timeout=timeout,
                    srv_service_name=srv_service_name,
                    srv_max_hosts=srv_max_hosts,
                )
                seeds.update(res["nodelist"])
                username = res["username"] or username
                password = res["password"] or password
                dbase = res["database"] or dbase
                opts = res["options"]
                fqdn = res["fqdn"]
            else:
                seeds.update(uri_parser.split_hosts(entity, port))
        if not seeds:
            raise ConfigurationError("need to specify at least one host")

        for hostname in [node[0] for node in seeds]:
            if _detect_external_db(hostname):
                break

        # Add options with named keyword arguments to the parsed kwarg options.
        if type_registry is not None:
            keyword_opts["type_registry"] = type_registry
        if tz_aware is None:
            tz_aware = opts.get("tz_aware", False)
        if connect is None:
            connect = opts.get("connect", True)
        keyword_opts["tz_aware"] = tz_aware
        keyword_opts["connect"] = connect

        # Handle deprecated options in kwarg options.
        keyword_opts = _handle_option_deprecations(keyword_opts)
        # Validate kwarg options.
        keyword_opts = common._CaseInsensitiveDictionary(
            dict(common.validate(keyword_opts.cased_key(k), v) for k, v in keyword_opts.items())
        )

        # Override connection string options with kwarg options.
        opts.update(keyword_opts)

        if srv_service_name is None:
            srv_service_name = opts.get("srvServiceName", common.SRV_SERVICE_NAME)

        srv_max_hosts = srv_max_hosts or opts.get("srvmaxhosts")
        # Handle security-option conflicts in combined options.
        opts = _handle_security_options(opts)
        # Normalize combined options.
        opts = _normalize_options(opts)
        _check_options(seeds, opts)

        # Username and password passed as kwargs override user info in URI.
        username = opts.get("username", username)
        password = opts.get("password", password)
        self._options = options = ClientOptions(username, password, dbase, opts)

        self._default_database_name = dbase
        self._lock = _create_lock()
        self._alock = _ALock(self._lock)
        self._kill_cursors_queue: list = []

        self._event_listeners = options.pool_options._event_listeners
        super().__init__(
            options.codec_options,
            options.read_preference,
            options.write_concern,
            options.read_concern,
        )

        self._topology_settings = TopologySettings(
            seeds=seeds,
            replica_set_name=options.replica_set_name,
            pool_class=pool_class,
            pool_options=options.pool_options,
            monitor_class=monitor_class,
            condition_class=condition_class,
            local_threshold_ms=options.local_threshold_ms,
            server_selection_timeout=options.server_selection_timeout,
            server_selector=options.server_selector,
            heartbeat_frequency=options.heartbeat_frequency,
            fqdn=fqdn,
            direct_connection=options.direct_connection,
            load_balanced=options.load_balanced,
            srv_service_name=srv_service_name,
            srv_max_hosts=srv_max_hosts,
            server_monitoring_mode=options.server_monitoring_mode,
        )

        self._init_background()

        self._encrypter = None
        if self._options.auto_encryption_opts:
            from pymongo.encryption import _Encrypter

            self._encrypter = _Encrypter(self, self._options.auto_encryption_opts)
        self._timeout = self._options.timeout

        if _HAS_REGISTER_AT_FORK:
            # Add this client to the list of weakly referenced items.
            # This will be used later if we fork.
            BaseMongoClient._clients[self._topology._topology_id] = self

    def _init_background(self) -> None:
        self._topology = Topology(self._topology_settings)

        async def target() -> bool:
            client = self_ref()
            if client is None:
                return False  # Stop the executor.
            await AsyncMongoClient._process_periodic_tasks(client)
            return True

        executor = periodic_executor.PeriodicExecutor(
            interval=common.KILL_CURSOR_FREQUENCY,
            min_interval=common.MIN_HEARTBEAT_INTERVAL,
            target=target,
            name="pymongo_kill_cursors_thread",
        )

        # We strongly reference the executor and it weakly references us via
        # this closure. When the client is freed, stop the executor soon.
        self_ref: Any = weakref.ref(self, executor.close)
        self._kill_cursors_executor = executor

    def _should_pin_cursor(self, session: Optional[ClientSession]) -> Optional[bool]:
        return self._options.load_balanced and not (session and session.in_transaction)

    def _after_fork(self) -> None:
        """Resets topology in a child after successfully forking."""
        self._init_background()

    def _duplicate(self, **kwargs: Any) -> BaseMongoClient:
        args = self._init_kwargs.copy()
        args.update(kwargs)
        if self._asynchronous:
            return AsyncMongoClient(**args)
        else:
            return MongoClient(**args)

    def watch(
        self,
        pipeline: Optional[_Pipeline] = None,
        full_document: Optional[str] = None,
        resume_after: Optional[Mapping[str, Any]] = None,
        max_await_time_ms: Optional[int] = None,
        batch_size: Optional[int] = None,
        collation: Optional[_CollationIn] = None,
        start_at_operation_time: Optional[Timestamp] = None,
        session: Optional[client_session.ClientSession] = None,
        start_after: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        full_document_before_change: Optional[str] = None,
        show_expanded_events: Optional[bool] = None,
    ) -> ChangeStream[_DocumentType]:
        """Watch changes on this cluster.

        Performs an aggregation with an implicit initial ``$changeStream``
        stage and returns a
        :class:`~pymongo.change_stream.ClusterChangeStream` cursor which
        iterates over changes on all databases on this cluster.

        Introduced in MongoDB 4.0.

        .. code-block:: python

           with client.watch() as stream:
               for change in stream:
                   print(change)

        The :class:`~pymongo.change_stream.ClusterChangeStream` iterable
        blocks until the next change document is returned or an error is
        raised. If the
        :meth:`~pymongo.change_stream.ClusterChangeStream.next` method
        encounters a network error when retrieving a batch from the server,
        it will automatically attempt to recreate the cursor such that no
        change events are missed. Any error encountered during the resume
        attempt indicates there may be an outage and will be raised.

        .. code-block:: python

            try:
                with client.watch([{"$match": {"operationType": "insert"}}]) as stream:
                    for insert_change in stream:
                        print(insert_change)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                logging.error("...")

        For a precise description of the resume process see the
        `change streams specification`_.

        :param pipeline: A list of aggregation pipeline stages to
            append to an initial ``$changeStream`` stage. Not all
            pipeline stages are valid after a ``$changeStream`` stage, see the
            MongoDB documentation on change streams for the supported stages.
        :param full_document: The fullDocument to pass as an option
            to the ``$changeStream`` stage. Allowed values: 'updateLookup',
            'whenAvailable', 'required'. When set to 'updateLookup', the
            change notification for partial updates will include both a delta
            describing the changes to the document, as well as a copy of the
            entire document that was changed from some time after the change
            occurred.
        :param full_document_before_change: Allowed values: 'whenAvailable'
            and 'required'. Change events may now result in a
            'fullDocumentBeforeChange' response field.
        :param resume_after: A resume token. If provided, the
            change stream will start returning changes that occur directly
            after the operation specified in the resume token. A resume token
            is the _id value of a change document.
        :param max_await_time_ms: The maximum time in milliseconds
            for the server to wait for changes before responding to a getMore
            operation.
        :param batch_size: The maximum number of documents to return
            per batch.
        :param collation: The :class:`~pymongo.collation.Collation`
            to use for the aggregation.
        :param start_at_operation_time: If provided, the resulting
            change stream will only return changes that occurred at or after
            the specified :class:`~bson.timestamp.Timestamp`. Requires
            MongoDB >= 4.0.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param start_after: The same as `resume_after` except that
            `start_after` can resume notifications after an invalidate event.
            This option and `resume_after` are mutually exclusive.
        :param comment: A user-provided comment to attach to this
            command.
        :param show_expanded_events: Include expanded events such as DDL events like `dropIndexes`.

        :return: A :class:`~pymongo.change_stream.ClusterChangeStream` cursor.

        .. versionchanged:: 4.3
           Added `show_expanded_events` parameter.

        .. versionchanged:: 4.2
            Added ``full_document_before_change`` parameter.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.9
           Added the ``start_after`` parameter.

        .. versionadded:: 3.7

        .. seealso:: The MongoDB documentation on `changeStreams <https://mongodb.com/docs/manual/changeStreams/>`_.

        .. _change streams specification:
            https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md
        """
        return ClusterChangeStream(
            self.admin,
            pipeline,
            full_document,
            resume_after,
            max_await_time_ms,
            batch_size,
            collation,
            start_at_operation_time,
            session,
            start_after,
            comment,
            full_document_before_change,
            show_expanded_events=show_expanded_events,
        )

    @property
    def topology_description(self) -> TopologyDescription:
        """The description of the connected MongoDB deployment.

        >>> client.topology_description
        <TopologyDescription id: 605a7b04e76489833a7c6113, topology_type: ReplicaSetWithPrimary, servers: [<ServerDescription ('localhost', 27017) server_type: RSPrimary, rtt: 0.0007973677999995488>, <ServerDescription ('localhost', 27018) server_type: RSSecondary, rtt: 0.0005540556000003249>, <ServerDescription ('localhost', 27019) server_type: RSSecondary, rtt: 0.0010367483999999649>]>
        >>> client.topology_description.topology_type_name
        'ReplicaSetWithPrimary'

        Note that the description is periodically updated in the background
        but the returned object itself is immutable. Access this property again
        to get a more recent
        :class:`~pymongo.topology_description.TopologyDescription`.

        :return: An instance of
          :class:`~pymongo.topology_description.TopologyDescription`.

        .. versionadded:: 4.0
        """
        return self._topology.description

    @property
    def is_mongos(self) -> bool:
        """If this client is connected to mongos. If the client is not
        connected, this will block until a connection is established or raise
        ServerSelectionTimeoutError if no server is available.
        """
        raise NotImplementedError

    @property
    def nodes(self) -> FrozenSet[_Address]:
        """Set of all currently connected servers.

        .. warning:: When connected to a replica set the value of :attr:`nodes`
          can change over time as :class:`MongoClient`'s view of the replica
          set changes. :attr:`nodes` can also be an empty set when
          :class:`MongoClient` is first instantiated and hasn't yet connected
          to any servers, or a network partition causes it to lose connection
          to all servers.
        """
        description = self._topology.description
        return frozenset(s.address for s in description.known_servers)

    @property
    def options(self) -> ClientOptions:
        """The configuration options for this client.

        :return: An instance of :class:`~pymongo.client_options.ClientOptions`.

        .. versionadded:: 4.0
        """
        return self._options

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self._topology == other._topology
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self._topology)

    def _repr_helper(self) -> str:
        def option_repr(option: str, value: Any) -> str:
            """Fix options whose __repr__ isn't usable in a constructor."""
            if option == "document_class":
                if value is dict:
                    return "document_class=dict"
                else:
                    return f"document_class={value.__module__}.{value.__name__}"
            if option in common.TIMEOUT_OPTIONS and value is not None:
                return f"{option}={int(value * 1000)}"

            return f"{option}={value!r}"

        # Host first...
        options = [
            "host=%r"
            % [
                "%s:%d" % (host, port) if port is not None else host
                for host, port in self._topology_settings.seeds
            ]
        ]
        # ... then everything in self._constructor_args...
        options.extend(
            option_repr(key, self._options._options[key]) for key in self._constructor_args
        )
        # ... then everything else.
        options.extend(
            option_repr(key, self._options._options[key])
            for key in self._options._options
            if key not in set(self._constructor_args) and key != "username" and key != "password"
        )
        return ", ".join(options)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._repr_helper()})"

    def __getattr__(self, name: str) -> database.Database[_DocumentType]:
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :param name: the name of the database to get
        """
        if name.startswith("_"):
            raise AttributeError(
                f"{type(self).__name__} has no attribute {name!r}. To access the {name}"
                f" database, use client[{name!r}]."
            )
        return self.__getitem__(name)

    def __getitem__(
        self, name: str
    ) -> database.AsyncDatabase[_DocumentType] | database.Database[_DocumentType]:
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :param name: the name of the database to get
        """
        if self._asynchronous:
            return database.AsyncDatabase(self, name)
        else:
            return database.Database(self, name)

    def _close_cursor_soon(
        self,
        cursor_id: int,
        address: Optional[_CursorAddress],
        conn_mgr: Optional[_ConnectionManager] = None,
    ) -> None:
        """Request that a cursor and/or connection be cleaned up soon."""
        self._kill_cursors_queue.append((address, cursor_id, conn_mgr))

    def _start_session(self, implicit: bool, **kwargs: Any) -> ClientSession:
        server_session = _EmptyServerSession()
        opts = client_session.SessionOptions(**kwargs)
        return client_session.ClientSession(self, server_session, opts, implicit)

    def start_session(
        self,
        causal_consistency: Optional[bool] = None,
        default_transaction_options: Optional[client_session.TransactionOptions] = None,
        snapshot: Optional[bool] = False,
    ) -> client_session.ClientSession:
        """Start a logical session.

        This method takes the same parameters as
        :class:`~pymongo.client_session.SessionOptions`. See the
        :mod:`~pymongo.client_session` module for details and examples.

        A :class:`~pymongo.client_session.ClientSession` may only be used with
        the MongoClient that started it. :class:`ClientSession` instances are
        **not thread-safe or fork-safe**. They can only be used by one thread
        or process at a time. A single :class:`ClientSession` cannot be used
        to run multiple operations concurrently.

        :return: An instance of :class:`~pymongo.client_session.ClientSession`.

        .. versionadded:: 3.6
        """
        return self._start_session(
            False,
            causal_consistency=causal_consistency,
            default_transaction_options=default_transaction_options,
            snapshot=snapshot,
        )

    def _ensure_session(self, session: Optional[ClientSession] = None) -> Optional[ClientSession]:
        """If provided session is None, lend a temporary session."""
        if session:
            return session

        try:
            # Don't make implicit sessions causally consistent. Applications
            # should always opt-in.
            return self._start_session(True, causal_consistency=False)
        except (ConfigurationError, InvalidOperation):
            # Sessions not supported.
            return None

    def _send_cluster_time(
        self, command: MutableMapping[str, Any], session: Optional[ClientSession]
    ) -> None:
        topology_time = self._topology.max_cluster_time()
        session_time = session.cluster_time if session else None
        if topology_time and session_time:
            if topology_time["clusterTime"] > session_time["clusterTime"]:
                cluster_time: Optional[ClusterTime] = topology_time
            else:
                cluster_time = session_time
        else:
            cluster_time = topology_time or session_time
        if cluster_time:
            command["$clusterTime"] = cluster_time

    def get_default_database(
        self,
        default: Optional[str] = None,
        codec_options: Optional[bson.CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
    ) -> database.Database[_DocumentType]:
        """Get the database named in the MongoDB connection URI.

        >>> uri = 'mongodb://host/my_database'
        >>> client = MongoClient(uri)
        >>> db = client.get_default_database()
        >>> assert db.name == 'my_database'
        >>> db = client.get_database()
        >>> assert db.name == 'my_database'

        Useful in scripts where you want to choose which database to use
        based only on the URI in a configuration file.

        :param default: the database name to use if no database name
            was provided in the URI.
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`MongoClient` is
            used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`MongoClient` is used. See :mod:`~pymongo.read_preferences`
            for options.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`MongoClient` is
            used.
        :param read_concern: An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`MongoClient` is
            used.
        :param comment: A user-provided comment to attach to this
            command.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.8
           Undeprecated. Added the ``default``, ``codec_options``,
           ``read_preference``, ``write_concern`` and ``read_concern``
           parameters.

        .. versionchanged:: 3.5
           Deprecated, use :meth:`get_database` instead.
        """
        if self._default_database_name is None and default is None:
            raise ConfigurationError("No default database name defined or provided.")

        name = cast(str, self._default_database_name or default)
        return database.Database(
            self, name, codec_options, read_preference, write_concern, read_concern
        )

    def get_database(
        self,
        name: Optional[str] = None,
        codec_options: Optional[bson.CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
    ) -> database.AsyncDatabase[_DocumentType] | database.Database[_DocumentType]:
        """Get a :class:`~pymongo.database.Database` with the given name and
        options.

        Useful for creating a :class:`~pymongo.database.Database` with
        different codec options, read preference, and/or write concern from
        this :class:`MongoClient`.

          >>> client.read_preference
          Primary()
          >>> db1 = client.test
          >>> db1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> db2 = client.get_database(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> db2.read_preference
          Secondary(tag_sets=None)

        :param name: The name of the database - a string. If ``None``
            (the default) the database named in the MongoDB connection URI is
            returned.
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`MongoClient` is
            used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`MongoClient` is used. See :mod:`~pymongo.read_preferences`
            for options.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`MongoClient` is
            used.
        :param read_concern: An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`MongoClient` is
            used.

        .. versionchanged:: 3.5
           The `name` parameter is now optional, defaulting to the database
           named in the MongoDB connection URI.
        """
        if name is None:
            if self._default_database_name is None:
                raise ConfigurationError("No default database defined")
            name = self._default_database_name

        if self._asynchronous:
            return database.AsyncDatabase(
                self, name, codec_options, read_preference, write_concern, read_concern
            )
        else:
            return database.Database(
                self, name, codec_options, read_preference, write_concern, read_concern
            )

    def _database_default_options(self, name: str) -> Database:
        """Get a Database instance with the default settings."""
        return self.get_database(
            name,
            codec_options=DEFAULT_CODEC_OPTIONS,
            read_preference=ReadPreference.PRIMARY,
            write_concern=DEFAULT_WRITE_CONCERN,
        )

    def __enter__(self) -> BaseMongoClient[_DocumentType]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # See PYTHON-3084.
    __iter__ = None

    def __next__(self) -> NoReturn:
        raise TypeError("'MongoClient' object is not iterable")

    next = __next__


class AsyncMongoClient(BaseMongoClient):
    """
    An asynchronous client-side representation of a MongoDB cluster.

    Instances can represent either a standalone MongoDB server, a replica
    set, or a sharded cluster. Instances of this class are responsible for
    maintaining up-to-date state of the cluster, and possibly cache
    resources related to this, including background threads for monitoring,
    and connection pools.
    """

    def __init__(
        self,
        host: Optional[Union[str, Sequence[str]]] = None,
        port: Optional[int] = None,
        document_class: Optional[Type[_DocumentType]] = None,
        tz_aware: Optional[bool] = None,
        connect: Optional[bool] = None,
        type_registry: Optional[TypeRegistry] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(host, port, document_class, tz_aware, connect, type_registry, **kwargs)
        self._asynchronous = True
        if connect:
            self._topology_task = asyncio.create_task(self._fetch_topology())

    async def _fetch_topology(self) -> Topology:
        await self._topology.open()
        async with self._alock:
            await self._kill_cursors_executor.open()
        return self._topology

    async def _server_property(self, attr_name: str) -> Any:
        """An attribute of the current server's description.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.

        Not threadsafe if used multiple times in a single method, since
        the server may change. In such cases, store a local reference to a
        ServerDescription first, then use its properties.
        """
        server = await self._topology.select_server(writable_server_selector, _Op.TEST)

        return getattr(server.description, attr_name)

    async def address(self) -> Optional[tuple[str, int]]:
        """(host, port) of the current standalone, primary, or mongos, or None.

        Accessing :attr:`address` raises :exc:`~.errors.InvalidOperation` if
        the client is load-balancing among mongoses, since there is no single
        address. Use :attr:`nodes` instead.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.

        .. versionadded:: 3.0
        """
        topology_type = self._topology._description.topology_type
        if (
            topology_type == TOPOLOGY_TYPE.Sharded
            and len(self.topology_description.server_descriptions()) > 1
        ):
            raise InvalidOperation(
                'Cannot use "address" property when load balancing among'
                ' mongoses, use "nodes" instead.'
            )
        if topology_type not in (
            TOPOLOGY_TYPE.ReplicaSetWithPrimary,
            TOPOLOGY_TYPE.Single,
            TOPOLOGY_TYPE.LoadBalanced,
            TOPOLOGY_TYPE.Sharded,
        ):
            return None
        return await self._server_property("address")

    async def primary(self) -> Optional[tuple[str, int]]:
        """The (host, port) of the current primary of the replica set.

        Returns ``None`` if this client is not connected to a replica set,
        there is no primary, or this client was created without the
        `replicaSet` option.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0.
        """
        return await self._topology.get_primary()  # type: ignore[return-value]

    async def secondaries(self) -> set[_Address]:
        """The secondary members known to this client.

        A sequence of (host, port) pairs. Empty if this client is not
        connected to a replica set, there are no visible secondaries, or this
        client was created without the `replicaSet` option.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0.
        """
        return await self._topology.get_secondaries()

    async def arbiters(self) -> set[_Address]:
        """Arbiters in the replica set.

        A sequence of (host, port) pairs. Empty if this client is not
        connected to a replica set, there are no arbiters, or this client was
        created without the `replicaSet` option.
        """
        return await self._topology.get_arbiters()

    async def is_primary(self) -> bool:
        """If this client is connected to a server that can accept writes.

        True if the current server is a standalone, mongos, or the primary of
        a replica set. If the client is not connected, this will block until a
        connection is established or raise ServerSelectionTimeoutError if no
        server is available.
        """
        return await self._server_property("is_writable")

    async def _end_sessions(self, session_ids: list[_ServerSession]) -> None:
        """Send endSessions command(s) with the given session ids."""
        try:
            # Use Connection.command directly to avoid implicitly creating
            # another session.
            async with await self._conn_for_reads(
                ReadPreference.PRIMARY_PREFERRED, None, operation=_Op.END_SESSIONS
            ) as (
                conn,
                read_pref,
            ):
                if not conn.supports_sessions:
                    return

                for i in range(0, len(session_ids), common._MAX_END_SESSIONS):
                    spec = {"endSessions": session_ids[i : i + common._MAX_END_SESSIONS]}
                    await conn.command("admin", spec, read_preference=read_pref, client=self)
        except PyMongoError:
            # Drivers MUST ignore any errors returned by the endSessions
            # command.
            pass

    async def close(self) -> None:
        """Cleanup client resources and disconnect from MongoDB.

        End all server sessions created by this client by sending one or more
        endSessions commands.

        Close all sockets in the connection pools and stop the monitor threads.

        .. versionchanged:: 4.0
           Once closed, the client cannot be used again and any attempt will
           raise :exc:`~pymongo.errors.InvalidOperation`.

        .. versionchanged:: 3.6
           End all server sessions created by this client.
        """
        session_ids = await self._topology.pop_all_sessions()
        if session_ids:
            await self._end_sessions(session_ids)
        # Stop the periodic task thread and then send pending killCursor
        # requests before closing the topology.
        self._kill_cursors_executor.close()
        await self._process_kill_cursors()
        await self._topology.close()
        if self._encrypter:
            # TODO: PYTHON-1921 Encrypted MongoClients cannot be re-opened.
            self._encrypter.close()

    async def _get_topology(self) -> Topology:
        """Get the internal :class:`~pymongo.topology.Topology` object.

        If this client was created with "connect=False", calling _get_topology
        launches the connection process in the background.
        """
        try:
            if self._topology_task is not None:
                return await self._topology_task
            else:
                self._topology_task = asyncio.create_task(self._fetch_topology())
        except AttributeError:
            self._topology_task = asyncio.create_task(self._fetch_topology())

        return await self._topology_task

    @contextlib.asynccontextmanager
    async def _checkout(
        self, server: Server, session: Optional[ClientSession]
    ) -> AsyncIterator[Connection]:
        in_txn = session and session.in_transaction
        async with _MongoClientErrorHandler(self, server, session) as err_handler:
            # Reuse the pinned connection, if it exists.
            if in_txn and session and session._pinned_connection:
                err_handler.contribute_socket(session._pinned_connection)
                yield session._pinned_connection
                return
            async with await server.checkout(handler=err_handler) as conn:
                # Pin this session to the selected server or connection.
                if (
                    in_txn
                    and session
                    and server.description.server_type
                    in (
                        SERVER_TYPE.Mongos,
                        SERVER_TYPE.LoadBalancer,
                    )
                ):
                    session._pin(server, conn)
                err_handler.contribute_socket(conn)
                if (
                    self._encrypter
                    and not self._encrypter._bypass_auto_encryption
                    and conn.max_wire_version < 8
                ):
                    raise ConfigurationError(
                        "Auto-encryption requires a minimum MongoDB version of 4.2"
                    )
                yield conn

    async def _select_server(
        self,
        server_selector: Callable[[Selection], Selection],
        session: Optional[ClientSession],
        operation: str,
        address: Optional[_Address] = None,
        deprioritized_servers: Optional[list[Server]] = None,
        operation_id: Optional[int] = None,
    ):
        """Select a server to run an operation on this client.

        :Parameters:
          - `server_selector`: The server selector to use if the session is
            not pinned and no address is given.
          - `session`: The ClientSession for the next operation, or None. May
            be pinned to a mongos server address.
          - `address` (optional): Address when sending a message
            to a specific server, used for getMore.
        """
        try:
            topology = await self._get_topology()
            if session and not session.in_transaction:
                session._transaction.reset()
            address = address or (session and session._pinned_address)
            if address:
                # We're running a getMore or this session is pinned to a mongos.
                server = await topology.select_server_by_address(
                    address, operation, operation_id=operation_id
                )
                if not server:
                    raise AutoReconnect("server %s:%d no longer available" % address)
            else:
                server = await topology.select_server(
                    server_selector,
                    operation,
                    deprioritized_servers=deprioritized_servers,
                    operation_id=operation_id,
                )
            return server
        except PyMongoError as exc:
            # Server selection errors in a transaction are transient.
            if session and session.in_transaction:
                exc._add_error_label("TransientTransactionError")
                session._unpin()
            raise

    async def _conn_for_writes(
        self, session: Optional[ClientSession], operation: str
    ) -> AsyncIterator[Connection]:
        server = await self._select_server(writable_server_selector, session, operation)
        return self._checkout(server, session)

    @contextlib.asynccontextmanager
    async def _conn_from_server(
        self, read_preference: _ServerMode, server: Server, session: Optional[ClientSession]
    ) -> AsyncIterator[tuple[Connection, _ServerMode]]:
        assert read_preference is not None, "read_preference must not be None"
        # Get a connection for a server matching the read preference, and yield
        # conn with the effective read preference. The Server Selection
        # Spec says not to send any $readPreference to standalones and to
        # always send primaryPreferred when directly connected to a repl set
        # member.
        # Thread safe: if the type is single it cannot change.
        topology = await self._get_topology()
        single = topology.description.topology_type == TOPOLOGY_TYPE.Single

        async with self._checkout(server, session) as conn:
            if single:
                if conn.is_repl and not (session and session.in_transaction):
                    # Use primary preferred to ensure any repl set member
                    # can handle the request.
                    read_preference = ReadPreference.PRIMARY_PREFERRED
                elif conn.is_standalone:
                    # Don't send read preference to standalones.
                    read_preference = ReadPreference.PRIMARY
            yield conn, read_preference

    async def _conn_for_reads(
        self,
        read_preference: _ServerMode,
        session: Optional[ClientSession],
        operation: str,
    ) -> AsyncIterator[tuple[Connection, _ServerMode]]:
        assert read_preference is not None, "read_preference must not be None"
        _ = await self._get_topology()
        server = await self._select_server(read_preference, session, operation)
        return self._conn_from_server(read_preference, server, session)

    @_csot.apply
    async def _run_operation(
        self,
        operation: Union[_Query, _GetMore],
        unpack_res: Callable,
        address: Optional[_Address] = None,
    ) -> Response:
        """Run a _Query/_GetMore operation and return a Response.

        :param operation: a _Query or _GetMore object.
        :param unpack_res: A callable that decodes the wire protocol response.
        :param address: Optional address when sending a message
            to a specific server, used for getMore.
        """
        if operation.conn_mgr:
            server = await self._select_server(
                operation.read_preference,
                operation.session,
                operation.name,
                address=address,
            )

            with operation.conn_mgr.lock:
                async with _MongoClientErrorHandler(self, server, operation.session) as err_handler:
                    err_handler.contribute_socket(operation.conn_mgr.conn)
                    return await server.run_operation(
                        operation.conn_mgr.conn,
                        operation,
                        operation.read_preference,
                        self._event_listeners,
                        unpack_res,
                        self,
                    )

        async def _cmd(
            _session: Optional[ClientSession],
            server: Server,
            conn: Connection,
            read_preference: _ServerMode,
        ) -> Response:
            operation.reset()  # Reset op in case of retry.
            return await server.run_operation(
                conn,
                operation,
                read_preference,
                self._event_listeners,
                unpack_res,
                self,
            )

        return await self._retryable_read(
            _cmd,
            operation.read_preference,
            operation.session,
            address=address,
            retryable=isinstance(operation, message._Query),
            operation=operation.name,
        )

    async def _retry_with_session(
        self,
        retryable: bool,
        func: _WriteCall[T],
        session: Optional[ClientSession],
        bulk: Optional[_Bulk],
        operation: str,
        operation_id: Optional[int] = None,
    ) -> T:
        """Execute an operation with at most one consecutive retries

        Returns func()'s return value on success. On error retries the same
        command.

        Re-raises any exception thrown by func().
        """
        # Ensure that the options supports retry_writes and there is a valid session not in
        # transaction, otherwise, we will not support retry behavior for this txn.
        retryable = bool(
            retryable and self.options.retry_writes and session and not session.in_transaction
        )
        return await self._retry_internal(
            func=func,
            session=session,
            bulk=bulk,
            operation=operation,
            retryable=retryable,
            operation_id=operation_id,
        )

    @_csot.apply
    async def _retry_internal(
        self,
        func: _WriteCall[T] | _ReadCall[T],
        session: Optional[ClientSession],
        bulk: Optional[_Bulk],
        operation: str,
        is_read: bool = False,
        address: Optional[_Address] = None,
        read_pref: Optional[_ServerMode] = None,
        retryable: bool = False,
        operation_id: Optional[int] = None,
    ) -> T:
        """Internal retryable helper for all client transactions.

        :param func: Callback function we want to retry
        :param session: Client Session on which the transaction should occur
        :param bulk: Abstraction to handle bulk write operations
        :param operation: The name of the operation that the server is being selected for
        :param is_read: If this is an exclusive read transaction, defaults to False
        :param address: Server Address, defaults to None
        :param read_pref: Topology of read operation, defaults to None
        :param retryable: If the operation should be retried once, defaults to None

        :return: Output of the calling func()
        """
        return await _ClientConnectionRetryable(
            mongo_client=self,
            func=func,
            bulk=bulk,
            operation=operation,
            is_read=is_read,
            session=session,
            read_pref=read_pref,
            address=address,
            retryable=retryable,
            operation_id=operation_id,
        ).run()

    async def _retryable_read(
        self,
        func: _ReadCall[T],
        read_pref: _ServerMode,
        session: Optional[ClientSession],
        operation: str,
        address: Optional[_Address] = None,
        retryable: bool = True,
        operation_id: Optional[int] = None,
    ) -> T:
        """Execute an operation with consecutive retries if possible

        Returns func()'s return value on success. On error retries the same
        command.

        Re-raises any exception thrown by func().

        :param func: Read call we want to execute
        :param read_pref: Desired topology of read operation
        :param session: Client session we should use to execute operation
        :param operation: The name of the operation that the server is being selected for
        :param address: Optional address when sending a message, defaults to None
        :param retryable: if we should attempt retries
            (may not always be supported even if supplied), defaults to False
        """

        # Ensure that the client supports retrying on reads and there is no session in
        # transaction, otherwise, we will not support retry behavior for this call.
        retryable = bool(
            retryable and self.options.retry_reads and not (session and session.in_transaction)
        )
        return await self._retry_internal(
            func,
            session,
            None,
            operation,
            is_read=True,
            address=address,
            read_pref=read_pref,
            retryable=retryable,
            operation_id=operation_id,
        )

    async def _retryable_write(
        self,
        retryable: bool,
        func: _WriteCall[T],
        session: Optional[ClientSession],
        operation: str,
        bulk: Optional[_Bulk] = None,
        operation_id: Optional[int] = None,
    ) -> T:
        """Execute an operation with consecutive retries if possible

        Returns func()'s return value on success. On error retries the same
        command.

        Re-raises any exception thrown by func().

        :param retryable: if we should attempt retries (may not always be supported)
        :param func: write call we want to execute during a session
        :param session: Client session we will use to execute write operation
        :param operation: The name of the operation that the server is being selected for
        :param bulk: bulk abstraction to execute operations in bulk, defaults to None
        """
        async with self._tmp_session(session) as s:
            return await self._retry_with_session(retryable, func, s, bulk, operation, operation_id)

    async def _cleanup_cursor(
        self,
        locks_allowed: bool,
        cursor_id: int,
        address: Optional[_CursorAddress],
        conn_mgr: _ConnectionManager,
        session: Optional[ClientSession],
        explicit_session: bool,
    ) -> None:
        """Cleanup a cursor from cursor.close() or __del__.

        This method handles cleanup for Cursors/CommandCursors including any
        pinned connection or implicit session attached at the time the cursor
        was closed or garbage collected.

        :param locks_allowed: True if we are allowed to acquire locks.
        :param cursor_id: The cursor id which may be 0.
        :param address: The _CursorAddress.
        :param conn_mgr: The _ConnectionManager for the pinned connection or None.
        :param session: The cursor's session.
        :param explicit_session: True if the session was passed explicitly.
        """
        if locks_allowed:
            if cursor_id:
                if conn_mgr and conn_mgr.more_to_come:
                    # If this is an exhaust cursor and we haven't completely
                    # exhausted the result set we *must* close the socket
                    # to stop the server from sending more data.
                    assert conn_mgr.conn is not None
                    conn_mgr.conn.close_conn(ConnectionClosedReason.ERROR)
                else:
                    await self._close_cursor_now(
                        cursor_id, address, session=session, conn_mgr=conn_mgr
                    )
            if conn_mgr:
                conn_mgr.close()
        else:
            # The cursor will be closed later in a different session.
            if cursor_id or conn_mgr:
                self._close_cursor_soon(cursor_id, address, conn_mgr)
        if session and not explicit_session:
            await session._end_session(lock=locks_allowed)

    async def _close_cursor_now(
        self,
        cursor_id: int,
        address: Optional[_CursorAddress],
        session: Optional[ClientSession] = None,
        conn_mgr: Optional[_ConnectionManager] = None,
    ) -> None:
        """Send a kill cursors message with the given id.

        The cursor is closed synchronously on the current thread.
        """
        if not isinstance(cursor_id, int):
            raise TypeError("cursor_id must be an instance of int")

        try:
            if conn_mgr:
                with conn_mgr.lock:  # TODO: ASYNC LOCK
                    # Cursor is pinned to LB outside of a transaction.
                    assert address is not None
                    assert conn_mgr.conn is not None
                    await self._kill_cursor_impl([cursor_id], address, session, conn_mgr.conn)
            else:
                await self._kill_cursors([cursor_id], address, self._get_topology(), session)
        except PyMongoError:
            # Make another attempt to kill the cursor later.
            self._close_cursor_soon(cursor_id, address)

    async def _kill_cursors(
        self,
        cursor_ids: Sequence[int],
        address: Optional[_CursorAddress],
        topology: Topology,
        session: Optional[ClientSession],
    ) -> None:
        """Send a kill cursors message with the given ids."""
        if address:
            # address could be a tuple or _CursorAddress, but
            # select_server_by_address needs (host, port).
            server = await topology.select_server_by_address(tuple(address), _Op.KILL_CURSORS)  # type: ignore[arg-type]
        else:
            # Application called close_cursor() with no address.
            server = await topology.select_server(writable_server_selector, _Op.KILL_CURSORS)

        async with self._checkout(server, session) as conn:
            assert address is not None
            await self._kill_cursor_impl(cursor_ids, address, session, conn)

    async def _kill_cursor_impl(
        self,
        cursor_ids: Sequence[int],
        address: _CursorAddress,
        session: Optional[ClientSession],
        conn: Connection,
    ) -> None:
        namespace = address.namespace
        db, coll = namespace.split(".", 1)
        spec = {"killCursors": coll, "cursors": cursor_ids}
        await conn.command(db, spec, session=session, client=self)

    async def _process_kill_cursors(self) -> None:
        """Process any pending kill cursors requests."""
        address_to_cursor_ids = defaultdict(list)
        pinned_cursors = []

        # Other threads or the GC may append to the queue concurrently.
        while True:
            try:
                address, cursor_id, conn_mgr = self._kill_cursors_queue.pop()
            except IndexError:
                break

            if conn_mgr:
                pinned_cursors.append((address, cursor_id, conn_mgr))
            else:
                address_to_cursor_ids[address].append(cursor_id)

        for address, cursor_id, conn_mgr in pinned_cursors:
            try:
                await self._cleanup_cursor(True, cursor_id, address, conn_mgr, None, False)
            except Exception as exc:
                if isinstance(exc, InvalidOperation) and self._topology._closed:
                    # Raise the exception when client is closed so that it
                    # can be caught in _process_periodic_tasks
                    raise
                else:
                    helpers._handle_exception()

        # Don't re-open topology if it's closed and there's no pending cursors.
        if address_to_cursor_ids:
            topology = await self._get_topology()
            for address, cursor_ids in address_to_cursor_ids.items():
                try:
                    await self._kill_cursors(cursor_ids, address, topology, session=None)
                except Exception as exc:
                    if isinstance(exc, InvalidOperation) and self._topology._closed:
                        raise
                    else:
                        helpers._handle_exception()

    # This method is run periodically by a background thread.
    async def _process_periodic_tasks(self) -> None:
        """Process any pending kill cursors requests and
        maintain connection pool parameters.
        """
        try:
            await self._process_kill_cursors()
            await self._topology.update_pool()
        except Exception as exc:
            if isinstance(exc, InvalidOperation) and self._topology._closed:
                return
            else:
                helpers._handle_exception()

    async def _return_server_session(
        self, server_session: Union[_ServerSession, _EmptyServerSession], lock: bool
    ) -> None:
        """Internal: return a _ServerSession to the pool."""
        if isinstance(server_session, _EmptyServerSession):
            return None
        return await self._topology.return_server_session(server_session, lock)

    @contextlib.asynccontextmanager
    async def _tmp_session(
        self, session: Optional[client_session.ClientSession], close: bool = True
    ) -> Generator[Optional[client_session.ClientSession], None, None]:
        """If provided session is None, lend a temporary session."""
        if session is not None:
            if not isinstance(session, client_session.ClientSession):
                raise ValueError("'session' argument must be a ClientSession or None.")
            # Don't call end_session.
            yield session
            return

        s = self._ensure_session(session)
        if s:
            try:
                yield s
            except Exception as exc:
                if isinstance(exc, ConnectionFailure):
                    s._server_session.mark_dirty()

                # Always call end_session on error.
                await s.end_session()
                raise
            finally:
                # Call end_session when we exit this scope.
                if close:
                    await s.end_session()
        else:
            yield None

    async def _process_response(
        self, reply: Mapping[str, Any], session: Optional[ClientSession]
    ) -> None:
        await self._topology.receive_cluster_time(reply.get("$clusterTime"))
        if session is not None:
            session._process_response(reply)

    async def server_info(
        self, session: Optional[client_session.ClientSession] = None
    ) -> dict[str, Any]:
        """Get information about the MongoDB server we're connected to.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        return cast(
            dict,
            await self.admin.command(
                "buildinfo", read_preference=ReadPreference.PRIMARY, session=session
            ),
        )

    async def _list_databases(
        self,
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[dict[str, Any]]:
        cmd = {"listDatabases": 1}
        cmd.update(kwargs)
        if comment is not None:
            cmd["comment"] = comment
        admin = self._database_default_options("admin")
        res = await admin._retryable_read_command(
            cmd, session=session, operation=_Op.LIST_DATABASES
        )
        # listDatabases doesn't return a cursor (yet). Fake one.
        cursor = {
            "id": 0,
            "firstBatch": res["databases"],
            "ns": "admin.$cmd",
        }
        return CommandCursor(admin["$cmd"], cursor, None, comment=comment)

    async def list_databases(
        self,
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[dict[str, Any]]:
        """Get a cursor over the databases of the connected server.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: Optional parameters of the
            `listDatabases command
            <https://mongodb.com/docs/manual/reference/command/listDatabases/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.


        :return: An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionadded:: 3.6
        """
        return await self._list_databases(session, comment, **kwargs)

    async def list_database_names(
        self,
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> list[str]:
        """Get a list of the names of all databases on the connected server.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionadded:: 3.6
        """
        return [
            doc["name"]
            async for doc in await self._list_databases(session, nameOnly=True, comment=comment)
        ]

    @_csot.apply
    async def drop_database(
        self,
        name_or_database: Union[str, database.Database[_DocumentTypeArg]],
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> None:
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        :class:`str` or :class:`~pymongo.database.Database`.

        :param name_or_database: the name of a database to drop, or a
            :class:`~pymongo.database.Database` instance representing the
            database to drop
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. note:: The :attr:`~pymongo.mongo_client.MongoClient.write_concern` of
           this client is automatically applied to this operation.

        .. versionchanged:: 3.4
           Apply this client's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        name = name_or_database
        if isinstance(name, database.Database):
            name = name.name

        if not isinstance(name, str):
            raise TypeError("name_or_database must be an instance of str or a Database")

        async with await self._conn_for_writes(session, operation=_Op.DROP_DATABASE) as conn:
            await self[name]._command(
                conn,
                {"dropDatabase": 1, "comment": comment},
                read_preference=ReadPreference.PRIMARY,
                write_concern=self._write_concern_for(session),
                parse_write_concern_error=True,
                session=session,
            )


def _retryable_error_doc(exc: PyMongoError) -> Optional[Mapping[str, Any]]:
    """Return the server response from PyMongo exception or None."""
    if isinstance(exc, BulkWriteError):
        # Check the last writeConcernError to determine if this
        # BulkWriteError is retryable.
        wces = exc.details["writeConcernErrors"]
        return wces[-1] if wces else None
    if isinstance(exc, (NotPrimaryError, OperationFailure)):
        return cast(Mapping[str, Any], exc.details)
    return None


def _add_retryable_write_error(exc: PyMongoError, max_wire_version: int, is_mongos: bool) -> None:
    doc = _retryable_error_doc(exc)
    if doc:
        code = doc.get("code", 0)
        # retryWrites on MMAPv1 should raise an actionable error.
        if code == 20 and str(exc).startswith("Transaction numbers"):
            errmsg = (
                "This MongoDB deployment does not support "
                "retryable writes. Please add retryWrites=false "
                "to your connection string."
            )
            raise OperationFailure(errmsg, code, exc.details)  # type: ignore[attr-defined]
        if max_wire_version >= 9:
            # In MongoDB 4.4+, the server reports the error labels.
            for label in doc.get("errorLabels", []):
                exc._add_error_label(label)
        else:
            # Do not consult writeConcernError for pre-4.4 mongos.
            if isinstance(exc, WriteConcernError) and is_mongos:
                pass
            elif code in helpers._RETRYABLE_ERROR_CODES:
                exc._add_error_label("RetryableWriteError")

    # Connection errors are always retryable except NotPrimaryError and WaitQueueTimeoutError which is
    # handled above.
    if isinstance(exc, ConnectionFailure) and not isinstance(
        exc, (NotPrimaryError, WaitQueueTimeoutError)
    ):
        exc._add_error_label("RetryableWriteError")


class _MongoClientErrorHandler:
    """Handle errors raised when executing an operation."""

    __slots__ = (
        "client",
        "server_address",
        "session",
        "max_wire_version",
        "sock_generation",
        "completed_handshake",
        "service_id",
        "handled",
    )

    def __init__(self, client: BaseMongoClient, server: Server, session: Optional[ClientSession]):
        self.client = client
        self.server_address = server.description.address
        self.session = session
        self.max_wire_version = common.MIN_WIRE_VERSION
        # XXX: When get_socket fails, this generation could be out of date:
        # "Note that when a network error occurs before the handshake
        # completes then the error's generation number is the generation
        # of the pool at the time the connection attempt was started."
        self.sock_generation = server.pool.gen.get_overall()
        self.completed_handshake = False
        self.service_id: Optional[ObjectId] = None
        self.handled = False

    def contribute_socket(self, conn: Connection, completed_handshake: bool = True) -> None:
        """Provide socket information to the error handler."""
        self.max_wire_version = conn.max_wire_version
        self.sock_generation = conn.generation
        self.service_id = conn.service_id
        self.completed_handshake = completed_handshake

    async def handle(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException]
    ) -> None:
        if self.handled or exc_val is None:
            return
        self.handled = True
        if self.session:
            if isinstance(exc_val, ConnectionFailure):
                if self.session.in_transaction:
                    exc_val._add_error_label("TransientTransactionError")
                self.session._server_session.mark_dirty()

            if isinstance(exc_val, PyMongoError):
                if exc_val.has_error_label("TransientTransactionError") or exc_val.has_error_label(
                    "RetryableWriteError"
                ):
                    self.session._unpin()
        err_ctx = _ErrorContext(
            exc_val,
            self.max_wire_version,
            self.sock_generation,
            self.completed_handshake,
            self.service_id,
        )
        await self.client._topology.handle_error(self.server_address, err_ctx)

    async def __aenter__(self) -> _MongoClientErrorHandler:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_val: Optional[Exception],
        exc_tb: Optional[TracebackType],
    ) -> None:
        return await self.handle(exc_type, exc_val)


class _ClientConnectionRetryable(Generic[T]):
    """Responsible for executing retryable connections on read or write operations"""

    def __init__(
        self,
        mongo_client: BaseMongoClient,
        func: _WriteCall[T] | _ReadCall[T],
        bulk: Optional[_Bulk],
        operation: str,
        is_read: bool = False,
        session: Optional[ClientSession] = None,
        read_pref: Optional[_ServerMode] = None,
        address: Optional[_Address] = None,
        retryable: bool = False,
        operation_id: Optional[int] = None,
    ):
        self._last_error: Optional[Exception] = None
        self._retrying = False
        self._multiple_retries = _csot.get_timeout() is not None
        self._client = mongo_client

        self._func = func
        self._bulk = bulk
        self._session = session
        self._is_read = is_read
        self._retryable = retryable
        self._read_pref = read_pref
        self._server_selector: Callable[[Selection], Selection] = (
            read_pref if is_read else writable_server_selector  # type: ignore
        )
        self._address = address
        self._server: Server = None  # type: ignore
        self._deprioritized_servers: list[Server] = []
        self._operation = operation
        self._operation_id = operation_id

    async def run(self) -> T:
        """Runs the supplied func() and attempts a retry

        :raises: self._last_error: Last exception raised

        :return: Result of the func() call
        """
        # Increment the transaction id up front to ensure any retry attempt
        # will use the proper txnNumber, even if server or socket selection
        # fails before the command can be sent.
        if self._is_session_state_retryable() and self._retryable and not self._is_read:
            self._session._start_retryable_write()  # type: ignore
            if self._bulk:
                self._bulk.started_retryable_write = True

        while True:
            self._check_last_error(check_csot=True)
            try:
                return await self._read() if self._is_read else await self._write()
            except ServerSelectionTimeoutError:
                # The application may think the write was never attempted
                # if we raise ServerSelectionTimeoutError on the retry
                # attempt. Raise the original exception instead.
                self._check_last_error()
                # A ServerSelectionTimeoutError error indicates that there may
                # be a persistent outage. Attempting to retry in this case will
                # most likely be a waste of time.
                raise
            except PyMongoError as exc:
                # Execute specialized catch on read
                if self._is_read:
                    if isinstance(exc, (ConnectionFailure, OperationFailure)):
                        # ConnectionFailures do not supply a code property
                        exc_code = getattr(exc, "code", None)
                        if self._is_not_eligible_for_retry() or (
                            isinstance(exc, OperationFailure)
                            and exc_code not in helpers._RETRYABLE_ERROR_CODES
                        ):
                            raise
                        self._retrying = True
                        self._last_error = exc
                    else:
                        raise

                # Specialized catch on write operation
                if not self._is_read:
                    if not self._retryable:
                        raise
                    retryable_write_error_exc = exc.has_error_label("RetryableWriteError")
                    if retryable_write_error_exc:
                        assert self._session
                        self._session._unpin()
                    if not retryable_write_error_exc or self._is_not_eligible_for_retry():
                        if exc.has_error_label("NoWritesPerformed") and self._last_error:
                            raise self._last_error from exc
                        else:
                            raise
                    if self._bulk:
                        self._bulk.retrying = True
                    else:
                        self._retrying = True
                    if not exc.has_error_label("NoWritesPerformed"):
                        self._last_error = exc
                    if self._last_error is None:
                        self._last_error = exc

                if self._client.topology_description.topology_type == TOPOLOGY_TYPE.Sharded:
                    self._deprioritized_servers.append(self._server)

    def _is_not_eligible_for_retry(self) -> bool:
        """Checks if the exchange is not eligible for retry"""
        return not self._retryable or (self._is_retrying() and not self._multiple_retries)

    def _is_retrying(self) -> bool:
        """Checks if the exchange is currently undergoing a retry"""
        return self._bulk.retrying if self._bulk else self._retrying

    def _is_session_state_retryable(self) -> bool:
        """Checks if provided session is eligible for retry

        reads: Make sure there is no ongoing transaction (if provided a session)
        writes: Make sure there is a session without an active transaction
        """
        if self._is_read:
            return not (self._session and self._session.in_transaction)
        return bool(self._session and not self._session.in_transaction)

    def _check_last_error(self, check_csot: bool = False) -> None:
        """Checks if the ongoing client exchange experienced a exception previously.
        If so, raise last error

        :param check_csot: Checks CSOT to ensure we are retrying with time remaining defaults to False
        """
        if self._is_retrying():
            remaining = _csot.remaining()
            if not check_csot or (remaining is not None and remaining <= 0):
                assert self._last_error is not None
                raise self._last_error

    async def _get_server(self) -> Server:
        """Retrieves a server object based on provided object context

        :return: Abstraction to connect to server
        """
        return await self._client._select_server(
            self._server_selector,
            self._session,
            self._operation,
            address=self._address,
            deprioritized_servers=self._deprioritized_servers,
            operation_id=self._operation_id,
        )

    async def _write(self) -> T:
        """Wrapper method for write-type retryable client executions

        :return: Output for func()'s call
        """
        try:
            max_wire_version = 0
            is_mongos = False
            self._server = await self._get_server()
            async with self._client._checkout(self._server, self._session) as conn:
                max_wire_version = conn.max_wire_version
                sessions_supported = (
                    self._session
                    and self._server.description.retryable_writes_supported
                    and conn.supports_sessions
                )
                is_mongos = conn.is_mongos
                if not sessions_supported:
                    # A retry is not possible because this server does
                    # not support sessions raise the last error.
                    self._check_last_error()
                    self._retryable = False
                return await self._func(self._session, conn, self._retryable)  # type: ignore
        except PyMongoError as exc:
            if not self._retryable:
                raise
            # Add the RetryableWriteError label, if applicable.
            _add_retryable_write_error(exc, max_wire_version, is_mongos)
            raise

    async def _read(self) -> T:
        """Wrapper method for read-type retryable client executions

        :return: Output for func()'s call
        """
        self._server = await self._get_server()
        assert self._read_pref is not None, "Read Preference required on read calls"
        async with self._client._conn_from_server(self._read_pref, self._server, self._session) as (
            conn,
            read_pref,
        ):
            if self._retrying and not self._retryable:
                self._check_last_error()
            return await self._func(self._session, self._server, conn, read_pref)  # type: ignore


class MongoClient(BaseMongoClient):
    def __init__(
        self,
        host: Optional[Union[str, Sequence[str]]] = None,
        port: Optional[int] = None,
        document_class: Optional[Type[_DocumentType]] = None,
        tz_aware: Optional[bool] = None,
        connect: Optional[bool] = None,
        type_registry: Optional[TypeRegistry] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(host, port, document_class, tz_aware, connect, type_registry, **kwargs)
        self._asynchronous = False
        if connect:
            self._topology_task = schedule_task(self._fetch_topology())

    @synchronize(AsyncMongoClient)
    def address(self) -> Optional[tuple[str, int]]:
        ...

    @synchronize(AsyncMongoClient)
    def primary(self) -> Optional[tuple[str, int]]:
        ...

    @synchronize(AsyncMongoClient)
    def secondaries(self) -> set[_Address]:
        ...

    @synchronize(AsyncMongoClient)
    def arbiters(self) -> set[_Address]:
        ...

    @synchronize(AsyncMongoClient)
    def is_primary(self) -> bool:
        ...

    @synchronize(AsyncMongoClient)
    def server_info(self, session: Optional[client_session.ClientSession] = None) -> dict[str, Any]:
        ...

    @synchronize(AsyncMongoClient)
    def close(self) -> None:
        ...

    @synchronize(AsyncMongoClient)
    def list_databases(
        self,
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[dict[str, Any]]:
        ...

    @synchronize(AsyncMongoClient)
    def list_database_names(
        self,
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> list[str]:
        ...

    @synchronize(AsyncMongoClient)
    def drop_database(
        self,
        name_or_database: Union[str, database.Database[_DocumentTypeArg]],
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> None:
        ...

    _server_property = AsyncMongoClient._server_property
    _conn_for_reads = AsyncMongoClient._conn_for_reads
    _conn_for_writes = AsyncMongoClient._conn_for_writes
    _conn_from_server = AsyncMongoClient._conn_from_server
    _select_server = AsyncMongoClient._select_server
    _checkout = AsyncMongoClient._checkout
    _tmp_session = AsyncMongoClient._tmp_session
    _return_server_session = AsyncMongoClient._return_server_session
    _process_response = AsyncMongoClient._process_response
    _retryable_read = AsyncMongoClient._retryable_read
    _retryable_write = AsyncMongoClient._retryable_write
    _retry_with_session = AsyncMongoClient._retry_with_session
    _retry_internal = AsyncMongoClient._retry_internal
    _cleanup_cursor = AsyncMongoClient._cleanup_cursor
    _close_cursor_now = AsyncMongoClient._close_cursor_now
    _kill_cursors = AsyncMongoClient._kill_cursors
    _kill_cursor_impl = AsyncMongoClient._kill_cursor_impl
    _process_kill_cursors = AsyncMongoClient._process_kill_cursors
    _process_periodic_tasks = AsyncMongoClient._process_periodic_tasks
    _list_databases = AsyncMongoClient._list_databases
    _run_operation = AsyncMongoClient._run_operation
    _fetch_topology = AsyncMongoClient._fetch_topology
    _get_topology = AsyncMongoClient._get_topology
    _end_sessions = AsyncMongoClient._end_sessions


def _after_fork_child() -> None:
    """Releases the locks in child process and resets the
    topologies in all MongoClients.
    """
    # Reinitialize locks
    _release_locks()

    # Perform cleanup in clients (i.e. get rid of topology)
    for _, client in MongoClient._clients.items():
        client._after_fork()


def _detect_external_db(entity: str) -> bool:
    """Detects external database hosts and logs an informational message at the INFO level."""
    entity = entity.lower()
    cosmos_db_hosts = [".cosmos.azure.com"]
    document_db_hosts = [".docdb.amazonaws.com", ".docdb-elastic.amazonaws.com"]

    for host in cosmos_db_hosts:
        if entity.endswith(host):
            _log_or_warn(
                _CLIENT_LOGGER,
                "You appear to be connected to a CosmosDB cluster. For more information regarding feature "
                "compatibility and support please visit https://www.mongodb.com/supportability/cosmosdb",
            )
            return True
    for host in document_db_hosts:
        if entity.endswith(host):
            _log_or_warn(
                _CLIENT_LOGGER,
                "You appear to be connected to a DocumentDB cluster. For more information regarding feature "
                "compatibility and support please visit https://www.mongodb.com/supportability/documentdb",
            )
            return True
    return False


if _HAS_REGISTER_AT_FORK:
    # This will run in the same thread as the fork was called.
    # If we fork in a critical region on the same thread, it should break.
    # This is fine since we would never call fork directly from a critical region.
    os.register_at_fork(after_in_child=_after_fork_child)
