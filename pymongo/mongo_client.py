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
  >>> c['test-database']
  Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'test-database')
"""

import contextlib
import threading
import weakref
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    FrozenSet,
    Generic,
    List,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from bson.codec_options import DEFAULT_CODEC_OPTIONS, CodecOptions, TypeRegistry
from bson.son import SON
from bson.timestamp import Timestamp
from pymongo import (
    client_session,
    common,
    database,
    helpers,
    message,
    periodic_executor,
    uri_parser,
)
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
)
from pymongo.pool import ConnectionClosedReason
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.server_selectors import writable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.settings import TopologySettings
from pymongo.topology import Topology, _ErrorContext
from pymongo.topology_description import TOPOLOGY_TYPE, TopologyDescription
from pymongo.typings import _Address, _CollationIn, _DocumentType, _Pipeline
from pymongo.uri_parser import (
    _check_options,
    _handle_option_deprecations,
    _handle_security_options,
    _normalize_options,
)
from pymongo.write_concern import DEFAULT_WRITE_CONCERN, WriteConcern

if TYPE_CHECKING:
    from pymongo.read_concern import ReadConcern


class MongoClient(common.BaseObject, Generic[_DocumentType]):
    """
    A client-side representation of a MongoDB cluster.

    Instances can represent either a standalone MongoDB server, a replica
    set, or a sharded cluster. Instances of this class are responsible for
    maintaining up-to-date state of the cluster, and possibly cache
    resources related to this, including background threads for monitoring,
    and connection pools.
    """

    HOST = "localhost"
    PORT = 27017
    # Define order to retrieve options from ClientOptions for __repr__.
    # No host/port; these are retrieved from TopologySettings.
    _constructor_args = ("document_class", "tz_aware", "connect")

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
        """Client for a MongoDB instance, a replica set, or a set of mongoses.

        The client object is thread-safe and has connection-pooling built in.
        If an operation fails because of a network error,
        :class:`~pymongo.errors.ConnectionFailure` is raised and the client
        reconnects in the background. Application code should handle this
        exception (recognizing that the operation failed) and then continue to
        execute.

        The `host` parameter can be a full `mongodb URI
        <http://dochub.mongodb.org/core/connections>`_, in addition to
        a simple hostname. It can also be a list of hostnames but no more
        than one URI. Any port specified in the host string(s) will override
        the `port` parameter. For username and
        passwords reserved characters like ':', '/', '+' and '@' must be
        percent encoded following RFC 2396::

            from urllib.parse import quote_plus

            uri = "mongodb://%s:%s@%s" % (
                quote_plus(user), quote_plus(password), host)
            client = MongoClient(uri)

        Unix domain sockets are also supported. The socket path must be percent
        encoded in the URI::

            uri = "mongodb://%s:%s@%s" % (
                quote_plus(user), quote_plus(password), quote_plus(socket_path))
            client = MongoClient(uri)

        But not when passed as a simple hostname::

            client = MongoClient('/tmp/mongodb-27017.sock')

        Starting with version 3.6, PyMongo supports mongodb+srv:// URIs. The
        URI must include one, and only one, hostname. The hostname will be
        resolved to one or more DNS `SRV records
        <https://en.wikipedia.org/wiki/SRV_record>`_ which will be used
        as the seed list for connecting to the MongoDB deployment. When using
        SRV URIs, the `authSource` and `replicaSet` configuration options can
        be specified using `TXT records
        <https://en.wikipedia.org/wiki/TXT_record>`_. See the
        `Initial DNS Seedlist Discovery spec
        <https://github.com/mongodb/specifications/blob/master/source/
        initial-dns-seedlist-discovery/initial-dns-seedlist-discovery.rst>`_
        for more details. Note that the use of SRV URIs implicitly enables
        TLS support. Pass tls=false in the URI to override.

        .. note:: MongoClient creation will block waiting for answers from
          DNS when mongodb+srv:// URIs are used.

        .. note:: Starting with version 3.0 the :class:`MongoClient`
          constructor no longer blocks while connecting to the server or
          servers, and it no longer raises
          :class:`~pymongo.errors.ConnectionFailure` if they are
          unavailable, nor :class:`~pymongo.errors.ConfigurationError`
          if the user's credentials are wrong. Instead, the constructor
          returns immediately and launches the connection process on
          background threads. You can check if the server is available
          like this::

            from pymongo.errors import ConnectionFailure
            client = MongoClient()
            try:
                # The ping command is cheap and does not require auth.
                client.admin.command('ping')
            except ConnectionFailure:
                print("Server not available")

        .. warning:: When using PyMongo in a multiprocessing context, please
          read :ref:`multiprocessing` first.

        .. note:: Many of the following options can be passed using a MongoDB
          URI or keyword parameters. If the same option is passed in a URI and
          as a keyword parameter the keyword parameter takes precedence.

        :Parameters:
          - `host` (optional): hostname or IP address or Unix domain socket
            path of a single mongod or mongos instance to connect to, or a
            mongodb URI, or a list of hostnames (but no more than one mongodb
            URI). If `host` is an IPv6 literal it must be enclosed in '['
            and ']' characters
            following the RFC2732 URL syntax (e.g. '[::1]' for localhost).
            Multihomed and round robin DNS addresses are **not** supported.
          - `port` (optional): port number on which to connect
          - `document_class` (optional): default class to use for
            documents returned from queries on this client
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`MongoClient` will be timezone
            aware (otherwise they will be naive)
          - `connect` (optional): if ``True`` (the default), immediately
            begin connecting to MongoDB in the background. Otherwise connect
            on the first operation.
          - `type_registry` (optional): instance of
            :class:`~bson.codec_options.TypeRegistry` to enable encoding
            and decoding of custom types.

          | **Other optional parameters can be passed as keyword arguments:**

          - `directConnection` (optional): if ``True``, forces this client to
             connect directly to the specified MongoDB host as a standalone.
             If ``false``, the client connects to the entire replica set of
             which the given MongoDB host(s) is a part. If this is ``True``
             and a mongodb+srv:// URI or a URI containing multiple seeds is
             provided, an exception will be raised.
          - `maxPoolSize` (optional): The maximum allowable number of
            concurrent connections to each connected server. Requests to a
            server will block if there are `maxPoolSize` outstanding
            connections to the requested server. Defaults to 100. Can be
            either 0 or None, in which case there is no limit on the number
            of concurrent connections.
          - `minPoolSize` (optional): The minimum required number of concurrent
            connections that the pool will maintain to each connected server.
            Default is 0.
          - `maxIdleTimeMS` (optional): The maximum number of milliseconds that
            a connection can remain idle in the pool before being removed and
            replaced. Defaults to `None` (no limit).
          - `maxConnecting` (optional): The maximum number of connections that
            each pool can establish concurrently. Defaults to `2`.
          - `socketTimeoutMS`: (integer or None) Controls how long (in
            milliseconds) the driver will wait for a response after sending an
            ordinary (non-monitoring) database operation before concluding that
            a network error has occurred. ``0`` or ``None`` means no timeout.
            Defaults to ``None`` (no timeout).
          - `connectTimeoutMS`: (integer or None) Controls how long (in
            milliseconds) the driver will wait during server monitoring when
            connecting a new socket to a server before concluding the server
            is unavailable. ``0`` or ``None`` means no timeout.
            Defaults to ``20000`` (20 seconds).
          - `server_selector`: (callable or None) Optional, user-provided
            function that augments server selection rules. The function should
            accept as an argument a list of
            :class:`~pymongo.server_description.ServerDescription` objects and
            return a list of server descriptions that should be considered
            suitable for the desired operation.
          - `serverSelectionTimeoutMS`: (integer) Controls how long (in
            milliseconds) the driver will wait to find an available,
            appropriate server to carry out a database operation; while it is
            waiting, multiple server monitoring operations may be carried out,
            each controlled by `connectTimeoutMS`. Defaults to ``30000`` (30
            seconds).
          - `waitQueueTimeoutMS`: (integer or None) How long (in milliseconds)
            a thread will wait for a socket from the pool if the pool has no
            free sockets. Defaults to ``None`` (no timeout).
          - `heartbeatFrequencyMS`: (optional) The number of milliseconds
            between periodic server checks, or None to accept the default
            frequency of 10 seconds.
          - `appname`: (string or None) The name of the application that
            created this MongoClient instance. The server will log this value
            upon establishing each connection. It is also recorded in the slow
            query log and profile collections.
          - `driver`: (pair or None) A driver implemented on top of PyMongo can
            pass a :class:`~pymongo.driver_info.DriverInfo` to add its name,
            version, and platform to the message printed in the server log when
            establishing a connection.
          - `event_listeners`: a list or tuple of event listeners. See
            :mod:`~pymongo.monitoring` for details.
          - `retryWrites`: (boolean) Whether supported write operations
            executed within this MongoClient will be retried once after a
            network error. Defaults to ``True``.
            The supported write operations are:

              - :meth:`~pymongo.collection.Collection.bulk_write`, as long as
                :class:`~pymongo.operations.UpdateMany` or
                :class:`~pymongo.operations.DeleteMany` are not included.
              - :meth:`~pymongo.collection.Collection.delete_one`
              - :meth:`~pymongo.collection.Collection.insert_one`
              - :meth:`~pymongo.collection.Collection.insert_many`
              - :meth:`~pymongo.collection.Collection.replace_one`
              - :meth:`~pymongo.collection.Collection.update_one`
              - :meth:`~pymongo.collection.Collection.find_one_and_delete`
              - :meth:`~pymongo.collection.Collection.find_one_and_replace`
              - :meth:`~pymongo.collection.Collection.find_one_and_update`

            Unsupported write operations include, but are not limited to,
            :meth:`~pymongo.collection.Collection.aggregate` using the ``$out``
            pipeline operator and any operation with an unacknowledged write
            concern (e.g. {w: 0})). See
            https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst
          - `retryReads`: (boolean) Whether supported read operations
            executed within this MongoClient will be retried once after a
            network error. Defaults to ``True``.
            The supported read operations are:
            :meth:`~pymongo.collection.Collection.find`,
            :meth:`~pymongo.collection.Collection.find_one`,
            :meth:`~pymongo.collection.Collection.aggregate` without ``$out``,
            :meth:`~pymongo.collection.Collection.distinct`,
            :meth:`~pymongo.collection.Collection.count`,
            :meth:`~pymongo.collection.Collection.estimated_document_count`,
            :meth:`~pymongo.collection.Collection.count_documents`,
            :meth:`pymongo.collection.Collection.watch`,
            :meth:`~pymongo.collection.Collection.list_indexes`,
            :meth:`pymongo.database.Database.watch`,
            :meth:`~pymongo.database.Database.list_collections`,
            :meth:`pymongo.mongo_client.MongoClient.watch`,
            and :meth:`~pymongo.mongo_client.MongoClient.list_databases`.

            Unsupported read operations include, but are not limited to
            :meth:`~pymongo.database.Database.command` and any getMore
            operation on a cursor.

            Enabling retryable reads makes applications more resilient to
            transient errors such as network failures, database upgrades, and
            replica set failovers. For an exact definition of which errors
            trigger a retry, see the `retryable reads specification
            <https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst>`_.

          - `compressors`: Comma separated list of compressors for wire
            protocol compression. The list is used to negotiate a compressor
            with the server. Currently supported options are "snappy", "zlib"
            and "zstd". Support for snappy requires the
            `python-snappy <https://pypi.org/project/python-snappy/>`_ package.
            zlib support requires the Python standard library zlib module. zstd
            requires the `zstandard <https://pypi.org/project/zstandard/>`_
            package. By default no compression is used. Compression support
            must also be enabled on the server. MongoDB 3.6+ supports snappy
            and zlib compression. MongoDB 4.2+ adds support for zstd.
          - `zlibCompressionLevel`: (int) The zlib compression level to use
            when zlib is used as the wire protocol compressor. Supported values
            are -1 through 9. -1 tells the zlib library to use its default
            compression level (usually 6). 0 means no compression. 1 is best
            speed. 9 is best compression. Defaults to -1.
          - `uuidRepresentation`: The BSON representation to use when encoding
            from and decoding to instances of :class:`~uuid.UUID`. Valid
            values are the strings: "standard", "pythonLegacy", "javaLegacy",
            "csharpLegacy", and "unspecified" (the default). New applications
            should consider setting this to "standard" for cross language
            compatibility. See :ref:`handling-uuid-data-example` for details.
          - `unicode_decode_error_handler`: The error handler to apply when
            a Unicode-related error occurs during BSON decoding that would
            otherwise raise :exc:`UnicodeDecodeError`. Valid options include
            'strict', 'replace', 'backslashreplace', 'surrogateescape', and
            'ignore'. Defaults to 'strict'.
          - `srvServiceName`: (string) The SRV service name to use for
            "mongodb+srv://" URIs. Defaults to "mongodb". Use it like so::

                MongoClient("mongodb+srv://example.com/?srvServiceName=customname")


          | **Write Concern options:**
          | (Only set if passed. No default values.)

          - `w`: (integer or string) If this is a replica set, write operations
            will block until they have been replicated to the specified number
            or tagged set of servers. `w=<int>` always includes the replica set
            primary (e.g. w=3 means write to the primary and wait until
            replicated to **two** secondaries). Passing w=0 **disables write
            acknowledgement** and all other write concern options.
          - `wTimeoutMS`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised. Passing wTimeoutMS=0
            will cause **write operations to wait indefinitely**.
          - `journal`: If ``True`` block until write operations have been
            committed to the journal. Cannot be used in combination with
            `fsync`. Write operations will fail with an exception if this
            option is used when the server is running without journaling.
          - `fsync`: If ``True`` and the server is running without journaling,
            blocks until the server has synced all data files to disk. If the
            server is running with journaling, this acts the same as the `j`
            option, blocking until write operations have been committed to the
            journal. Cannot be used in combination with `j`.

          | **Replica set keyword arguments for connecting with a replica set
            - either directly or via a mongos:**

          - `replicaSet`: (string or None) The name of the replica set to
            connect to. The driver will verify that all servers it connects to
            match this name. Implies that the hosts specified are a seed list
            and the driver should attempt to find all members of the set.
            Defaults to ``None``.

          | **Read Preference:**

          - `readPreference`: The replica set read preference for this client.
            One of ``primary``, ``primaryPreferred``, ``secondary``,
            ``secondaryPreferred``, or ``nearest``. Defaults to ``primary``.
          - `readPreferenceTags`: Specifies a tag set as a comma-separated list
            of colon-separated key-value pairs. For example ``dc:ny,rack:1``.
            Defaults to ``None``.
          - `maxStalenessSeconds`: (integer) The maximum estimated
            length of time a replica set secondary can fall behind the primary
            in replication before it will no longer be selected for operations.
            Defaults to ``-1``, meaning no maximum. If maxStalenessSeconds
            is set, it must be a positive integer greater than or equal to
            90 seconds.

          .. seealso:: :doc:`/examples/server_selection`

          | **Authentication:**

          - `username`: A string.
          - `password`: A string.

            Although username and password must be percent-escaped in a MongoDB
            URI, they must not be percent-escaped when passed as parameters. In
            this example, both the space and slash special characters are passed
            as-is::

              MongoClient(username="user name", password="pass/word")

          - `authSource`: The database to authenticate on. Defaults to the
            database specified in the URI, if provided, or to "admin".
          - `authMechanism`: See :data:`~pymongo.auth.MECHANISMS` for options.
            If no mechanism is specified, PyMongo automatically SCRAM-SHA-1
            when connected to MongoDB 3.6 and negotiates the mechanism to use
            (SCRAM-SHA-1 or SCRAM-SHA-256) when connected to MongoDB 4.0+.
          - `authMechanismProperties`: Used to specify authentication mechanism
            specific options. To specify the service name for GSSAPI
            authentication pass authMechanismProperties='SERVICE_NAME:<service
            name>'.
            To specify the session token for MONGODB-AWS authentication pass
            ``authMechanismProperties='AWS_SESSION_TOKEN:<session token>'``.

          .. seealso:: :doc:`/examples/authentication`

          | **TLS/SSL configuration:**

          - `tls`: (boolean) If ``True``, create the connection to the server
            using transport layer security. Defaults to ``False``.
          - `tlsInsecure`: (boolean) Specify whether TLS constraints should be
            relaxed as much as possible. Setting ``tlsInsecure=True`` implies
            ``tlsAllowInvalidCertificates=True`` and
            ``tlsAllowInvalidHostnames=True``. Defaults to ``False``. Think
            very carefully before setting this to ``True`` as it dramatically
            reduces the security of TLS.
          - `tlsAllowInvalidCertificates`: (boolean) If ``True``, continues
            the TLS handshake regardless of the outcome of the certificate
            verification process. If this is ``False``, and a value is not
            provided for ``tlsCAFile``, PyMongo will attempt to load system
            provided CA certificates. If the python version in use does not
            support loading system CA certificates then the ``tlsCAFile``
            parameter must point to a file of CA certificates.
            ``tlsAllowInvalidCertificates=False`` implies ``tls=True``.
            Defaults to ``False``. Think very carefully before setting this
            to ``True`` as that could make your application vulnerable to
            on-path attackers.
          - `tlsAllowInvalidHostnames`: (boolean) If ``True``, disables TLS
            hostname verification. ``tlsAllowInvalidHostnames=False`` implies
            ``tls=True``. Defaults to ``False``. Think very carefully before
            setting this to ``True`` as that could make your application
            vulnerable to on-path attackers.
          - `tlsCAFile`: A file containing a single or a bundle of
            "certification authority" certificates, which are used to validate
            certificates passed from the other end of the connection.
            Implies ``tls=True``. Defaults to ``None``.
          - `tlsCertificateKeyFile`: A file containing the client certificate
            and private key. Implies ``tls=True``. Defaults to ``None``.
          - `tlsCRLFile`: A file containing a PEM or DER formatted
            certificate revocation list. Only supported by python 2.7.9+
            (pypy 2.5.1+). Implies ``tls=True``. Defaults to ``None``.
          - `tlsCertificateKeyFilePassword`: The password or passphrase for
            decrypting the private key in ``tlsCertificateKeyFile``. Only
            necessary if the private key is encrypted. Only supported by
            python 2.7.9+ (pypy 2.5.1+) and 3.3+. Defaults to ``None``.
          - `tlsDisableOCSPEndpointCheck`: (boolean) If ``True``, disables
            certificate revocation status checking via the OCSP responder
            specified on the server certificate.
            ``tlsDisableOCSPEndpointCheck=False`` implies ``tls=True``.
            Defaults to ``False``.
          - `ssl`: (boolean) Alias for ``tls``.

          | **Read Concern options:**
          | (If not set explicitly, this will use the server default)

          - `readConcernLevel`: (string) The read concern level specifies the
            level of isolation for read operations.  For example, a read
            operation using a read concern level of ``majority`` will only
            return data that has been written to a majority of nodes. If the
            level is left unspecified, the server default will be used.

          | **Client side encryption options:**
          | (If not set explicitly, client side encryption will not be enabled.)

          - `auto_encryption_opts`: A
            :class:`~pymongo.encryption_options.AutoEncryptionOpts` which
            configures this client to automatically encrypt collection commands
            and automatically decrypt results. See
            :ref:`automatic-client-side-encryption` for an example.
            If a :class:`MongoClient` is configured with
            ``auto_encryption_opts`` and a non-None ``maxPoolSize``, a
            separate internal ``MongoClient`` is created if any of the
            following are true:

              - A ``key_vault_client`` is not passed to
                :class:`~pymongo.encryption_options.AutoEncryptionOpts`
              - ``bypass_auto_encrpytion=False`` is passed to
                :class:`~pymongo.encryption_options.AutoEncryptionOpts`

          | **Stable API options:**
          | (If not set explicitly, Stable API will not be enabled.)

          - `server_api`: A
            :class:`~pymongo.server_api.ServerApi` which configures this
            client to use Stable API. See :ref:`versioned-api-ref` for
            details.

        .. seealso:: The MongoDB documentation on `connections <https://dochub.mongodb.org/core/connections>`_.

        .. versionchanged:: 4.0

             - Removed the fsync, unlock, is_locked, database_names, and
               close_cursor methods.
               See the :ref:`pymongo4-migration-guide`.
             - Removed the ``waitQueueMultiple`` and ``socketKeepAlive``
               keyword arguments.
             - The default for `uuidRepresentation` was changed from
               ``pythonLegacy`` to ``unspecified``.
             - Added the ``srvServiceName`` and ``maxConnecting`` URI and
               keyword argument.

        .. versionchanged:: 3.12
           Added the ``server_api`` keyword argument.
           The following keyword arguments were deprecated:

             - ``ssl_certfile`` and ``ssl_keyfile`` were deprecated in favor
               of ``tlsCertificateKeyFile``.

        .. versionchanged:: 3.11
           Added the following keyword arguments and URI options:

             - ``tlsDisableOCSPEndpointCheck``
             - ``directConnection``

        .. versionchanged:: 3.9
           Added the ``retryReads`` keyword argument and URI option.
           Added the ``tlsInsecure`` keyword argument and URI option.
           The following keyword arguments and URI options were deprecated:

             - ``wTimeout`` was deprecated in favor of ``wTimeoutMS``.
             - ``j`` was deprecated in favor of ``journal``.
             - ``ssl_cert_reqs`` was deprecated in favor of
               ``tlsAllowInvalidCertificates``.
             - ``ssl_match_hostname`` was deprecated in favor of
               ``tlsAllowInvalidHostnames``.
             - ``ssl_ca_certs`` was deprecated in favor of ``tlsCAFile``.
             - ``ssl_certfile`` was deprecated in favor of
               ``tlsCertificateKeyFile``.
             - ``ssl_crlfile`` was deprecated in favor of ``tlsCRLFile``.
             - ``ssl_pem_passphrase`` was deprecated in favor of
               ``tlsCertificateKeyFilePassword``.

        .. versionchanged:: 3.9
           ``retryWrites`` now defaults to ``True``.

        .. versionchanged:: 3.8
           Added the ``server_selector`` keyword argument.
           Added the ``type_registry`` keyword argument.

        .. versionchanged:: 3.7
           Added the ``driver`` keyword argument.

        .. versionchanged:: 3.6
           Added support for mongodb+srv:// URIs.
           Added the ``retryWrites`` keyword argument and URI option.

        .. versionchanged:: 3.5
           Add ``username`` and ``password`` options. Document the
           ``authSource``, ``authMechanism``, and ``authMechanismProperties``
           options.
           Deprecated the ``socketKeepAlive`` keyword argument and URI option.
           ``socketKeepAlive`` now defaults to ``True``.

        .. versionchanged:: 3.0
           :class:`~pymongo.mongo_client.MongoClient` is now the one and only
           client class for a standalone server, mongos, or replica set.
           It includes the functionality that had been split into
           :class:`~pymongo.mongo_client.MongoReplicaSetClient`: it can connect
           to a replica set, discover all its members, and monitor the set for
           stepdowns, elections, and reconfigs.

           The :class:`~pymongo.mongo_client.MongoClient` constructor no
           longer blocks while connecting to the server or servers, and it no
           longer raises :class:`~pymongo.errors.ConnectionFailure` if they
           are unavailable, nor :class:`~pymongo.errors.ConfigurationError`
           if the user's credentials are wrong. Instead, the constructor
           returns immediately and launches the connection process on
           background threads.

           Therefore the ``alive`` method is removed since it no longer
           provides meaningful information; even if the client is disconnected,
           it may discover a server in time to fulfill the next operation.

           In PyMongo 2.x, :class:`~pymongo.MongoClient` accepted a list of
           standalone MongoDB servers and used the first it could connect to::

               MongoClient(['host1.com:27017', 'host2.com:27017'])

           A list of multiple standalones is no longer supported; if multiple
           servers are listed they must be members of the same replica set, or
           mongoses in the same sharded cluster.

           The behavior for a list of mongoses is changed from "high
           availability" to "load balancing". Before, the client connected to
           the lowest-latency mongos in the list, and used it until a network
           error prompted it to re-evaluate all mongoses' latencies and
           reconnect to one of them. In PyMongo 3, the client monitors its
           network latency to all the mongoses continuously, and distributes
           operations evenly among those with the lowest latency. See
           :ref:`mongos-load-balancing` for more information.

           The ``connect`` option is added.

           The ``start_request``, ``in_request``, and ``end_request`` methods
           are removed, as well as the ``auto_start_request`` option.

           The ``copy_database`` method is removed, see the
           :doc:`copy_database examples </examples/copydb>` for alternatives.

           The :meth:`MongoClient.disconnect` method is removed; it was a
           synonym for :meth:`~pymongo.MongoClient.close`.

           :class:`~pymongo.mongo_client.MongoClient` no longer returns an
           instance of :class:`~pymongo.database.Database` for attribute names
           with leading underscores. You must use dict-style lookups instead::

               client['__my_database__']

           Not::

               client.__my_database__
        """
        doc_class = document_class or dict
        self.__init_kwargs: Dict[str, Any] = {
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
        self.__options = options = ClientOptions(username, password, dbase, opts)

        self.__default_database_name = dbase
        self.__lock = threading.Lock()
        self.__kill_cursors_queue: List = []

        self._event_listeners = options.pool_options._event_listeners
        super(MongoClient, self).__init__(
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
        )

        self._topology = Topology(self._topology_settings)

        def target():
            client = self_ref()
            if client is None:
                return False  # Stop the executor.
            MongoClient._process_periodic_tasks(client)
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

        if connect:
            self._get_topology()

        self._encrypter = None
        if self.__options.auto_encryption_opts:
            from pymongo.encryption import _Encrypter

            self._encrypter = _Encrypter(self, self.__options.auto_encryption_opts)

    def _duplicate(self, **kwargs):
        args = self.__init_kwargs.copy()
        args.update(kwargs)
        return MongoClient(**args)

    def _server_property(self, attr_name):
        """An attribute of the current server's description.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.

        Not threadsafe if used multiple times in a single method, since
        the server may change. In such cases, store a local reference to a
        ServerDescription first, then use its properties.
        """
        server = self._topology.select_server(writable_server_selector)

        return getattr(server.description, attr_name)

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
                with client.watch(
                        [{'$match': {'operationType': 'insert'}}]) as stream:
                    for insert_change in stream:
                        print(insert_change)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                logging.error('...')

        For a precise description of the resume process see the
        `change streams specification`_.

        :Parameters:
          - `pipeline` (optional): A list of aggregation pipeline stages to
            append to an initial ``$changeStream`` stage. Not all
            pipeline stages are valid after a ``$changeStream`` stage, see the
            MongoDB documentation on change streams for the supported stages.
          - `full_document` (optional): The fullDocument to pass as an option
            to the ``$changeStream`` stage. Allowed values: 'updateLookup'.
            When set to 'updateLookup', the change notification for partial
            updates will include both a delta describing the changes to the
            document, as well as a copy of the entire document that was
            changed from some time after the change occurred.
          - `resume_after` (optional): A resume token. If provided, the
            change stream will start returning changes that occur directly
            after the operation specified in the resume token. A resume token
            is the _id value of a change document.
          - `max_await_time_ms` (optional): The maximum time in milliseconds
            for the server to wait for changes before responding to a getMore
            operation.
          - `batch_size` (optional): The maximum number of documents to return
            per batch.
          - `collation` (optional): The :class:`~pymongo.collation.Collation`
            to use for the aggregation.
          - `start_at_operation_time` (optional): If provided, the resulting
            change stream will only return changes that occurred at or after
            the specified :class:`~bson.timestamp.Timestamp`. Requires
            MongoDB >= 4.0.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `start_after` (optional): The same as `resume_after` except that
            `start_after` can resume notifications after an invalidate event.
            This option and `resume_after` are mutually exclusive.
          - `comment` (optional): A user-provided comment to attach to this
            command.

        :Returns:
          A :class:`~pymongo.change_stream.ClusterChangeStream` cursor.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.9
           Added the ``start_after`` parameter.

        .. versionadded:: 3.7

        .. seealso:: The MongoDB documentation on `changeStreams <https://mongodb.com/docs/manual/changeStreams/>`_.

        .. _change streams specification:
            https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst
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
            comment=comment,
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

        :Returns:
          An instance of
          :class:`~pymongo.topology_description.TopologyDescription`.

        .. versionadded:: 4.0
        """
        return self._topology.description

    @property
    def address(self) -> Optional[Tuple[str, int]]:
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
        return self._server_property("address")

    @property
    def primary(self) -> Optional[Tuple[str, int]]:
        """The (host, port) of the current primary of the replica set.

        Returns ``None`` if this client is not connected to a replica set,
        there is no primary, or this client was created without the
        `replicaSet` option.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0.
        """
        return self._topology.get_primary()

    @property
    def secondaries(self) -> Set[Tuple[str, int]]:
        """The secondary members known to this client.

        A sequence of (host, port) pairs. Empty if this client is not
        connected to a replica set, there are no visible secondaries, or this
        client was created without the `replicaSet` option.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0.
        """
        return self._topology.get_secondaries()

    @property
    def arbiters(self) -> Set[Tuple[str, int]]:
        """Arbiters in the replica set.

        A sequence of (host, port) pairs. Empty if this client is not
        connected to a replica set, there are no arbiters, or this client was
        created without the `replicaSet` option.
        """
        return self._topology.get_arbiters()

    @property
    def is_primary(self) -> bool:
        """If this client is connected to a server that can accept writes.

        True if the current server is a standalone, mongos, or the primary of
        a replica set. If the client is not connected, this will block until a
        connection is established or raise ServerSelectionTimeoutError if no
        server is available.
        """
        return self._server_property("is_writable")

    @property
    def is_mongos(self) -> bool:
        """If this client is connected to mongos. If the client is not
        connected, this will block until a connection is established or raise
        ServerSelectionTimeoutError if no server is available..
        """
        return self._server_property("server_type") == SERVER_TYPE.Mongos

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

        :Returns:
          An instance of :class:`~pymongo.client_options.ClientOptions`.

        .. versionadded:: 4.0
        """
        return self.__options

    def _end_sessions(self, session_ids):
        """Send endSessions command(s) with the given session ids."""
        try:
            # Use SocketInfo.command directly to avoid implicitly creating
            # another session.
            with self._socket_for_reads(ReadPreference.PRIMARY_PREFERRED, None) as (
                sock_info,
                read_pref,
            ):
                if not sock_info.supports_sessions:
                    return

                for i in range(0, len(session_ids), common._MAX_END_SESSIONS):
                    spec = SON([("endSessions", session_ids[i : i + common._MAX_END_SESSIONS])])
                    sock_info.command("admin", spec, read_preference=read_pref, client=self)
        except PyMongoError:
            # Drivers MUST ignore any errors returned by the endSessions
            # command.
            pass

    def close(self) -> None:
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
        session_ids = self._topology.pop_all_sessions()
        if session_ids:
            self._end_sessions(session_ids)
        # Stop the periodic task thread and then send pending killCursor
        # requests before closing the topology.
        self._kill_cursors_executor.close()
        self._process_kill_cursors()
        self._topology.close()
        if self._encrypter:
            # TODO: PYTHON-1921 Encrypted MongoClients cannot be re-opened.
            self._encrypter.close()

    def _get_topology(self):
        """Get the internal :class:`~pymongo.topology.Topology` object.

        If this client was created with "connect=False", calling _get_topology
        launches the connection process in the background.
        """
        self._topology.open()
        with self.__lock:
            self._kill_cursors_executor.open()
        return self._topology

    @contextlib.contextmanager
    def _get_socket(self, server, session):
        in_txn = session and session.in_transaction
        with _MongoClientErrorHandler(self, server, session) as err_handler:
            # Reuse the pinned connection, if it exists.
            if in_txn and session._pinned_connection:
                yield session._pinned_connection
                return
            with server.get_socket(handler=err_handler) as sock_info:
                # Pin this session to the selected server or connection.
                if in_txn and server.description.server_type in (
                    SERVER_TYPE.Mongos,
                    SERVER_TYPE.LoadBalancer,
                ):
                    session._pin(server, sock_info)
                err_handler.contribute_socket(sock_info)
                if (
                    self._encrypter
                    and not self._encrypter._bypass_auto_encryption
                    and sock_info.max_wire_version < 8
                ):
                    raise ConfigurationError(
                        "Auto-encryption requires a minimum MongoDB version of 4.2"
                    )
                yield sock_info

    def _select_server(self, server_selector, session, address=None):
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
            topology = self._get_topology()
            if session and not session.in_transaction:
                session._transaction.reset()
            address = address or (session and session._pinned_address)
            if address:
                # We're running a getMore or this session is pinned to a mongos.
                server = topology.select_server_by_address(address)
                if not server:
                    raise AutoReconnect("server %s:%d no longer available" % address)
            else:
                server = topology.select_server(server_selector)
            return server
        except PyMongoError as exc:
            # Server selection errors in a transaction are transient.
            if session and session.in_transaction:
                exc._add_error_label("TransientTransactionError")
                session._unpin()
            raise

    def _socket_for_writes(self, session):
        server = self._select_server(writable_server_selector, session)
        return self._get_socket(server, session)

    @contextlib.contextmanager
    def _socket_from_server(self, read_preference, server, session):
        assert read_preference is not None, "read_preference must not be None"
        # Get a socket for a server matching the read preference, and yield
        # sock_info with the effective read preference. The Server Selection
        # Spec says not to send any $readPreference to standalones and to
        # always send primaryPreferred when directly connected to a repl set
        # member.
        # Thread safe: if the type is single it cannot change.
        topology = self._get_topology()
        single = topology.description.topology_type == TOPOLOGY_TYPE.Single

        with self._get_socket(server, session) as sock_info:
            if single:
                if sock_info.is_repl:
                    # Use primary preferred to ensure any repl set member
                    # can handle the request.
                    read_preference = ReadPreference.PRIMARY_PREFERRED
                elif sock_info.is_standalone:
                    # Don't send read preference to standalones.
                    read_preference = ReadPreference.PRIMARY
            yield sock_info, read_preference

    def _socket_for_reads(self, read_preference, session):
        assert read_preference is not None, "read_preference must not be None"
        _ = self._get_topology()
        server = self._select_server(read_preference, session)
        return self._socket_from_server(read_preference, server, session)

    def _should_pin_cursor(self, session):
        return self.__options.load_balanced and not (session and session.in_transaction)

    def _run_operation(self, operation, unpack_res, address=None):
        """Run a _Query/_GetMore operation and return a Response.

        :Parameters:
          - `operation`: a _Query or _GetMore object.
          - `unpack_res`: A callable that decodes the wire protocol response.
          - `address` (optional): Optional address when sending a message
            to a specific server, used for getMore.
        """
        if operation.sock_mgr:
            server = self._select_server(
                operation.read_preference, operation.session, address=address
            )

            with operation.sock_mgr.lock:
                with _MongoClientErrorHandler(self, server, operation.session) as err_handler:
                    err_handler.contribute_socket(operation.sock_mgr.sock)
                    return server.run_operation(
                        operation.sock_mgr.sock, operation, True, self._event_listeners, unpack_res
                    )

        def _cmd(session, server, sock_info, read_preference):
            return server.run_operation(
                sock_info, operation, read_preference, self._event_listeners, unpack_res
            )

        return self._retryable_read(
            _cmd,
            operation.read_preference,
            operation.session,
            address=address,
            retryable=isinstance(operation, message._Query),
        )

    def _retry_with_session(self, retryable, func, session, bulk):
        """Execute an operation with at most one consecutive retries

        Returns func()'s return value on success. On error retries the same
        command once.

        Re-raises any exception thrown by func().
        """
        retryable = (
            retryable and self.options.retry_writes and session and not session.in_transaction
        )
        return self._retry_internal(retryable, func, session, bulk)

    def _retry_internal(self, retryable, func, session, bulk):
        """Internal retryable write helper."""
        max_wire_version = 0
        last_error: Optional[Exception] = None
        retrying = False

        def is_retrying():
            return bulk.retrying if bulk else retrying

        # Increment the transaction id up front to ensure any retry attempt
        # will use the proper txnNumber, even if server or socket selection
        # fails before the command can be sent.
        if retryable and session and not session.in_transaction:
            session._start_retryable_write()
            if bulk:
                bulk.started_retryable_write = True

        while True:
            try:
                server = self._select_server(writable_server_selector, session)
                supports_session = (
                    session is not None and server.description.retryable_writes_supported
                )
                with self._get_socket(server, session) as sock_info:
                    max_wire_version = sock_info.max_wire_version
                    if retryable and not supports_session:
                        if is_retrying():
                            # A retry is not possible because this server does
                            # not support sessions raise the last error.
                            assert last_error is not None
                            raise last_error
                        retryable = False
                    return func(session, sock_info, retryable)
            except ServerSelectionTimeoutError:
                if is_retrying():
                    # The application may think the write was never attempted
                    # if we raise ServerSelectionTimeoutError on the retry
                    # attempt. Raise the original exception instead.
                    assert last_error is not None
                    raise last_error
                # A ServerSelectionTimeoutError error indicates that there may
                # be a persistent outage. Attempting to retry in this case will
                # most likely be a waste of time.
                raise
            except PyMongoError as exc:
                if not retryable:
                    raise
                # Add the RetryableWriteError label, if applicable.
                _add_retryable_write_error(exc, max_wire_version)
                retryable_error = exc.has_error_label("RetryableWriteError")
                if retryable_error:
                    session._unpin()
                if is_retrying() or not retryable_error:
                    raise
                if bulk:
                    bulk.retrying = True
                else:
                    retrying = True
                last_error = exc

    def _retryable_read(self, func, read_pref, session, address=None, retryable=True):
        """Execute an operation with at most one consecutive retries

        Returns func()'s return value on success. On error retries the same
        command once.

        Re-raises any exception thrown by func().
        """
        retryable = (
            retryable and self.options.retry_reads and not (session and session.in_transaction)
        )
        last_error: Optional[Exception] = None
        retrying = False

        while True:
            try:
                server = self._select_server(read_pref, session, address=address)
                with self._socket_from_server(read_pref, server, session) as (sock_info, read_pref):
                    if retrying and not retryable:
                        # A retry is not possible because this server does
                        # not support retryable reads, raise the last error.
                        assert last_error is not None
                        raise last_error
                    return func(session, server, sock_info, read_pref)
            except ServerSelectionTimeoutError:
                if retrying:
                    # The application may think the write was never attempted
                    # if we raise ServerSelectionTimeoutError on the retry
                    # attempt. Raise the original exception instead.
                    assert last_error is not None
                    raise last_error
                # A ServerSelectionTimeoutError error indicates that there may
                # be a persistent outage. Attempting to retry in this case will
                # most likely be a waste of time.
                raise
            except ConnectionFailure as exc:
                if not retryable or retrying:
                    raise
                retrying = True
                last_error = exc
            except OperationFailure as exc:
                if not retryable or retrying:
                    raise
                if exc.code not in helpers._RETRYABLE_ERROR_CODES:
                    raise
                retrying = True
                last_error = exc

    def _retryable_write(self, retryable, func, session):
        """Internal retryable write helper."""
        with self._tmp_session(session) as s:
            return self._retry_with_session(retryable, func, s, None)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self._topology == other._topology
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self._topology)

    def _repr_helper(self):
        def option_repr(option, value):
            """Fix options whose __repr__ isn't usable in a constructor."""
            if option == "document_class":
                if value is dict:
                    return "document_class=dict"
                else:
                    return "document_class=%s.%s" % (value.__module__, value.__name__)
            if option in common.TIMEOUT_OPTIONS and value is not None:
                return "%s=%s" % (option, int(value * 1000))

            return "%s=%r" % (option, value)

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
            option_repr(key, self.__options._options[key]) for key in self._constructor_args
        )
        # ... then everything else.
        options.extend(
            option_repr(key, self.__options._options[key])
            for key in self.__options._options
            if key not in set(self._constructor_args) and key != "username" and key != "password"
        )
        return ", ".join(options)

    def __repr__(self):
        return "MongoClient(%s)" % (self._repr_helper(),)

    def __getattr__(self, name: str) -> database.Database[_DocumentType]:
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        if name.startswith("_"):
            raise AttributeError(
                "MongoClient has no attribute %r. To access the %s"
                " database, use client[%r]." % (name, name, name)
            )
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> database.Database[_DocumentType]:
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return database.Database(self, name)

    def _cleanup_cursor(
        self, locks_allowed, cursor_id, address, sock_mgr, session, explicit_session
    ):
        """Cleanup a cursor from cursor.close() or __del__.

        This method handles cleanup for Cursors/CommandCursors including any
        pinned connection or implicit session attached at the time the cursor
        was closed or garbage collected.

        :Parameters:
          - `locks_allowed`: True if we are allowed to acquire locks.
          - `cursor_id`: The cursor id which may be 0.
          - `address`: The _CursorAddress.
          - `sock_mgr`: The _SocketManager for the pinned connection or None.
          - `session`: The cursor's session.
          - `explicit_session`: True if the session was passed explicitly.
        """
        if locks_allowed:
            if cursor_id:
                if sock_mgr and sock_mgr.more_to_come:
                    # If this is an exhaust cursor and we haven't completely
                    # exhausted the result set we *must* close the socket
                    # to stop the server from sending more data.
                    sock_mgr.sock.close_socket(ConnectionClosedReason.ERROR)
                else:
                    self._close_cursor_now(cursor_id, address, session=session, sock_mgr=sock_mgr)
            if sock_mgr:
                sock_mgr.close()
        else:
            # The cursor will be closed later in a different session.
            if cursor_id or sock_mgr:
                self._close_cursor_soon(cursor_id, address, sock_mgr)
        if session and not explicit_session:
            session._end_session(lock=locks_allowed)

    def _close_cursor_soon(self, cursor_id, address, sock_mgr=None):
        """Request that a cursor and/or connection be cleaned up soon."""
        self.__kill_cursors_queue.append((address, cursor_id, sock_mgr))

    def _close_cursor_now(self, cursor_id, address=None, session=None, sock_mgr=None):
        """Send a kill cursors message with the given id.

        The cursor is closed synchronously on the current thread.
        """
        if not isinstance(cursor_id, int):
            raise TypeError("cursor_id must be an instance of int")

        try:
            if sock_mgr:
                with sock_mgr.lock:
                    # Cursor is pinned to LB outside of a transaction.
                    self._kill_cursor_impl([cursor_id], address, session, sock_mgr.sock)
            else:
                self._kill_cursors([cursor_id], address, self._get_topology(), session)
        except PyMongoError:
            # Make another attempt to kill the cursor later.
            self._close_cursor_soon(cursor_id, address)

    def _kill_cursors(self, cursor_ids, address, topology, session):
        """Send a kill cursors message with the given ids."""
        if address:
            # address could be a tuple or _CursorAddress, but
            # select_server_by_address needs (host, port).
            server = topology.select_server_by_address(tuple(address))
        else:
            # Application called close_cursor() with no address.
            server = topology.select_server(writable_server_selector)

        with self._get_socket(server, session) as sock_info:
            self._kill_cursor_impl(cursor_ids, address, session, sock_info)

    def _kill_cursor_impl(self, cursor_ids, address, session, sock_info):
        namespace = address.namespace
        db, coll = namespace.split(".", 1)
        spec = SON([("killCursors", coll), ("cursors", cursor_ids)])
        sock_info.command(db, spec, session=session, client=self)

    def _process_kill_cursors(self):
        """Process any pending kill cursors requests."""
        address_to_cursor_ids = defaultdict(list)
        pinned_cursors = []

        # Other threads or the GC may append to the queue concurrently.
        while True:
            try:
                address, cursor_id, sock_mgr = self.__kill_cursors_queue.pop()
            except IndexError:
                break

            if sock_mgr:
                pinned_cursors.append((address, cursor_id, sock_mgr))
            else:
                address_to_cursor_ids[address].append(cursor_id)

        for address, cursor_id, sock_mgr in pinned_cursors:
            try:
                self._cleanup_cursor(True, cursor_id, address, sock_mgr, None, False)
            except Exception as exc:
                if isinstance(exc, InvalidOperation) and self._topology._closed:
                    # Raise the exception when client is closed so that it
                    # can be caught in _process_periodic_tasks
                    raise
                else:
                    helpers._handle_exception()

        # Don't re-open topology if it's closed and there's no pending cursors.
        if address_to_cursor_ids:
            topology = self._get_topology()
            for address, cursor_ids in address_to_cursor_ids.items():
                try:
                    self._kill_cursors(cursor_ids, address, topology, session=None)
                except Exception as exc:
                    if isinstance(exc, InvalidOperation) and self._topology._closed:
                        raise
                    else:
                        helpers._handle_exception()

    # This method is run periodically by a background thread.
    def _process_periodic_tasks(self):
        """Process any pending kill cursors requests and
        maintain connection pool parameters."""
        try:
            self._process_kill_cursors()
            self._topology.update_pool()
        except Exception as exc:
            if isinstance(exc, InvalidOperation) and self._topology._closed:
                return
            else:
                helpers._handle_exception()

    def __start_session(self, implicit, **kwargs):
        # Raises ConfigurationError if sessions are not supported.
        if implicit:
            self._topology._check_implicit_session_support()
            server_session = _EmptyServerSession()
        else:
            server_session = self._get_server_session()
        opts = client_session.SessionOptions(**kwargs)
        return client_session.ClientSession(self, server_session, opts, implicit)

    def start_session(
        self,
        causal_consistency: Optional[bool] = None,
        default_transaction_options: Optional[client_session.TransactionOptions] = None,
        snapshot: Optional[bool] = False,
    ) -> client_session.ClientSession[_DocumentType]:
        """Start a logical session.

        This method takes the same parameters as
        :class:`~pymongo.client_session.SessionOptions`. See the
        :mod:`~pymongo.client_session` module for details and examples.

        A :class:`~pymongo.client_session.ClientSession` may only be used with
        the MongoClient that started it. :class:`ClientSession` instances are
        **not thread-safe or fork-safe**. They can only be used by one thread
        or process at a time. A single :class:`ClientSession` cannot be used
        to run multiple operations concurrently.

        :Returns:
          An instance of :class:`~pymongo.client_session.ClientSession`.

        .. versionadded:: 3.6
        """
        return self.__start_session(
            False,
            causal_consistency=causal_consistency,
            default_transaction_options=default_transaction_options,
            snapshot=snapshot,
        )

    def _get_server_session(self):
        """Internal: start or resume a _ServerSession."""
        return self._topology.get_server_session()

    def _return_server_session(self, server_session, lock):
        """Internal: return a _ServerSession to the pool."""
        if isinstance(server_session, _EmptyServerSession):
            return
        return self._topology.return_server_session(server_session, lock)

    def _ensure_session(self, session=None):
        """If provided session is None, lend a temporary session."""
        if session:
            return session

        try:
            # Don't make implicit sessions causally consistent. Applications
            # should always opt-in.
            return self.__start_session(True, causal_consistency=False)
        except (ConfigurationError, InvalidOperation):
            # Sessions not supported.
            return None

    @contextlib.contextmanager
    def _tmp_session(self, session, close=True):
        """If provided session is None, lend a temporary session."""
        if session:
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
                s.end_session()
                raise
            finally:
                # Call end_session when we exit this scope.
                if close:
                    s.end_session()
        else:
            yield None

    def _send_cluster_time(self, command, session):
        topology_time = self._topology.max_cluster_time()
        session_time = session.cluster_time if session else None
        if topology_time and session_time:
            if topology_time["clusterTime"] > session_time["clusterTime"]:
                cluster_time = topology_time
            else:
                cluster_time = session_time
        else:
            cluster_time = topology_time or session_time
        if cluster_time:
            command["$clusterTime"] = cluster_time

    def _process_response(self, reply, session):
        self._topology.receive_cluster_time(reply.get("$clusterTime"))
        if session is not None:
            session._process_response(reply)

    def server_info(self, session: Optional[client_session.ClientSession] = None) -> Dict[str, Any]:
        """Get information about the MongoDB server we're connected to.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        return cast(
            dict,
            self.admin.command(
                "buildinfo", read_preference=ReadPreference.PRIMARY, session=session
            ),
        )

    def list_databases(
        self,
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[Dict[str, Any]]:
        """Get a cursor over the databases of the connected server.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
            command.
          - `**kwargs` (optional): Optional parameters of the
            `listDatabases command
            <https://mongodb.com/docs/manual/reference/command/listDatabases/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.


        :Returns:
          An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionadded:: 3.6
        """
        cmd = SON([("listDatabases", 1)])
        cmd.update(kwargs)
        if comment is not None:
            cmd["comment"] = comment
        admin = self._database_default_options("admin")
        res = admin._retryable_read_command(cmd, session=session)
        # listDatabases doesn't return a cursor (yet). Fake one.
        cursor = {
            "id": 0,
            "firstBatch": res["databases"],
            "ns": "admin.$cmd",
        }
        return CommandCursor(admin["$cmd"], cursor, None, comment=comment)

    def list_database_names(
        self,
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> List[str]:
        """Get a list of the names of all databases on the connected server.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
            command.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionadded:: 3.6
        """
        return [doc["name"] for doc in self.list_databases(session, nameOnly=True, comment=comment)]

    def drop_database(
        self,
        name_or_database: Union[str, database.Database],
        session: Optional[client_session.ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> None:
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        :class:`basestring` (:class:`str` in python 3) or
        :class:`~pymongo.database.Database`.

        :Parameters:
          - `name_or_database`: the name of a database to drop, or a
            :class:`~pymongo.database.Database` instance representing the
            database to drop
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
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

        with self._socket_for_writes(session) as sock_info:
            self[name]._command(
                sock_info,
                {"dropDatabase": 1, "comment": comment},
                read_preference=ReadPreference.PRIMARY,
                write_concern=self._write_concern_for(session),
                parse_write_concern_error=True,
                session=session,
            )

    def get_default_database(
        self,
        default: Optional[str] = None,
        codec_options: Optional[CodecOptions] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional["ReadConcern"] = None,
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

        :Parameters:
          - `default` (optional): the database name to use if no database name
            was provided in the URI.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`MongoClient` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`MongoClient` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`MongoClient` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`MongoClient` is
            used.
          - `comment` (optional): A user-provided comment to attach to this
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
        if self.__default_database_name is None and default is None:
            raise ConfigurationError("No default database name defined or provided.")

        name = cast(str, self.__default_database_name or default)
        return database.Database(
            self, name, codec_options, read_preference, write_concern, read_concern
        )

    def get_database(
        self,
        name: Optional[str] = None,
        codec_options: Optional[CodecOptions] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional["ReadConcern"] = None,
    ) -> database.Database[_DocumentType]:
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

        :Parameters:
          - `name` (optional): The name of the database - a string. If ``None``
            (the default) the database named in the MongoDB connection URI is
            returned.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`MongoClient` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`MongoClient` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`MongoClient` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`MongoClient` is
            used.

        .. versionchanged:: 3.5
           The `name` parameter is now optional, defaulting to the database
           named in the MongoDB connection URI.
        """
        if name is None:
            if self.__default_database_name is None:
                raise ConfigurationError("No default database defined")
            name = self.__default_database_name

        return database.Database(
            self, name, codec_options, read_preference, write_concern, read_concern
        )

    def _database_default_options(self, name):
        """Get a Database instance with the default settings."""
        return self.get_database(
            name,
            codec_options=DEFAULT_CODEC_OPTIONS,
            read_preference=ReadPreference.PRIMARY,
            write_concern=DEFAULT_WRITE_CONCERN,
        )

    def __enter__(self) -> "MongoClient[_DocumentType]":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # See PYTHON-3084.
    __iter__ = None

    def __next__(self) -> NoReturn:
        raise TypeError("'MongoClient' object is not iterable")

    next = __next__


def _retryable_error_doc(exc):
    """Return the server response from PyMongo exception or None."""
    if isinstance(exc, BulkWriteError):
        # Check the last writeConcernError to determine if this
        # BulkWriteError is retryable.
        wces = exc.details["writeConcernErrors"]
        wce = wces[-1] if wces else None
        return wce
    if isinstance(exc, (NotPrimaryError, OperationFailure)):
        return exc.details
    return None


def _add_retryable_write_error(exc, max_wire_version):
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
            raise OperationFailure(errmsg, code, exc.details)
        if max_wire_version >= 9:
            # In MongoDB 4.4+, the server reports the error labels.
            for label in doc.get("errorLabels", []):
                exc._add_error_label(label)
        else:
            if code in helpers._RETRYABLE_ERROR_CODES:
                exc._add_error_label("RetryableWriteError")

    # Connection errors are always retryable except NotPrimaryError which is
    # handled above.
    if isinstance(exc, ConnectionFailure) and not isinstance(exc, NotPrimaryError):
        exc._add_error_label("RetryableWriteError")


class _MongoClientErrorHandler(object):
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

    def __init__(self, client, server, session):
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
        self.service_id = None
        self.handled = False

    def contribute_socket(self, sock_info):
        """Provide socket information to the error handler."""
        self.max_wire_version = sock_info.max_wire_version
        self.sock_generation = sock_info.generation
        self.service_id = sock_info.service_id
        self.completed_handshake = True

    def handle(self, exc_type, exc_val):
        if self.handled or exc_type is None:
            return
        self.handled = True
        if self.session:
            if issubclass(exc_type, ConnectionFailure):
                if self.session.in_transaction:
                    exc_val._add_error_label("TransientTransactionError")
                self.session._server_session.mark_dirty()

            if issubclass(exc_type, PyMongoError):
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
        self.client._topology.handle_error(self.server_address, err_ctx)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.handle(exc_type, exc_val)
