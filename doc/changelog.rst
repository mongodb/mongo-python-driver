Changelog
=========

Changes in Version 3.0.3
------------------------

Version 3.0.3 fixes issues reported since the release of 3.0.2, including a
feature breaking bug in the GSSAPI implementation.

Issues Resolved
...............

See the `PyMongo 3.0.3 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 3.0.3 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/15528

Changes in Version 3.0.2
------------------------

Version 3.0.2 fixes issues reported since the release of 3.0.1, most
importantly a bug that could route operations to replica set members
that are not in primary or secondary state when using
:class:`~pymongo.read_preferences.PrimaryPreferred` or
:class:`~pymongo.read_preferences.Nearest`. It is a recommended upgrade for
all users of PyMongo 3.0.x.

Issues Resolved
...............

See the `PyMongo 3.0.2 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 3.0.2 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/15430

Changes in Version 3.0.1
------------------------

Version 3.0.1 fixes issues reported since the release of 3.0, most
importantly a bug in GridFS.delete that could prevent file chunks from
actually being deleted.

Issues Resolved
...............

See the `PyMongo 3.0.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 3.0.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/15322

Changes in Version 3.0
----------------------

PyMongo 3.0 is a partial rewrite of PyMongo bringing a large number of
improvements:

- A unified client class. MongoClient is the one and only client class for
  connecting to a standalone mongod, replica set, or sharded cluster. Migrating
  from a standalone, to a replica set, to a sharded cluster can be accomplished
  with only a simple URI change.
- MongoClient is much more responsive to configuration changes in your MongoDB
  deployment. All connected servers are monitored in a non-blocking manner.
  Slow to respond or down servers no longer block server discovery, reducing
  application startup time and time to respond to new or reconfigured
  servers and replica set failovers.
- A unified CRUD API. All official MongoDB drivers now implement a standard
  CRUD API allowing polyglot developers to move from language to language
  with ease.
- Single source support for Python 2.x and 3.x. PyMongo no longer relies on
  2to3 to support Python 3.
- A rewritten pure Python BSON implementation, improving performance
  with pypy and cpython deployments without support for C extensions.
- Better support for greenlet based async frameworks including eventlet.
- Immutable client, database, and collection classes, avoiding a host of thread
  safety issues in client applications.

PyMongo 3.0 brings a large number of API changes. Be sure to read the changes
listed below before upgrading from PyMongo 2.x.

.. warning:: PyMongo no longer supports Python 2.4, 2.5, or 3.1. If you
  must use PyMongo with these versions of Python the 2.x branch of PyMongo
  will be minimally supported for some time.

SONManipulator changes
......................

The :class:`~pymongo.son_manipulator.SONManipulator` API has limitations as a
technique for transforming your data. Instead, it is more flexible and
straightforward to transform outgoing documents in your own code before passing
them to PyMongo, and transform incoming documents after receiving them from
PyMongo.

Thus the :meth:`~pymongo.database.Database.add_son_manipulator` method is
deprecated. PyMongo 3's new CRUD API does **not** apply SON manipulators to
documents passed to :meth:`~pymongo.collection.Collection.bulk_write`,
:meth:`~pymongo.collection.Collection.insert_one`,
:meth:`~pymongo.collection.Collection.insert_many`,
:meth:`~pymongo.collection.Collection.update_one`, or
:meth:`~pymongo.collection.Collection.update_many`. SON manipulators are **not**
applied to documents returned by the new methods
:meth:`~pymongo.collection.Collection.find_one_and_delete`,
:meth:`~pymongo.collection.Collection.find_one_and_replace`, and
:meth:`~pymongo.collection.Collection.find_one_and_update`.

SSL/TLS changes
...............

When `ssl` is ``True`` the `ssl_cert_reqs` option now defaults to
:attr:`ssl.CERT_REQUIRED` if not provided. PyMongo will attempt to load OS
provided CA certificates to verify the server, raising
:exc:`~pymongo.errors.ConfigurationError` if it cannot.

Gevent Support
..............

In previous versions, PyMongo supported Gevent in two modes: you could call
``gevent.monkey.patch_socket()`` and pass ``use_greenlets=True`` to
:class:`~pymongo.mongo_client.MongoClient`, or you could simply call
``gevent.monkey.patch_all()`` and omit the ``use_greenlets`` argument.

In PyMongo 3.0, the ``use_greenlets`` option is gone. To use PyMongo with
Gevent simply call ``gevent.monkey.patch_all()``.

For more information,
see :doc:`PyMongo's Gevent documentation <examples/gevent>`.

:class:`~pymongo.mongo_client.MongoClient` changes
..................................................

:class:`~pymongo.mongo_client.MongoClient` is now the one and only
client class for a standalone server, mongos, or replica set.
It includes the functionality that had been split into
``MongoReplicaSetClient``: it can connect to a replica set, discover all its
members, and monitor the set for stepdowns, elections, and reconfigs.
:class:`~pymongo.mongo_client.MongoClient` now also supports the full
:class:`~pymongo.read_preferences.ReadPreference` API.

The obsolete classes ``MasterSlaveConnection``, ``Connection``, and
``ReplicaSetConnection`` are removed.

The :class:`~pymongo.mongo_client.MongoClient` constructor no
longer blocks while connecting to the server or servers, and it no
longer raises :class:`~pymongo.errors.ConnectionFailure` if they
are unavailable, nor :class:`~pymongo.errors.ConfigurationError`
if the user's credentials are wrong. Instead, the constructor
returns immediately and launches the connection process on
background threads. The ``connect`` option is added to control whether
these threads are started immediately, or when the client is first used.

Therefore the ``alive`` method is removed since it no longer provides meaningful
information; even if the client is disconnected, it may discover a server in
time to fulfill the next operation.

In PyMongo 2.x, :class:`~pymongo.mongo_client.MongoClient` accepted a list of
standalone MongoDB servers and used the first it could connect to::

    MongoClient(['host1.com:27017', 'host2.com:27017'])

A list of multiple standalones is no longer supported; if multiple servers
are listed they must be members of the same replica set, or mongoses in the
same sharded cluster.

The behavior for a list of mongoses is changed from "high availability" to
"load balancing". Before, the client connected to the lowest-latency mongos in
the list, and used it until a network error prompted it to re-evaluate all
mongoses' latencies and reconnect to one of them. In PyMongo 3, the client
monitors its network latency to all the mongoses continuously, and distributes
operations evenly among those with the lowest latency.
See :ref:`mongos-load-balancing` for more information.

The client methods ``start_request``, ``in_request``, and ``end_request``
are removed, and so is the ``auto_start_request`` option. Requests were
designed to make read-your-writes consistency more likely with the ``w=0``
write concern. Additionally, a thread in a request used the same member for
all secondary reads in a replica set. To ensure read-your-writes consistency
in PyMongo 3.0, do not override the default write concern with ``w=0``, and
do not override the default :ref:`read preference <secondary-reads>` of
PRIMARY.

Support for the ``slaveOk`` (or ``slave_okay``), ``safe``, and
``network_timeout`` options has been removed. Use
:attr:`~pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED` instead of
slave_okay. Accept the default write concern, acknowledged writes, instead of
setting safe=True. Use socketTimeoutMS in place of network_timeout (note that
network_timeout was in seconds, where as socketTimeoutMS is milliseconds).

The ``max_pool_size`` option has been removed. It is replaced by the
``maxPoolSize`` MongoDB URI option. ``maxPoolSize`` is now a supported URI
option in PyMongo and can be passed as a keyword argument.

The ``copy_database`` method is removed, see the
:doc:`copy_database examples </examples/copydb>` for alternatives.

The ``disconnect`` method is removed. Use
:meth:`~pymongo.mongo_client.MongoClient.close` instead.

The ``get_document_class`` method is removed. Use
:attr:`~pymongo.mongo_client.MongoClient.codec_options` instead.

The ``get_lasterror_options``, ``set_lasterror_options``, and
``unset_lasterror_options`` methods are removed. Write concern options
can be passed to :class:`~pymongo.mongo_client.MongoClient` as keyword
arguments or MongoDB URI options.

The :meth:`~pymongo.mongo_client.MongoClient.get_database` method is added for
getting a Database instance with its options configured differently than the
MongoClient's.

The following read-only attributes have been added:

- :attr:`~pymongo.mongo_client.MongoClient.codec_options`

The following attributes are now read-only:

- :attr:`~pymongo.mongo_client.MongoClient.read_preference`
- :attr:`~pymongo.mongo_client.MongoClient.write_concern`

The following attributes have been removed:

- :attr:`~pymongo.mongo_client.MongoClient.document_class`
  (use :attr:`~pymongo.mongo_client.MongoClient.codec_options` instead)
- :attr:`~pymongo.mongo_client.MongoClient.host`
  (use :attr:`~pymongo.mongo_client.MongoClient.address` instead)
- :attr:`~pymongo.mongo_client.MongoClient.min_wire_version`
- :attr:`~pymongo.mongo_client.MongoClient.max_wire_version`
- :attr:`~pymongo.mongo_client.MongoClient.port`
  (use :attr:`~pymongo.mongo_client.MongoClient.address` instead)
- :attr:`~pymongo.mongo_client.MongoClient.safe`
  (use :attr:`~pymongo.mongo_client.MongoClient.write_concern` instead)
- :attr:`~pymongo.mongo_client.MongoClient.slave_okay`
  (use :attr:`~pymongo.mongo_client.MongoClient.read_preference` instead)
- :attr:`~pymongo.mongo_client.MongoClient.tag_sets`
  (use :attr:`~pymongo.mongo_client.MongoClient.read_preference` instead)
- :attr:`~pymongo.mongo_client.MongoClient.tz_aware`
  (use :attr:`~pymongo.mongo_client.MongoClient.codec_options` instead)

The following attributes have been renamed:

- :attr:`~pymongo.mongo_client.MongoClient.secondary_acceptable_latency_ms` is
  now :attr:`~pymongo.mongo_client.MongoClient.local_threshold_ms` and is now
  read-only.

:class:`~pymongo.cursor.Cursor` changes
.......................................

The ``conn_id`` property is renamed to :attr:`~pymongo.cursor.Cursor.address`.

Cursor management changes
.........................

:class:`~pymongo.cursor_manager.CursorManager` and
:meth:`~pymongo.mongo_client.MongoClient.set_cursor_manager` are no longer
deprecated. If you subclass :class:`~pymongo.cursor_manager.CursorManager`
your implementation of :meth:`~pymongo.cursor_manager.CursorManager.close`
must now take a second parameter, `address`. The ``BatchCursorManager`` class
is removed.

The second parameter to :meth:`~pymongo.mongo_client.MongoClient.close_cursor`
is renamed from ``_conn_id`` to ``address``.
:meth:`~pymongo.mongo_client.MongoClient.kill_cursors` now accepts an `address`
parameter.

:class:`~pymongo.database.Database` changes
...........................................

The ``connection`` property is renamed to
:attr:`~pymongo.database.Database.client`.

The following read-only attributes have been added:

- :attr:`~pymongo.database.Database.codec_options`

The following attributes are now read-only:

- :attr:`~pymongo.database.Database.read_preference`
- :attr:`~pymongo.database.Database.write_concern`

Use :meth:`~pymongo.mongo_client.MongoClient.get_database` for getting a
Database instance with its options configured differently than the
MongoClient's.

The following attributes have been removed:

- :attr:`~pymongo.database.Database.safe`
- :attr:`~pymongo.database.Database.secondary_acceptable_latency_ms`
- :attr:`~pymongo.database.Database.slave_okay`
- :attr:`~pymongo.database.Database.tag_sets`

The following methods have been added:

- :meth:`~pymongo.database.Database.get_collection`

The following methods have been changed:

- :meth:`~pymongo.database.Database.command`. Support for `as_class`,
  `uuid_subtype`, `tag_sets`, and `secondary_acceptable_latency_ms` have been
  removed. You can instead pass an instance of
  :class:`~bson.codec_options.CodecOptions` as `codec_options` and an instance
  of a read preference class from :mod:`~pymongo.read_preferences` as
  `read_preference`. The `fields` and `compile_re` options are also removed.
  The `fields` options was undocumented and never really worked. Regular
  expressions are always decoded to :class:`~bson.regex.Regex`.

The following methods have been deprecated:

- :meth:`~pymongo.database.Database.add_son_manipulator`

The following methods have been removed:

The ``get_lasterror_options``, ``set_lasterror_options``, and
``unset_lasterror_options`` methods have been removed. Use
:class:`~pymongo.write_concern.WriteConcern` with
:meth:`~pymongo.mongo_client.MongoClient.get_database` instead.

:class:`~pymongo.collection.Collection` changes
...............................................

The following read-only attributes have been added:

- :attr:`~pymongo.collection.Collection.codec_options`

The following attributes are now read-only:

- :attr:`~pymongo.collection.Collection.read_preference`
- :attr:`~pymongo.collection.Collection.write_concern`

Use :meth:`~pymongo.database.Database.get_collection` or
:meth:`~pymongo.collection.Collection.with_options` for getting a Collection
instance with its options configured differently than the Database's.

The following attributes have been removed:

- :attr:`~pymongo.collection.Collection.safe`
- :attr:`~pymongo.collection.Collection.secondary_acceptable_latency_ms`
- :attr:`~pymongo.collection.Collection.slave_okay`
- :attr:`~pymongo.collection.Collection.tag_sets`

The following methods have been added:

- :meth:`~pymongo.collection.Collection.bulk_write`
- :meth:`~pymongo.collection.Collection.insert_one`
- :meth:`~pymongo.collection.Collection.insert_many`
- :meth:`~pymongo.collection.Collection.update_one`
- :meth:`~pymongo.collection.Collection.update_many`
- :meth:`~pymongo.collection.Collection.replace_one`
- :meth:`~pymongo.collection.Collection.delete_one`
- :meth:`~pymongo.collection.Collection.delete_many`
- :meth:`~pymongo.collection.Collection.find_one_and_delete`
- :meth:`~pymongo.collection.Collection.find_one_and_replace`
- :meth:`~pymongo.collection.Collection.find_one_and_update`
- :meth:`~pymongo.collection.Collection.with_options`
- :meth:`~pymongo.collection.Collection.create_indexes`
- :meth:`~pymongo.collection.Collection.list_indexes`

The following methods have changed:

- :meth:`~pymongo.collection.Collection.aggregate` now **always** returns an
  instance of :class:`~pymongo.command_cursor.CommandCursor`. See the
  documentation for all options.
- :meth:`~pymongo.collection.Collection.count` now optionally takes a filter
  argument, as well as other options supported by the count command.
- :meth:`~pymongo.collection.Collection.distinct` now optionally takes a filter
  argument.
- :meth:`~pymongo.collection.Collection.create_index` no longer caches
  indexes, therefore the `cache_for` parameter has been removed. It also
  no longer supports the `bucket_size` and `drop_dups` aliases for `bucketSize`
  and `dropDups`.

The following methods are deprecated:

- :meth:`~pymongo.collection.Collection.save`
- :meth:`~pymongo.collection.Collection.insert`
- :meth:`~pymongo.collection.Collection.update`
- :meth:`~pymongo.collection.Collection.remove`
- :meth:`~pymongo.collection.Collection.find_and_modify`
- :meth:`~pymongo.collection.Collection.ensure_index`

The following methods have been removed:

The ``get_lasterror_options``, ``set_lasterror_options``, and
``unset_lasterror_options`` methods have been removed. Use
:class:`~pymongo.write_concern.WriteConcern` with
:meth:`~pymongo.collection.Collection.with_options` instead.

Changes to :meth:`~pymongo.collection.Collection.find` and :meth:`~pymongo.collection.Collection.find_one`
``````````````````````````````````````````````````````````````````````````````````````````````````````````

The following find/find_one options have been renamed:

These renames only affect your code if you passed these as keyword arguments,
like find(fields=['fieldname']). If you passed only positional parameters these
changes are not significant for your application.

- spec -> filter
- fields -> projection
- partial -> allow_partial_results

The following find/find_one options have been added:

- cursor_type (see :class:`~pymongo.cursor.CursorType` for values)
- oplog_replay
- modifiers

The following find/find_one options have been removed:

- network_timeout (use :meth:`~pymongo.cursor.Cursor.max_time_ms` instead)
- slave_okay (use one of the read preference classes from
  :mod:`~pymongo.read_preferences` and
  :meth:`~pymongo.collection.Collection.with_options` instead)
- read_preference (use :meth:`~pymongo.collection.Collection.with_options`
  instead)
- tag_sets (use one of the read preference classes from
  :mod:`~pymongo.read_preferences` and
  :meth:`~pymongo.collection.Collection.with_options` instead)
- secondary_acceptable_latency_ms (use the `localThresholdMS` URI option
  instead)
- max_scan (use the new `modifiers` option instead)
- snapshot (use the new `modifiers` option instead)
- tailable (use the new `cursor_type` option instead)
- await_data (use the new `cursor_type` option instead)
- exhaust (use the new `cursor_type` option instead)
- as_class (use :meth:`~pymongo.collection.Collection.with_options` with
  :class:`~bson.codec_options.CodecOptions` instead)
- compile_re (BSON regular expressions are always decoded to
  :class:`~bson.regex.Regex`)

The following find/find_one options are deprecated:

- manipulate

The following renames need special handling.

- timeout -> no_cursor_timeout -
  The default for `timeout` was True. The default for `no_cursor_timeout` is
  False. If you were previously passing False for `timeout` you must pass
  **True** for `no_cursor_timeout` to keep the previous behavior.

:mod:`~pymongo.errors` changes
..............................

The exception classes ``UnsupportedOption`` and ``TimeoutError`` are deleted.

:mod:`~gridfs` changes
......................

Since PyMongo 1.6, methods ``open`` and ``close`` of :class:`~gridfs.GridFS`
raised an ``UnsupportedAPI`` exception, as did the entire ``GridFile`` class.
The unsupported methods, the class, and the exception are all deleted.

:mod:`~bson` changes
....................

The `compile_re` option is removed from all methods
that accepted it in :mod:`~bson` and :mod:`~bson.json_util`. Additionally, it
is removed from :meth:`~pymongo.collection.Collection.find`,
:meth:`~pymongo.collection.Collection.find_one`,
:meth:`~pymongo.collection.Collection.aggregate`,
:meth:`~pymongo.database.Database.command`, and so on.
PyMongo now always represents BSON regular expressions as
:class:`~bson.regex.Regex` objects. This prevents errors for incompatible
patterns, see `PYTHON-500`_. Use :meth:`~bson.regex.Regex.try_compile` to
attempt to convert from a BSON regular expression to a Python regular
expression object.

PyMongo now decodes the int64 BSON type to :class:`~bson.int64.Int64`, a
trivial wrapper around long (in python 2.x) or int (in python 3.x). This
allows BSON int64 to be round tripped without losing type information in
python 3. Note that if you store a python long (or a python int larger than
4 bytes) it will be returned from PyMongo as :class:`~bson.int64.Int64`.

The `as_class`, `tz_aware`, and `uuid_subtype` options are removed from all
BSON encoding and decoding methods. Use
:class:`~bson.codec_options.CodecOptions` to configure these options. The
APIs affected are:

- :func:`~bson.decode_all`
- :func:`~bson.decode_iter`
- :func:`~bson.decode_file_iter`
- :meth:`~bson.BSON.encode`
- :meth:`~bson.BSON.decode`

This is a breaking change for any application that uses the BSON API directly
and changes any of the named parameter defaults. No changes are required for
applications that use the default values for these options. The behavior
remains the same.

.. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500

Issues Resolved
...............

See the `PyMongo 3.0 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 3.0 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12501

Changes in Version 2.8.1
------------------------

Version 2.8.1 fixes a number of issues reported since the release of PyMongo
2.8. It is a recommended upgrade for all users of PyMongo 2.x.

Issues Resolved
...............

See the `PyMongo 2.8.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.8.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/15324

Changes in Version 2.8
----------------------

Version 2.8 is a major release that provides full support for MongoDB 3.0 and
fixes a number of bugs.

Special thanks to Don Mitchell, Ximing, Can Zhang, Sergey Azovskov, and Heewa
Barfchin for their contributions to this release.

Highlights include:

- Support for the SCRAM-SHA-1 authentication mechanism (new in MongoDB 3.0).
- JSON decoder support for the new $numberLong and $undefined types.
- JSON decoder support for the $date type as an ISO-8601 string.
- Support passing an index name to :meth:`~pymongo.cursor.Cursor.hint`.
- The :meth:`~pymongo.cursor.Cursor.count` method will use a hint if one
  has been provided through :meth:`~pymongo.cursor.Cursor.hint`.
- A new socketKeepAlive option for the connection pool.
- New generator based BSON decode functions, :func:`~bson.decode_iter`
  and :func:`~bson.decode_file_iter`.
- Internal changes to support alternative storage engines like wiredtiger.

.. note:: There are a number of deprecations in this release for features that
  will be removed in PyMongo 3.0. These include:

  - :meth:`~pymongo.mongo_client.MongoClient.start_request`
  - :meth:`~pymongo.mongo_client.MongoClient.in_request`
  - :meth:`~pymongo.mongo_client.MongoClient.end_request`
  - :meth:`~pymongo.mongo_client.MongoClient.copy_database`
  - :meth:`~pymongo.database.Database.error`
  - :meth:`~pymongo.database.Database.last_status`
  - :meth:`~pymongo.database.Database.previous_error`
  - :meth:`~pymongo.database.Database.reset_error_history`
  - :class:`~pymongo.master_slave_connection.MasterSlaveConnection`

  The JSON format for :class:`~bson.timestamp.Timestamp` has changed from
  '{"t": <int>, "i": <int>}' to '{"$timestamp": {"t": <int>, "i": <int>}}'.
  This new format will be decoded to an instance of
  :class:`~bson.timestamp.Timestamp`. The old format will continue to be
  decoded to a python dict as before. Encoding to the old format is no
  longer supported as it was never correct and loses type information.

Issues Resolved
...............

See the `PyMongo 2.8 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.8 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/14223

Changes in Version 2.7.2
------------------------

Version 2.7.2 includes fixes for upsert reporting in the bulk API for MongoDB
versions previous to 2.6, a regression in how son manipulators are applied in
:meth:`~pymongo.collection.Collection.insert`, a few obscure connection pool
semaphore leaks, and a few other minor issues. See the list of issues resolved
for full details.

Issues Resolved
...............

See the `PyMongo 2.7.2 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.7.2 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/14005

Changes in Version 2.7.1
------------------------

Version 2.7.1 fixes a number of issues reported since the release of 2.7,
most importantly a fix for creating indexes and manipulating users through
mongos versions older than 2.4.0.

Issues Resolved
...............

See the `PyMongo 2.7.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.7.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/13823

Changes in Version 2.7
----------------------

PyMongo 2.7 is a major release with a large number of new features and bug
fixes. Highlights include:

- Full support for MongoDB 2.6.
- A new :doc:`bulk write operations API </examples/bulk>`.
- Support for server side query timeouts using
  :meth:`~pymongo.cursor.Cursor.max_time_ms`.
- Support for writing :meth:`~pymongo.collection.Collection.aggregate`
  output to a collection.
- A new :meth:`~pymongo.collection.Collection.parallel_scan` helper.
- :class:`~pymongo.errors.OperationFailure` and its subclasses now include
  a :attr:`~pymongo.errors.OperationFailure.details` attribute with complete
  error details from the server.
- A new GridFS :meth:`~gridfs.GridFS.find` method that returns a
  :class:`~gridfs.grid_file.GridOutCursor`.
- Greatly improved :doc:`support for mod_wsgi </examples/mod_wsgi>` when using
  PyMongo's C extensions. Read `Jesse's blog post
  <http://emptysqua.re/blog/python-c-extensions-and-mod-wsgi/>`_ for details.
- Improved C extension support for ARM little endian.

Breaking changes
................

Version 2.7 drops support for replica sets running MongoDB versions older
than 1.6.2.

Issues Resolved
...............

See the `PyMongo 2.7 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.7 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12892

Changes in Version 2.6.3
------------------------

Version 2.6.3 fixes issues reported since the release of 2.6.2, most
importantly a semaphore leak when a connection to the server fails.

Issues Resolved
...............

See the `PyMongo 2.6.3 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.6.3 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/13098

Changes in Version 2.6.2
------------------------

Version 2.6.2 fixes a :exc:`TypeError` problem when max_pool_size=None
is used in Python 3.

Issues Resolved
...............

See the `PyMongo 2.6.2 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.6.2 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12910

Changes in Version 2.6.1
------------------------

Version 2.6.1 fixes a reference leak in
the :meth:`~pymongo.collection.Collection.insert` method.

Issues Resolved
...............

See the `PyMongo 2.6.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.6.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12905

Changes in Version 2.6
----------------------

Version 2.6 includes some frequently requested improvements and adds
support for some early MongoDB 2.6 features.

Special thanks go to Justin Patrin for his work on the connection pool
in this release.

Important new features:

- The ``max_pool_size`` option for :class:`~pymongo.mongo_client.MongoClient`
  and :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` now
  actually caps the number of sockets the pool will open concurrently.
  Once the pool has reached :attr:`~pymongo.mongo_client.MongoClient.max_pool_size`
  operations will block waiting for a socket to become available. If
  ``waitQueueTimeoutMS`` is set, an operation that blocks waiting for a socket
  will raise :exc:`~pymongo.errors.ConnectionFailure` after the timeout. By
  default ``waitQueueTimeoutMS`` is not set.
  See :ref:`connection-pooling` for more information.
- The :meth:`~pymongo.collection.Collection.insert` method automatically splits
  large batches of documents into multiple insert messages based on
  :attr:`~pymongo.mongo_client.MongoClient.max_message_size`
- Support for the exhaust cursor flag.
  See :meth:`~pymongo.collection.Collection.find` for details and caveats.
- Support for the PLAIN and MONGODB-X509 authentication mechanisms.
  See :doc:`the authentication docs </examples/authentication>` for more
  information.
- Support aggregation output as a :class:`~pymongo.cursor.Cursor`. See
  :meth:`~pymongo.collection.Collection.aggregate` for details.

.. warning:: SIGNIFICANT BEHAVIOR CHANGE in 2.6. Previously, `max_pool_size`
  would limit only the idle sockets the pool would hold onto, not the
  number of open sockets. The default has also changed, from 10 to 100.
  If you pass a value for ``max_pool_size`` make sure it is large enough for
  the expected load. (Sockets are only opened when needed, so there is no cost
  to having a ``max_pool_size`` larger than necessary. Err towards a larger
  value.) If your application accepts the default, continue to do so.

  See :ref:`connection-pooling` for more information.

Issues Resolved
...............

See the `PyMongo 2.6 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.6 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12380

Changes in Version 2.5.2
------------------------

Version 2.5.2 fixes a NULL pointer dereference issue when decoding
an invalid :class:`~bson.dbref.DBRef`.

Issues Resolved
...............

See the `PyMongo 2.5.2 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.5.2 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12581

Changes in Version 2.5.1
------------------------

Version 2.5.1 is a minor release that fixes issues discovered after the
release of 2.5. Most importantly, this release addresses some race
conditions in replica set monitoring.

Issues Resolved
...............

See the `PyMongo 2.5.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.5.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12484

Changes in Version 2.5
----------------------

Version 2.5 includes changes to support new features in MongoDB 2.4.

Important new features:

- Support for :ref:`GSSAPI (Kerberos) authentication <use_kerberos>`.
- Support for SSL certificate validation with hostname matching.
- Support for delegated and role based authentication.
- New GEOSPHERE (2dsphere) and HASHED index constants.

.. note:: :meth:`~pymongo.database.Database.authenticate` now raises a
    subclass of :class:`~pymongo.errors.PyMongoError` if authentication
    fails due to invalid credentials or configuration issues.

Issues Resolved
...............

See the `PyMongo 2.5 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.5 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/11981

Changes in Version 2.4.2
------------------------

Version 2.4.2 is a minor release that fixes issues discovered after the
release of 2.4.1. Most importantly, PyMongo will no longer select a replica
set member for read operations that is not in primary or secondary state.

Issues Resolved
...............

See the `PyMongo 2.4.2 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.4.2 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12299

Changes in Version 2.4.1
------------------------

Version 2.4.1 is a minor release that fixes issues discovered after the
release of 2.4. Most importantly, this release fixes a regression using
:meth:`~pymongo.collection.Collection.aggregate`, and possibly other commands,
with mongos.

Issues Resolved
...............

See the `PyMongo 2.4.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.4.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/12286

Changes in Version 2.4
----------------------

Version 2.4 includes a few important new features and a large number of bug
fixes.

Important new features:

- New :class:`~pymongo.mongo_client.MongoClient` and
  :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` classes -
  these connection classes do acknowledged write operations (previously referred
  to as 'safe' writes) by default. :class:`~pymongo.connection.Connection` and
  :class:`~pymongo.replica_set_connection.ReplicaSetConnection` are deprecated
  but still support the old default fire-and-forget behavior.
- A new write concern API implemented as a
  :attr:`~pymongo.collection.Collection.write_concern` attribute on the connection,
  :class:`~pymongo.database.Database`, or :class:`~pymongo.collection.Collection`
  classes.
- :class:`~pymongo.mongo_client.MongoClient` (and :class:`~pymongo.connection.Connection`)
  now support Unix Domain Sockets.
- :class:`~pymongo.cursor.Cursor` can be copied with functions from the :mod:`copy`
  module.
- The :meth:`~pymongo.database.Database.set_profiling_level` method now supports
  a `slow_ms` option.
- The replica set monitor task (used by
  :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` and
  :class:`~pymongo.replica_set_connection.ReplicaSetConnection`) is a daemon thread
  once again, meaning you won't have to call
  :meth:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient.close` before
  exiting the python interactive shell.

.. warning::

    The constructors for :class:`~pymongo.mongo_client.MongoClient`,
    :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`,
    :class:`~pymongo.connection.Connection`, and
    :class:`~pymongo.replica_set_connection.ReplicaSetConnection` now raise
    :exc:`~pymongo.errors.ConnectionFailure` instead of its subclass
    :exc:`~pymongo.errors.AutoReconnect` if the server is unavailable. Applications
    that expect to catch :exc:`~pymongo.errors.AutoReconnect` should now catch
    :exc:`~pymongo.errors.ConnectionFailure` while creating a new connection.

Issues Resolved
...............

See the `PyMongo 2.4 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.4 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/11485

Changes in Version 2.3
----------------------

Version 2.3 adds support for new features and behavior changes in MongoDB
2.2.

Important New Features:

- Support for expanded read preferences including directing reads to tagged
  servers - See :ref:`secondary-reads` for more information.
- Support for mongos failover.
- A new :meth:`~pymongo.collection.Collection.aggregate` method to support
  MongoDB's new `aggregation framework
  <http://docs.mongodb.org/manual/applications/aggregation/>`_.
- Support for legacy Java and C# byte order when encoding and decoding UUIDs.
- Support for connecting directly to an arbiter.

.. warning::

    Starting with MongoDB 2.2 the getLastError command requires authentication
    when the server's `authentication features
    <http://www.mongodb.org/display/DOCS/Security+and+Authentication>`_ are enabled.
    Changes to PyMongo were required to support this behavior change. Users of
    authentication must upgrade to PyMongo 2.3 (or newer) for "safe" write operations
    to function correctly.

Issues Resolved
...............

See the `PyMongo 2.3 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.3 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/11146

Changes in Version 2.2.1
------------------------

Version 2.2.1 is a minor release that fixes issues discovered after the
release of 2.2. Most importantly, this release fixes an incompatibility
with mod_wsgi 2.x that could cause connections to leak. Users of mod_wsgi
2.x are strongly encouraged to upgrade from PyMongo 2.2.

Issues Resolved
...............

See the `PyMongo 2.2.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.2.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/11185

Changes in Version 2.2
----------------------

Version 2.2 adds a few more frequently requested features and fixes a
number of bugs.

Special thanks go to Alex Grönholm for his contributions to Python 3
support and maintaining the original pymongo3 port. Christoph Simon,
Wouter Bolsterlee, Mike O'Brien, and Chris Tompkinson also contributed
to this release.

Important New Features:

- Support for Python 3 -
  See the :doc:`python3` for more information.
- Support for Gevent -
  See :doc:`examples/gevent` for more information.
- Improved connection pooling.
  See `PYTHON-287 <https://jira.mongodb.org/browse/PYTHON-287>`_.

.. warning::

    A number of methods and method parameters that were deprecated in
    PyMongo 1.9 or older versions have been removed in this release.
    The full list of changes can be found in the following JIRA ticket:

    https://jira.mongodb.org/browse/PYTHON-305

    BSON module aliases from the pymongo package that were deprecated in
    PyMongo 1.9 have also been removed in this release. See the following
    JIRA ticket for details:

    https://jira.mongodb.org/browse/PYTHON-304

    As a result of this cleanup some minor code changes may be required
    to use this release.

Issues Resolved
...............

See the `PyMongo 2.2 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.2 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/10584

Changes in Version 2.1.1
------------------------

Version 2.1.1 is a minor release that fixes a few issues
discovered after the release of 2.1. You can now use
:class:`~pymongo.replica_set_connection.ReplicaSetConnection`
to run inline map reduce commands on secondaries. See
:meth:`~pymongo.collection.Collection.inline_map_reduce` for details.

Special thanks go to Samuel Clay and Ross Lawley for their contributions
to this release.

Issues Resolved
...............

See the `PyMongo 2.1.1 release notes in JIRA`_ for the list of resolved issues
in this release.

.. _PyMongo 2.1.1 release notes in JIRA: https://jira.mongodb.org/browse/PYTHON/fixforversion/11081

Changes in Version 2.1
----------------------

Version 2.1 adds a few frequently requested features and includes the usual
round of bug fixes and improvements.

Special thanks go to Alexey Borzenkov, Dan Crosta, Kostya Rybnikov,
Flavio Percoco Premoli, Jonas Haag, and Jesse Davis for their contributions
to this release.

Important New Features:

- ReplicaSetConnection -
  :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
  can be used to distribute reads to secondaries in a replica set. It supports
  automatic failover handling and periodically checks the state of the
  replica set to handle issues like primary stepdown or secondaries
  being removed for backup operations. Read preferences are defined through
  :class:`~pymongo.read_preferences.ReadPreference`.
- PyMongo supports the new BSON binary subtype 4 for UUIDs. The default
  subtype to use can be set through
  :attr:`~pymongo.collection.Collection.uuid_subtype`
  The current default remains :attr:`~bson.binary.OLD_UUID_SUBTYPE` but will
  be changed to :attr:`~bson.binary.UUID_SUBTYPE` in a future release.
- The getLastError option 'w' can be set to a string, allowing for options
  like "majority" available in newer version of MongoDB.
- Added support for the MongoDB URI options socketTimeoutMS and connectTimeoutMS.
- Added support for the ContinueOnError insert flag.
- Added basic SSL support.
- Added basic support for Jython.
- Secondaries can be used for :meth:`~pymongo.cursor.Cursor.count`,
  :meth:`~pymongo.cursor.Cursor.distinct`, :meth:`~pymongo.collection.Collection.group`,
  and querying :class:`~gridfs.GridFS`.
- Added document_class and tz_aware options to
  :class:`~pymongo.master_slave_connection.MasterSlaveConnection`

Issues Resolved
...............

See the `PyMongo 2.1 release notes in JIRA`_ for the list of resolved issues in this release.

.. _PyMongo 2.1 release notes in JIRA: https://jira.mongodb.org/secure/ReleaseNote.jspa?projectId=10004&version=10583

Changes in Version 2.0.1
------------------------

Version 2.0.1 fixes a regression in :class:`~gridfs.grid_file.GridIn` when
writing pre-chunked strings. Thanks go to Alexey Borzenkov for reporting the
issue and submitting a patch.

Issues Resolved
...............

- `PYTHON-271 <https://jira.mongodb.org/browse/PYTHON-271>`_:
  Regression in GridFS leads to serious loss of data.

Changes in Version 2.0
----------------------

Version 2.0 adds a large number of features and fixes a number of issues.

Special thanks go to James Murty, Abhay Vardhan, David Pisoni, Ryan Smith-Roberts,
Andrew Pendleton, Mher Movsisyan, Reed O'Brien, Michael Schurter, Josip Delic
and Jonas Haag for their contributions to this release.

Important New Features:

- PyMongo now performs automatic per-socket database authentication. You no
  longer have to re-authenticate for each new thread or after a replica set
  failover. Authentication credentials are cached by the driver until the
  application calls :meth:`~pymongo.database.Database.logout`.
- slave_okay can be set independently at the connection, database, collection
  or query level. Each level will inherit the slave_okay setting from the
  previous level and each level can override the previous level's setting.
- safe and getLastError options (e.g. w, wtimeout, etc.) can be set
  independently at the connection, database, collection or query level. Each
  level will inherit settings from the previous level and each level can
  override the previous level's setting.
- PyMongo now supports the `await_data` and `partial` cursor flags. If the
  `await_data` flag is set on a `tailable` cursor the server will block for
  some extra time waiting for more data to return. The `partial` flag tells
  a mongos to return partial data for a query if not all shards are available.
- :meth:`~pymongo.collection.Collection.map_reduce` will accept a `dict` or
  instance of :class:`~bson.son.SON` as the `out` parameter.
- The URI parser has been moved into its own module and can be used directly
  by application code.
- AutoReconnect exception now provides information about the error that
  actually occured instead of a generic failure message.
- A number of new helper methods have been added with options for setting and
  unsetting cursor flags, re-indexing a collection, fsync and locking a server,
  and getting the server's current operations.

API changes:

- If only one host:port pair is specified :class:`~pymongo.connection.Connection`
  will make a direct connection to only that host. Please note that `slave_okay`
  must be `True` in order to query from a secondary.
- If more than one host:port pair is specified or the `replicaset` option is
  used PyMongo will treat the specified host:port pair(s) as a seed list and
  connect using replica set behavior.

.. warning::

    The default subtype for :class:`~bson.binary.Binary` has changed
    from :const:`~bson.binary.OLD_BINARY_SUBTYPE` (2) to
    :const:`~bson.binary.BINARY_SUBTYPE` (0).

Issues Resolved
...............

See the `PyMongo 2.0 release notes in JIRA`_ for the list of resolved issues in this release.

.. _PyMongo 2.0 release notes in JIRA: https://jira.mongodb.org/secure/ReleaseNote.jspa?projectId=10004&version=10274

Changes in Version 1.11
-----------------------

Version 1.11 adds a few new features and fixes a few more bugs.

New Features:

- Basic IPv6 support: pymongo prefers IPv4 but will try IPv6. You can
  also specify an IPv6 address literal in the `host` parameter or a
  MongoDB URI provided it is enclosed in '[' and ']'.
- max_pool_size option: previously pymongo had a hard coded pool size
  of 10 connections. With this change you can specify a different pool
  size as a parameter to :class:`~pymongo.connection.Connection`
  (max_pool_size=<integer>) or in the MongoDB URI (maxPoolSize=<integer>).
- Find by metadata in GridFS: You can know specify query fields as
  keyword parameters for :meth:`~gridfs.GridFS.get_version` and
  :meth:`~gridfs.GridFS.get_last_version`.
- Per-query slave_okay option: slave_okay=True is now a valid keyword
  argument for :meth:`~pymongo.collection.Collection.find` and
  :meth:`~pymongo.collection.Collection.find_one`.

API changes:

- :meth:`~pymongo.database.Database.validate_collection` now returns a
  dict instead of a string. This change was required to deal with an
  API change on the server. This method also now takes the optional
  `scandata` and `full` parameters. See the documentation for more
  details.

.. warning::  The `pool_size`, `auto_start_request`, and `timeout` parameters
              for :class:`~pymongo.connection.Connection` have been completely
              removed in this release. They were deprecated in pymongo-1.4 and
              have had no effect since then. Please make sure that your code
              doesn't currently pass these parameters when creating a
              Connection instance.

Issues resolved
...............

- `PYTHON-241 <https://jira.mongodb.org/browse/PYTHON-241>`_:
  Support setting slaveok at the cursor level.
- `PYTHON-240 <https://jira.mongodb.org/browse/PYTHON-240>`_:
  Queries can sometimes permanently fail after a replica set fail over.
- `PYTHON-238 <https://jira.mongodb.org/browse/PYTHON-238>`_:
  error after few million requests
- `PYTHON-237 <https://jira.mongodb.org/browse/PYTHON-237>`_:
  Basic IPv6 support.
- `PYTHON-236 <https://jira.mongodb.org/browse/PYTHON-236>`_:
  Restore option to specify pool size in Connection.
- `PYTHON-212 <https://jira.mongodb.org/browse/PYTHON-212>`_:
  pymongo does not recover after stale config
- `PYTHON-138 <https://jira.mongodb.org/browse/PYTHON-138>`_:
  Find method for GridFS

Changes in Version 1.10.1
-------------------------

Version 1.10.1 is primarily a bugfix release. It fixes a regression in
version 1.10 that broke pickling of ObjectIds. A number of other bugs
have been fixed as well.

There are two behavior changes to be aware of:

- If a read slave raises :class:`~pymongo.errors.AutoReconnect`
  :class:`~pymongo.master_slave_connection.MasterSlaveConnection` will now
  retry the query on each slave until it is successful or all slaves have
  raised :class:`~pymongo.errors.AutoReconnect`. Any other exception will
  immediately be raised. The order that the slaves are tried is random.
  Previously the read would be sent to one randomly chosen slave and
  :class:`~pymongo.errors.AutoReconnect` was immediately raised in case
  of a connection failure.
- A Python `long` is now always BSON encoded as an int64. Previously the
  encoding was based only on the value of the field and a `long` with a
  value less than `2147483648` or greater than `-2147483649` would always
  be BSON encoded as an int32.

Issues resolved
...............

- `PYTHON-234 <https://jira.mongodb.org/browse/PYTHON-234>`_:
  Fix setup.py to raise exception if any when building extensions
- `PYTHON-233 <https://jira.mongodb.org/browse/PYTHON-233>`_:
  Add information to build and test with extensions on windows
- `PYTHON-232 <https://jira.mongodb.org/browse/PYTHON-232>`_:
  Traceback when hashing a DBRef instance
- `PYTHON-231 <https://jira.mongodb.org/browse/PYTHON-231>`_:
  Traceback when pickling a DBRef instance
- `PYTHON-230 <https://jira.mongodb.org/browse/PYTHON-230>`_:
  Pickled ObjectIds are not compatible between pymongo 1.9 and 1.10
- `PYTHON-228 <https://jira.mongodb.org/browse/PYTHON-228>`_:
  Cannot pickle bson.ObjectId
- `PYTHON-227 <https://jira.mongodb.org/browse/PYTHON-227>`_:
  Traceback when calling find() on system.js
- `PYTHON-216 <https://jira.mongodb.org/browse/PYTHON-216>`_:
  MasterSlaveConnection is missing disconnect() method
- `PYTHON-186 <https://jira.mongodb.org/browse/PYTHON-186>`_:
  When storing integers, type is selected according to value instead of type
- `PYTHON-173 <https://jira.mongodb.org/browse/PYTHON-173>`_:
  as_class option is not propogated by Cursor.clone
- `PYTHON-113 <https://jira.mongodb.org/browse/PYTHON-113>`_:
  Redunducy in MasterSlaveConnection

Changes in Version 1.10
-----------------------

Version 1.10 includes changes to support new features in MongoDB 1.8.x.
Highlights include a modified map/reduce API including an inline map/reduce
helper method, a new find_and_modify helper, and the ability to query the
server for the maximum BSON document size it supports.

- added :meth:`~pymongo.collection.Collection.find_and_modify`.
- added :meth:`~pymongo.collection.Collection.inline_map_reduce`.
- changed :meth:`~pymongo.collection.Collection.map_reduce`.

.. warning:: MongoDB versions greater than 1.7.4 no longer generate temporary
   collections for map/reduce results. An output collection name must be
   provided and the output will replace any existing output collection with
   the same name. :meth:`~pymongo.collection.Collection.map_reduce` now
   requires the `out` parameter.

Issues resolved
...............

- PYTHON-225: :class:`~pymongo.objectid.ObjectId` class definition should use __slots__.
- PYTHON-223: Documentation fix.
- PYTHON-220: Documentation fix.
- PYTHON-219: KeyError in :meth:`~pymongo.collection.Collection.find_and_modify`
- PYTHON-213: Query server for maximum BSON document size.
- PYTHON-208: Fix :class:`~pymongo.connection.Connection` __repr__.
- PYTHON-207: Changes to Map/Reduce API.
- PYTHON-205: Accept slaveOk in the URI to match the URI docs.
- PYTHON-203: When slave_okay=True and we only specify one host don't autodetect other set members.
- PYTHON-194: Show size when whining about a document being too large.
- PYTHON-184: Raise :class:`~pymongo.errors.DuplicateKeyError` for duplicate keys in capped collections.
- PYTHON-178: Don't segfault when trying to encode a recursive data structure.
- PYTHON-177: Don't segfault when decoding dicts with broken iterators.
- PYTHON-172: Fix a typo.
- PYTHON-170: Add :meth:`~pymongo.collection.Collection.find_and_modify`.
- PYTHON-169: Support deepcopy of DBRef.
- PYTHON-167: Duplicate of PYTHON-166.
- PYTHON-166: Fixes a concurrency issue.
- PYTHON-158: Add code and err string to `db assertion` messages.

Changes in Version 1.9
----------------------

Version 1.9 adds a new package to the PyMongo distribution,
:mod:`bson`. :mod:`bson` contains all of the `BSON
<http://bsonspec.org>`_ encoding and decoding logic, and the BSON
types that were formerly in the :mod:`pymongo` package. The following
modules have been renamed:

  - :mod:`pymongo.bson` -> :mod:`bson`
  - :mod:`pymongo._cbson` -> :mod:`bson._cbson` and
    :mod:`pymongo._cmessage`
  - :mod:`pymongo.binary` -> :mod:`bson.binary`
  - :mod:`pymongo.code` -> :mod:`bson.code`
  - :mod:`pymongo.dbref` -> :mod:`bson.dbref`
  - :mod:`pymongo.json_util` -> :mod:`bson.json_util`
  - :mod:`pymongo.max_key` -> :mod:`bson.max_key`
  - :mod:`pymongo.min_key` -> :mod:`bson.min_key`
  - :mod:`pymongo.objectid` -> :mod:`bson.objectid`
  - :mod:`pymongo.son` -> :mod:`bson.son`
  - :mod:`pymongo.timestamp` -> :mod:`bson.timestamp`
  - :mod:`pymongo.tz_util` -> :mod:`bson.tz_util`

In addition, the following exception classes have been renamed:

  - :class:`pymongo.errors.InvalidBSON` ->
    :class:`bson.errors.InvalidBSON`
  - :class:`pymongo.errors.InvalidStringData` ->
    :class:`bson.errors.InvalidStringData`
  - :class:`pymongo.errors.InvalidDocument` ->
    :class:`bson.errors.InvalidDocument`
  - :class:`pymongo.errors.InvalidId` ->
    :class:`bson.errors.InvalidId`

The above exceptions now inherit from :class:`bson.errors.BSONError`
rather than :class:`pymongo.errors.PyMongoError`.

.. note::  All of the renamed modules and exceptions above have aliases
           created with the old names, so these changes should not break
           existing code. The old names will eventually be deprecated and then
           removed, so users should begin migrating towards the new names now.

.. warning::

  The change to the exception hierarchy mentioned above is
  possibly breaking. If your code is catching
  :class:`~pymongo.errors.PyMongoError`, then the exceptions raised
  by :mod:`bson` will not be caught, even though they would have been
  caught previously. Before upgrading, it is recommended that users
  check for any cases like this.

- the C extension now shares buffer.c/h with the Ruby driver
- :mod:`bson` no longer raises :class:`~pymongo.errors.InvalidName`,
  all occurrences have been replaced with
  :class:`~bson.errors.InvalidDocument`.
- renamed :meth:`bson._to_dicts` to :meth:`~bson.decode_all`.
- renamed :meth:`~bson.BSON.from_dict` to :meth:`~bson.BSON.encode`
  and :meth:`~bson.BSON.to_dict` to :meth:`~bson.BSON.decode`.
- added :meth:`~pymongo.cursor.Cursor.batch_size`.
- allow updating (some) file metadata after a
  :class:`~gridfs.grid_file.GridIn` instance has been closed.
- performance improvements for reading from GridFS.
- special cased slice with the same start and stop to return an empty
  cursor.
- allow writing :class:`unicode` to GridFS if an :attr:`encoding`
  attribute has been specified for the file.
- added :meth:`gridfs.GridFS.get_version`.
- scope variables for :class:`~bson.code.Code` can now be specified as
  keyword arguments.
- added :meth:`~gridfs.grid_file.GridOut.readline` to
  :class:`~gridfs.grid_file.GridOut`.
- make a best effort to transparently auto-reconnect if a
  :class:`~pymongo.connection.Connection` has been idle for a while.
- added :meth:`~pymongo.database.SystemJS.list` to
  :class:`~pymongo.database.SystemJS`.
- added `file_document` argument to :meth:`~gridfs.grid_file.GridOut`
  to allow initializing from an existing file document.
- raise :class:`~pymongo.errors.TimeoutError` even if the
  ``getLastError`` command was run manually and not through "safe"
  mode.
- added :class:`uuid` support to :mod:`~bson.json_util`.

Changes in Version 1.8.1
------------------------

- fixed a typo in the C extension that could cause safe-mode
  operations to report a failure (:class:`SystemError`) even when none
  occurred.
- added a :meth:`__ne__` implementation to any class where we define
  :meth:`__eq__`.

Changes in Version 1.8
----------------------

Version 1.8 adds support for connecting to replica sets, specifying
per-operation values for `w` and `wtimeout`, and decoding to
timezone-aware datetimes.

- fixed a reference leak in the C extension when decoding a
  :class:`~bson.dbref.DBRef`.
- added support for `w`, `wtimeout`, and `fsync` (and any other
  options for `getLastError`) to "safe mode" operations.
- added :attr:`~pymongo.connection.Connection.nodes` property.
- added a maximum pool size of 10 sockets.
- added support for replica sets.
- DEPRECATED :meth:`~pymongo.connection.Connection.from_uri` and
  :meth:`~pymongo.connection.Connection.paired`, both are supplanted
  by extended functionality in :meth:`~pymongo.connection.Connection`.
- added tz aware support for datetimes in
  :class:`~bson.objectid.ObjectId`,
  :class:`~bson.timestamp.Timestamp` and :mod:`~bson.json_util`
  methods.
- added :meth:`~pymongo.collection.Collection.drop` helper.
- reuse the socket used for finding the master when a
  :class:`~pymongo.connection.Connection` is first created.
- added support for :class:`~bson.min_key.MinKey`,
  :class:`~bson.max_key.MaxKey` and
  :class:`~bson.timestamp.Timestamp` to :mod:`~bson.json_util`.
- added support for decoding datetimes as aware (UTC) - it is highly
  recommended to enable this by setting the `tz_aware` parameter to
  :meth:`~pymongo.connection.Connection` to ``True``.
- added `network_timeout` option for individual calls to
  :meth:`~pymongo.collection.Collection.find` and
  :meth:`~pymongo.collection.Collection.find_one`.
- added :meth:`~gridfs.GridFS.exists` to check if a file exists in
  GridFS.
- added support for additional keys in :class:`~bson.dbref.DBRef`
  instances.
- added :attr:`~pymongo.errors.OperationFailure.code` attribute to
  :class:`~pymongo.errors.OperationFailure` exceptions.
- fixed serialization of int and float subclasses in the C extension.

Changes in Version 1.7
----------------------

Version 1.7 is a recommended upgrade for all PyMongo users. The full
release notes are below, and some more in depth discussion of the
highlights is `here
<http://dirolf.com/2010/06/17/pymongo-1.7-released.html>`_.

- no longer attempt to build the C extension on big-endian systems.
- added :class:`~bson.min_key.MinKey` and
  :class:`~bson.max_key.MaxKey`.
- use unsigned for :class:`~bson.timestamp.Timestamp` in BSON
  encoder/decoder.
- support ``True`` as ``"ok"`` in command responses, in addition to
  ``1.0`` - necessary for server versions **>= 1.5.X**
- BREAKING change to
  :meth:`~pymongo.collection.Collection.index_information` to add
  support for querying unique status and other index information.
- added :attr:`~pymongo.connection.Connection.document_class`, to
  specify class for returned documents.
- added `as_class` argument for
  :meth:`~pymongo.collection.Collection.find`, and in the BSON decoder.
- added support for creating :class:`~bson.timestamp.Timestamp`
  instances using a :class:`~datetime.datetime`.
- allow `dropTarget` argument for
  :class:`~pymongo.collection.Collection.rename`.
- handle aware :class:`~datetime.datetime` instances, by converting to
  UTC.
- added support for :class:`~pymongo.cursor.Cursor.max_scan`.
- raise :class:`~gridfs.errors.FileExists` exception when creating a
  duplicate GridFS file.
- use `y2038 <http://code.google.com/p/y2038/>`_ for time handling in
  the C extension - eliminates 2038 problems when extension is
  installed.
- added `sort` parameter to
  :meth:`~pymongo.collection.Collection.find`
- finalized deprecation of changes from versions **<= 1.4**
- take any non-:class:`dict` as an ``"_id"`` query for
  :meth:`~pymongo.collection.Collection.find_one` or
  :meth:`~pymongo.collection.Collection.remove`
- added ability to pass a :class:`dict` for `fields` argument to
  :meth:`~pymongo.collection.Collection.find` (supports ``"$slice"``
  and field negation)
- simplified code to find master, since paired setups don't always have
  a remote
- fixed bug in C encoder for certain invalid types (like
  :class:`~pymongo.collection.Collection` instances).
- don't transparently map ``"filename"`` key to :attr:`name` attribute
  for GridFS.

Changes in Version 1.6
----------------------

The biggest change in version 1.6 is a complete re-implementation of
:mod:`gridfs` with a lot of improvements over the old
implementation. There are many details and examples of using the new
API in `this blog post
<http://dirolf.com/2010/03/29/new-gridfs-implementation-for-pymongo.html>`_. The
old API has been removed in this version, so existing code will need
to be modified before upgrading to 1.6.

- fixed issue where connection pool was being shared across
  :class:`~pymongo.connection.Connection` instances.
- more improvements to Python code caching in C extension - should
  improve behavior on mod_wsgi.
- added :meth:`~bson.objectid.ObjectId.from_datetime`.
- complete rewrite of :mod:`gridfs` support.
- improvements to the :meth:`~pymongo.database.Database.command` API.
- fixed :meth:`~pymongo.collection.Collection.drop_indexes` behavior
  on non-existent collections.
- disallow empty bulk inserts.

Changes in Version 1.5.2
------------------------
- fixed response handling to ignore unknown response flags in queries.
- handle server versions containing '-pre-'.

Changes in Version 1.5.1
------------------------
- added :data:`~gridfs.grid_file.GridFile._id` property for
  :class:`~gridfs.grid_file.GridFile` instances.
- fix for making a :class:`~pymongo.connection.Connection` (with
  `slave_okay` set) directly to a slave in a replica pair.
- accept kwargs for
  :meth:`~pymongo.collection.Collection.create_index` and
  :meth:`~pymongo.collection.Collection.ensure_index` to support all
  indexing options.
- add :data:`pymongo.GEO2D` and support for geo indexing.
- improvements to Python code caching in C extension - should improve
  behavior on mod_wsgi.

Changes in Version 1.5
----------------------
- added subtype constants to :mod:`~bson.binary` module.
- DEPRECATED `options` argument to
  :meth:`~pymongo.collection.Collection` and
  :meth:`~pymongo.database.Database.create_collection` in favor of
  kwargs.
- added :meth:`~pymongo.has_c` to check for C extension.
- added :meth:`~pymongo.connection.Connection.copy_database`.
- added :data:`~pymongo.cursor.Cursor.alive` to tell when a cursor
  might have more data to return (useful for tailable cursors).
- added :class:`~bson.timestamp.Timestamp` to better support
  dealing with internal MongoDB timestamps.
- added `name` argument for
  :meth:`~pymongo.collection.Collection.create_index` and
  :meth:`~pymongo.collection.Collection.ensure_index`.
- fixed connection pooling w/ fork
- :meth:`~pymongo.connection.Connection.paired` takes all kwargs that
  are allowed for :meth:`~pymongo.connection.Connection`.
- :meth:`~pymongo.collection.Collection.insert` returns list for bulk
  inserts of size one.
- fixed handling of :class:`datetime.datetime` instances in
  :mod:`~bson.json_util`.
- added :meth:`~pymongo.connection.Connection.from_uri` to support
  MongoDB connection uri scheme.
- fixed chunk number calculation when unaligned in :mod:`gridfs`.
- :meth:`~pymongo.database.Database.command` takes a string for simple
  commands.
- added :data:`~pymongo.database.Database.system_js` helper for
  dealing with server-side JS.
- don't wrap queries containing ``"$query"`` (support manual use of
  ``"$min"``, etc.).
- added :class:`~gridfs.errors.GridFSError` as base class for
  :mod:`gridfs` exceptions.

Changes in Version 1.4
----------------------

Perhaps the most important change in version 1.4 is that we have
decided to **no longer support Python 2.3**. The most immediate reason
for this is to allow some improvements to connection pooling. This
will also allow us to use some new (as in Python 2.4 ;) idioms and
will help begin the path towards supporting Python 3.0. If you need to
use Python 2.3 you should consider using version 1.3 of this driver,
although that will no longer be actively supported.

Other changes:

- move ``"_id"`` to front only for top-level documents (fixes some
  corner cases).
- :meth:`~pymongo.collection.Collection.update` and
  :meth:`~pymongo.collection.Collection.remove` return the entire
  response to the *lastError* command when safe is ``True``.
- completed removal of things that were deprecated in version 1.2 or
  earlier.
- enforce that collection names do not contain the NULL byte.
- fix to allow using UTF-8 collection names with the C extension.
- added :class:`~pymongo.errors.PyMongoError` as base exception class
  for all :mod:`~pymongo.errors`. this changes the exception hierarchy
  somewhat, and is a BREAKING change if you depend on
  :class:`~pymongo.errors.ConnectionFailure` being a :class:`IOError`
  or :class:`~bson.errors.InvalidBSON` being a :class:`ValueError`,
  for example.
- added :class:`~pymongo.errors.DuplicateKeyError` for calls to
  :meth:`~pymongo.collection.Collection.insert` or
  :meth:`~pymongo.collection.Collection.update` with `safe` set to
  ``True``.
- removed :mod:`~pymongo.thread_util`.
- added :meth:`~pymongo.database.Database.add_user` and
  :meth:`~pymongo.database.Database.remove_user` helpers.
- fix for :meth:`~pymongo.database.Database.authenticate` when using
  non-UTF-8 names or passwords.
- minor fixes for
  :class:`~pymongo.master_slave_connection.MasterSlaveConnection`.
- clean up all cases where :class:`~pymongo.errors.ConnectionFailure`
  is raised.
- simplification of connection pooling - makes driver ~2x faster for
  simple benchmarks. see :ref:`connection-pooling` for more information.
- DEPRECATED `pool_size`, `auto_start_request` and `timeout`
  parameters to :class:`~pymongo.connection.Connection`. DEPRECATED
  :meth:`~pymongo.connection.Connection.start_request`.
- use :meth:`socket.sendall`.
- removed :meth:`~bson.son.SON.from_xml` as it was only being used
  for some internal testing - also eliminates dependency on
  :mod:`elementtree`.
- implementation of :meth:`~pymongo.message.update` in C.
- deprecate :meth:`~pymongo.database.Database._command` in favor of
  :meth:`~pymongo.database.Database.command`.
- send all commands without wrapping as ``{"query": ...}``.
- support string as `key` argument to
  :meth:`~pymongo.collection.Collection.group` (keyf) and run all
  groups as commands.
- support for equality testing for :class:`~bson.code.Code`
  instances.
- allow the NULL byte in strings and disallow it in key names or regex
  patterns

Changes in Version 1.3
----------------------
- DEPRECATED running :meth:`~pymongo.collection.Collection.group` as
  :meth:`~pymongo.database.Database.eval`, also changed default for
  :meth:`~pymongo.collection.Collection.group` to running as a command
- remove :meth:`pymongo.cursor.Cursor.__len__`, which was deprecated
  in 1.1.1 - needed to do this aggressively due to it's presence
  breaking **Django** template *for* loops
- DEPRECATED :meth:`~pymongo.connection.Connection.host`,
  :meth:`~pymongo.connection.Connection.port`,
  :meth:`~pymongo.database.Database.connection`,
  :meth:`~pymongo.database.Database.name`,
  :meth:`~pymongo.collection.Collection.database`,
  :meth:`~pymongo.collection.Collection.name` and
  :meth:`~pymongo.collection.Collection.full_name` in favor of
  :attr:`~pymongo.connection.Connection.host`,
  :attr:`~pymongo.connection.Connection.port`,
  :attr:`~pymongo.database.Database.connection`,
  :attr:`~pymongo.database.Database.name`,
  :attr:`~pymongo.collection.Collection.database`,
  :attr:`~pymongo.collection.Collection.name` and
  :attr:`~pymongo.collection.Collection.full_name`, respectively. The
  deprecation schedule for this change will probably be faster than
  usual, as it carries some performance implications.
- added :meth:`~pymongo.connection.Connection.disconnect`

Changes in Version 1.2.1
------------------------
- added :doc:`changelog` to docs
- added ``setup.py doc --test`` to run doctests for tutorial, examples
- moved most examples to Sphinx docs (and remove from *examples/*
  directory)
- raise :class:`~bson.errors.InvalidId` instead of
  :class:`TypeError` when passing a 24 character string to
  :class:`~bson.objectid.ObjectId` that contains non-hexadecimal
  characters
- allow :class:`unicode` instances for :class:`~bson.objectid.ObjectId` init

Changes in Version 1.2
----------------------
- `spec` parameter for :meth:`~pymongo.collection.Collection.remove` is
  now optional to allow for deleting all documents in a
  :class:`~pymongo.collection.Collection`
- always wrap queries with ``{query: ...}`` even when no special options -
  get around some issues with queries on fields named ``query``
- enforce 4MB document limit on the client side
- added :meth:`~pymongo.collection.Collection.map_reduce` helper - see
  :doc:`example <examples/aggregation>`
- added :meth:`~pymongo.cursor.Cursor.distinct` method on
  :class:`~pymongo.cursor.Cursor` instances to allow distinct with
  queries
- fix for :meth:`~pymongo.cursor.Cursor.__getitem__` after
  :meth:`~pymongo.cursor.Cursor.skip`
- allow any UTF-8 string in :class:`~bson.BSON` encoder, not
  just ASCII subset
- added :attr:`~bson.objectid.ObjectId.generation_time`
- removed support for legacy :class:`~bson.objectid.ObjectId`
  format - pretty sure this was never used, and is just confusing
- DEPRECATED :meth:`~bson.objectid.ObjectId.url_encode` and
  :meth:`~bson.objectid.ObjectId.url_decode` in favor of :meth:`str`
  and :meth:`~bson.objectid.ObjectId`, respectively
- allow *oplog.$main* as a valid collection name
- some minor fixes for installation process
- added support for datetime and regex in :mod:`~bson.json_util`

Changes in Version 1.1.2
------------------------
- improvements to :meth:`~pymongo.collection.Collection.insert` speed
  (using C for insert message creation)
- use random number for request_id
- fix some race conditions with :class:`~pymongo.errors.AutoReconnect`

Changes in Version 1.1.1
------------------------
- added `multi` parameter for
  :meth:`~pymongo.collection.Collection.update`
- fix unicode regex patterns with C extension
- added :meth:`~pymongo.collection.Collection.distinct`
- added `database` support for :class:`~bson.dbref.DBRef`
- added :mod:`~bson.json_util` with helpers for encoding / decoding
  special types to JSON
- DEPRECATED :meth:`pymongo.cursor.Cursor.__len__` in favor of
  :meth:`~pymongo.cursor.Cursor.count` with `with_limit_and_skip` set
  to ``True`` due to performance regression
- switch documentation to Sphinx

Changes in Version 1.1
----------------------
- added :meth:`__hash__` for :class:`~bson.dbref.DBRef` and
  :class:`~bson.objectid.ObjectId`
- bulk :meth:`~pymongo.collection.Collection.insert` works with any
  iterable
- fix :class:`~bson.objectid.ObjectId` generation when using
  :mod:`multiprocessing`
- added :attr:`~pymongo.cursor.Cursor.collection`
- added `network_timeout` parameter for
  :meth:`~pymongo.connection.Connection`
- DEPRECATED `slave_okay` parameter for individual queries
- fix for `safe` mode when multi-threaded
- added `safe` parameter for :meth:`~pymongo.collection.Collection.remove`
- added `tailable` parameter for :meth:`~pymongo.collection.Collection.find`

Changes in Version 1.0
----------------------
- fixes for
  :class:`~pymongo.master_slave_connection.MasterSlaveConnection`
- added `finalize` parameter for :meth:`~pymongo.collection.Collection.group`
- improvements to :meth:`~pymongo.collection.Collection.insert` speed
- improvements to :mod:`gridfs` speed
- added :meth:`~pymongo.cursor.Cursor.__getitem__` and
  :meth:`~pymongo.cursor.Cursor.__len__` for
  :class:`~pymongo.cursor.Cursor` instances

Changes in Version 0.16
-----------------------
- support for encoding/decoding :class:`uuid.UUID` instances
- fix for :meth:`~pymongo.cursor.Cursor.explain` with limits

Changes in Version 0.15.2
-------------------------
- documentation changes only

Changes in Version 0.15.1
-------------------------
- various performance improvements
- API CHANGE no longer need to specify direction for
  :meth:`~pymongo.collection.Collection.create_index` and
  :meth:`~pymongo.collection.Collection.ensure_index` when indexing a
  single key
- support for encoding :class:`tuple` instances as :class:`list`
  instances

Changes in Version 0.15
-----------------------
- fix string representation of :class:`~bson.objectid.ObjectId`
  instances
- added `timeout` parameter for
  :meth:`~pymongo.collection.Collection.find`
- allow scope for `reduce` function in
  :meth:`~pymongo.collection.Collection.group`

Changes in Version 0.14.2
-------------------------
- minor bugfixes

Changes in Version 0.14.1
-------------------------
- :meth:`~gridfs.grid_file.GridFile.seek` and
  :meth:`~gridfs.grid_file.GridFile.tell` for (read mode)
  :class:`~gridfs.grid_file.GridFile` instances

Changes in Version 0.14
-----------------------
- support for long in :class:`~bson.BSON`
- added :meth:`~pymongo.collection.Collection.rename`
- added `snapshot` parameter for
  :meth:`~pymongo.collection.Collection.find`

Changes in Version 0.13
-----------------------
- better
  :class:`~pymongo.master_slave_connection.MasterSlaveConnection`
  support
- API CHANGE :meth:`~pymongo.collection.Collection.insert` and
  :meth:`~pymongo.collection.Collection.save` both return inserted
  ``_id``
- DEPRECATED passing an index name to
  :meth:`~pymongo.cursor.Cursor.hint`

Changes in Version 0.12
-----------------------
- improved :class:`~bson.objectid.ObjectId` generation
- added :class:`~pymongo.errors.AutoReconnect` exception for when
  reconnection is possible
- make :mod:`gridfs` thread-safe
- fix for :mod:`gridfs` with non :class:`~bson.objectid.ObjectId` ``_id``

Changes in Version 0.11.3
-------------------------
- don't allow NULL bytes in string encoder
- fixes for Python 2.3

Changes in Version 0.11.2
-------------------------
- PEP 8
- updates for :meth:`~pymongo.collection.Collection.group`
- VS build

Changes in Version 0.11.1
-------------------------
- fix for connection pooling under Python 2.5

Changes in Version 0.11
-----------------------
- better build failure detection
- driver support for selecting fields in sub-documents
- disallow insertion of invalid key names
- added `timeout` parameter for :meth:`~pymongo.connection.Connection`

Changes in Version 0.10.3
-------------------------
- fix bug with large :meth:`~pymongo.cursor.Cursor.limit`
- better exception when modules get reloaded out from underneath the C
  extension
- better exception messages when calling a
  :class:`~pymongo.collection.Collection` or
  :class:`~pymongo.database.Database` instance

Changes in Version 0.10.2
-------------------------
- support subclasses of :class:`dict` in C encoder

Changes in Version 0.10.1
-------------------------
- alias :class:`~pymongo.connection.Connection` as
  :attr:`pymongo.Connection`
- raise an exception rather than silently overflowing in encoder

Changes in Version 0.10
-----------------------
- added :meth:`~pymongo.collection.Collection.ensure_index`

Changes in Version 0.9.7
------------------------
- allow sub-collections of *$cmd* as valid
  :class:`~pymongo.collection.Collection` names
- add version as :attr:`pymongo.version`
- add ``--no_ext`` command line option to *setup.py*

.. toctree::
   :hidden:

   python3
   examples/gevent
