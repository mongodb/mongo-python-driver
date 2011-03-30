Changelog
=========

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

- PYTHON-225: :class:`pymongo.objectid.ObjectId` class definition should use __slots__.
- PYTHON-223: Documentation fix.
- PYTHON-220: Documentation fix.
- PYTHON-219: KeyError in :meth:`~pymongo.collection.Collection.find_and_modify`
- PYTHON-213: Query server for maximum BSON document size.
- PYTHON-208: Fix :class:`pymongo.connection.Connection` __repr__.
- PYTHON-207: Changes to Map/Reduce API.
- PYTHON-205: Accept slaveOk in the URI to match the URI docs.
- PYTHON-203: When slave_okay=True and we only specify one host don't autodetect other set members.
- PYTHON-194: Show size when whining about a document being too large.
- PYTHON-184: Raise :class:`pymongo.errors.DuplicateKeyError` for duplicate keys in capped collections.
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

.. note:: All of the renamed modules and exceptions above have aliases
   created with the old names, so these changes should not break
   existing code. The old names will eventually be deprecated and then
   removed, so users should begin migrating towards the new names now.

.. warning:: The change to the exception hierarchy mentioned above is
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
  :doc:`example <examples/map_reduce>`
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
