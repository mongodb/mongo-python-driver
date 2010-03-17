Changelog
=========

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
- added subtype constants to :mod:`~pymongo.binary` module.
- DEPRECATED `options` argument to
  :meth:`~pymongo.collection.Collection` and
  :meth:`~pymongo.database.Database.create_collection` in favor of
  kwargs.
- added :meth:`~pymongo.has_c` to check for C extension.
- added :meth:`~pymongo.connection.Connection.copy_database`.
- added :data:`~pymongo.cursor.Cursor.alive` to tell when a cursor
  might have more data to return (useful for tailable cursors).
- added :class:`~pymongo.timestamp.Timestamp` to better support
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
  :mod:`~pymongo.json_util`.
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
  or :class:`~pymongo.errors.InvalidBSON` being a :class:`ValueError`,
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
- removed :meth:`~pymongo.son.SON.from_xml` as it was only being used
  for some internal testing - also eliminates dependency on
  :mod:`elementtree`.
- implementation of :meth:`~pymongo.message.update` in C.
- deprecate :meth:`~pymongo.database.Database._command` in favor of
  :meth:`~pymongo.database.Database.command`.
- send all commands without wrapping as ``{"query": ...}``.
- support string as `key` argument to
  :meth:`~pymongo.collection.Collection.group` (keyf) and run all
  groups as commands.
- support for equality testing for :class:`~pymongo.code.Code`
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
- raise :class:`~pymongo.errors.InvalidId` instead of
  :class:`TypeError` when passing a 24 character string to
  :class:`~pymongo.objectid.ObjectId` that contains non-hexadecimal
  characters
- allow :class:`unicode` instances for :class:`~pymongo.objectid.ObjectId` init

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
- allow any UTF-8 string in :class:`~pymongo.bson.BSON` encoder, not
  just ASCII subset
- added :attr:`~pymongo.objectid.ObjectId.generation_time`
- removed support for legacy :class:`~pymongo.objectid.ObjectId`
  format - pretty sure this was never used, and is just confusing
- DEPRECATED :meth:`~pymongo.objectid.ObjectId.url_encode` and
  :meth:`~pymongo.objectid.ObjectId.url_decode` in favor of :meth:`str`
  and :meth:`~pymongo.objectid.ObjectId`, respectively
- allow *oplog.$main* as a valid collection name
- some minor fixes for installation process
- added support for datetime and regex in :mod:`~pymongo.json_util`

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
- added `database` support for :class:`~pymongo.dbref.DBRef`
- added :mod:`~pymongo.json_util` with helpers for encoding / decoding
  special types to JSON
- DEPRECATED :meth:`pymongo.cursor.Cursor.__len__` in favor of
  :meth:`~pymongo.cursor.Cursor.count` with `with_limit_and_skip` set
  to ``True`` due to performance regression
- switch documentation to Sphinx

Changes in Version 1.1
----------------------
- added :meth:`__hash__` for :class:`~pymongo.dbref.DBRef` and
  :class:`~pymongo.objectid.ObjectId`
- bulk :meth:`~pymongo.collection.Collection.insert` works with any
  iterable
- fix :class:`~pymongo.objectid.ObjectId` generation when using
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
- fix string representation of :class:`~pymongo.objectid.ObjectId`
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
- support for long in :class:`~pymongo.bson.BSON`
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
- improved :class:`~pymongo.objectid.ObjectId` generation
- added :class:`~pymongo.errors.AutoReconnect` exception for when
  reconnection is possible
- make :mod:`gridfs` thread-safe
- fix for :mod:`gridfs` with non :class:`~pymongo.objectid.ObjectId` ``_id``

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
