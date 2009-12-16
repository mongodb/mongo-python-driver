Changelog
=========

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
