PyMongo 3 Migration Guide
=========================

.. contents::

.. testsetup::

  from pymongo import MongoClient, ReadPreference
  client = MongoClient()
  collection = client.my_database.my_collection

PyMongo 3 is a partial rewrite bringing a large number of improvements. It
also brings a number of backward breaking changes. This guide provides a
roadmap for migrating an existing application from PyMongo 2.x to 3.x or
writing libraries that will work with both PyMongo 2.x and 3.x.

PyMongo 2.9
-----------

The first step in any successful migration involves upgrading to, or
requiring, at least PyMongo 2.9. If your project has a
requirements.txt file, add the line "pymongo >= 2.9, < 3.0" until you have
completely migrated to PyMongo 3. Most of the key new
methods and options from PyMongo 3.0 are backported in PyMongo 2.9 making
migration much easier.

Enable Deprecation Warnings
---------------------------

Starting with PyMongo 2.9, :exc:`DeprecationWarning` is raised by most methods
removed in PyMongo 3.0. Make sure you enable runtime warnings to see
where deprecated functions and methods are being used in your application::

  python -Wd <your application>

Warnings can also be changed to errors::

  python -Wd -Werror <your application>

.. note:: Not all deprecated features raise :exc:`DeprecationWarning` when
  used. For example, the :meth:`~pymongo.collection.Collection.find` options
  renamed in PyMongo 3.0 do not raise :exc:`DeprecationWarning` when used in
  PyMongo 2.x. See also `Removed features with no migration path`_.

CRUD API
--------

Changes to find() and find_one()
................................

"spec" renamed "filter"
~~~~~~~~~~~~~~~~~~~~~~~

The `spec` option has been renamed to `filter`. Code like this::

  >>> cursor = collection.find(spec={"a": 1})

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> cursor = collection.find(filter={"a": 1})

or this with any version of PyMongo:

.. doctest::

  >>> cursor = collection.find({"a": 1})

"fields" renamed "projection"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `fields` option has been renamed to `projection`. Code like this::

  >>> cursor = collection.find({"a": 1}, fields={"_id": False})

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> cursor = collection.find({"a": 1}, projection={"_id": False})

or this with any version of PyMongo:

.. doctest::

  >>> cursor = collection.find({"a": 1}, {"_id": False})

"partial" renamed "allow_partial_results"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `partial` option has been renamed to `allow_partial_results`. Code like
this::

  >>> cursor = collection.find({"a": 1}, partial=True)

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> cursor = collection.find({"a": 1}, allow_partial_results=True)

"timeout" replaced by "no_cursor_timeout"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `timeout` option has been replaced by `no_cursor_timeout`. Code like this::

  >>> cursor = collection.find({"a": 1}, timeout=False)

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> cursor = collection.find({"a": 1}, no_cursor_timeout=True)

"network_timeout" is removed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `network_timeout` option has been removed. This option was always the
wrong solution for timing out long running queries and should never be used
in production. Starting with **MongoDB 2.6** you can use the $maxTimeMS query
modifier. Code like this::

  # Set a 5 second select() timeout.
  >>> cursor = collection.find({"a": 1}, network_timeout=5)

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  # Set a 5 second (5000 millisecond) server side query timeout.
  >>> cursor = collection.find({"a": 1}, modifiers={"$maxTimeMS": 5000})

or with PyMongo 3.5 or later:

  >>> cursor = collection.find({"a": 1}, max_time_ms=5000)

or with any version of PyMongo:

.. doctest::

  >>> cursor = collection.find({"$query": {"a": 1}, "$maxTimeMS": 5000})

.. seealso:: `$maxTimeMS
  <http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/>`_

Tailable cursors
~~~~~~~~~~~~~~~~

The `tailable` and `await_data` options have been replaced by `cursor_type`.
Code like this::

  >>> cursor = collection.find({"a": 1}, tailable=True)
  >>> cursor = collection.find({"a": 1}, tailable=True, await_data=True)

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> from pymongo import CursorType
  >>> cursor = collection.find({"a": 1}, cursor_type=CursorType.TAILABLE)
  >>> cursor = collection.find({"a": 1}, cursor_type=CursorType.TAILABLE_AWAIT)

Other removed options
~~~~~~~~~~~~~~~~~~~~~

The `slave_okay`, `read_preference`, `tag_sets`,
and `secondary_acceptable_latency_ms` options have been removed. See the `Read
Preferences`_ section for solutions.

The aggregate method always returns a cursor
............................................

PyMongo 2.6 added an option to return an iterable cursor from
:meth:`~pymongo.collection.Collection.aggregate`. In PyMongo 3
:meth:`~pymongo.collection.Collection.aggregate` always returns a cursor. Use
the `cursor` option for consistent behavior with PyMongo 2.9 and later:

.. doctest::

  >>> for result in collection.aggregate([], cursor={}):
  ...     pass

Read Preferences
----------------

The "slave_okay" option is removed
..................................

The `slave_okay` option is removed from PyMongo's API. The
secondaryPreferred read preference provides the same behavior.
Code like this::

  >>> client = MongoClient(slave_okay=True)

can be changed to this with PyMongo 2.9 or newer:

.. doctest::

  >>> client = MongoClient(readPreference="secondaryPreferred")

The "read_preference" attribute is immutable
............................................

Code like this::

  >>> from pymongo import ReadPreference
  >>> db = client.my_database
  >>> db.read_preference = ReadPreference.SECONDARY

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> db = client.get_database("my_database",
  ...                          read_preference=ReadPreference.SECONDARY)

Code like this::

  >>> cursor = collection.find({"a": 1},
  ...                          read_preference=ReadPreference.SECONDARY)

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> coll2 = collection.with_options(read_preference=ReadPreference.SECONDARY)
  >>> cursor = coll2.find({"a": 1})

.. seealso:: :meth:`~pymongo.database.Database.get_collection`

The "tag_sets" option and attribute are removed
...............................................

The `tag_sets` MongoClient option is removed. The `read_preference`
option can be used instead. Code like this::

  >>> client = MongoClient(
  ...     read_preference=ReadPreference.SECONDARY,
  ...     tag_sets=[{"dc": "ny"}, {"dc": "sf"}])

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> from pymongo.read_preferences import Secondary
  >>> client = MongoClient(read_preference=Secondary([{"dc": "ny"}]))

To change the tags sets for a Database or Collection, code like this::

  >>> db = client.my_database
  >>> db.read_preference = ReadPreference.SECONDARY
  >>> db.tag_sets = [{"dc": "ny"}]

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> db = client.get_database("my_database",
  ...                          read_preference=Secondary([{"dc": "ny"}]))

Code like this::

  >>> cursor = collection.find(
  ...     {"a": 1},
  ...     read_preference=ReadPreference.SECONDARY,
  ...     tag_sets=[{"dc": "ny"}])

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> from pymongo.read_preferences import Secondary
  >>> coll2 = collection.with_options(
  ...     read_preference=Secondary([{"dc": "ny"}]))
  >>> cursor = coll2.find({"a": 1})

.. seealso:: :meth:`~pymongo.database.Database.get_collection`

The "secondary_acceptable_latency_ms" option and attribute are removed
......................................................................

PyMongo 2.x supports `secondary_acceptable_latency_ms` as an option to methods
throughout the driver, but mongos only supports a global latency option.
PyMongo 3.x has changed to match the behavior of mongos, allowing migration
from a single server, to a replica set, to a sharded cluster without a
surprising change in server selection behavior. A new option,
`localThresholdMS`, is available through MongoClient and should be used in
place of `secondaryAcceptableLatencyMS`. Code like this::

  >>> client = MongoClient(readPreference="nearest",
  ...                      secondaryAcceptableLatencyMS=100)

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> client = MongoClient(readPreference="nearest",
  ...                      localThresholdMS=100)

Write Concern
-------------

The "safe" option is removed
............................

In PyMongo 3 the `safe` option is removed from the entire API.
:class:`~pymongo.mongo_client.MongoClient` has always defaulted to acknowledged
write operations and continues to do so in PyMongo 3.

The "write_concern" attribute is immutable
..........................................

The `write_concern` attribute is immutable in PyMongo 3. Code like this::

  >>> client = MongoClient()
  >>> client.write_concern = {"w": "majority"}

can be changed to this with any version of PyMongo:

.. doctest::

  >>> client = MongoClient(w="majority")

Code like this::

  >>> db = client.my_database
  >>> db.write_concern = {"w": "majority"}

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> from pymongo import WriteConcern
  >>> db = client.get_database("my_database",
  ...                          write_concern=WriteConcern(w="majority"))

The new CRUD API write methods do not accept write concern options. Code like
this::

  >>> oid = collection.insert({"a": 2}, w="majority")

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> from pymongo import WriteConcern
  >>> coll2 = collection.with_options(
  ...     write_concern=WriteConcern(w="majority"))
  >>> oid = coll2.insert({"a": 2})

.. seealso:: :meth:`~pymongo.database.Database.get_collection`

Codec Options
-------------

The "document_class" attribute is removed
.........................................

Code like this::

  >>> from bson.son import SON
  >>> client = MongoClient()
  >>> client.document_class = SON

can be replaced by this in any version of PyMongo:

.. doctest::

  >>> from bson.son import SON
  >>> client = MongoClient(document_class=SON)

or to change the `document_class` for a :class:`~pymongo.database.Database`
with PyMongo 2.9 or later:

.. doctest::

  >>> from bson.codec_options import CodecOptions
  >>> from bson.son import SON
  >>> db = client.get_database("my_database", CodecOptions(SON))

.. seealso:: :meth:`~pymongo.database.Database.get_collection` and
  :meth:`~pymongo.collection.Collection.with_options`

The "uuid_subtype" option and attribute are removed
...................................................

Code like this::

  >>> from bson.binary import JAVA_LEGACY
  >>> db = client.my_database
  >>> db.uuid_subtype = JAVA_LEGACY

can be replaced by this with PyMongo 2.9 or later:

.. doctest::

  >>> from bson.binary import JAVA_LEGACY
  >>> from bson.codec_options import CodecOptions
  >>> db = client.get_database("my_database",
  ...                          CodecOptions(uuid_representation=JAVA_LEGACY))

.. seealso:: :meth:`~pymongo.database.Database.get_collection` and
  :meth:`~pymongo.collection.Collection.with_options`

MongoClient
-----------

MongoClient connects asynchronously
...................................

In PyMongo 3, the :class:`~pymongo.mongo_client.MongoClient` constructor no
longer blocks while connecting to the server or servers, and it no longer
raises :exc:`~pymongo.errors.ConnectionFailure` if they are unavailable, nor
:exc:`~pymongo.errors.ConfigurationError` if the user’s credentials are wrong.
Instead, the constructor returns immediately and launches the connection
process on background threads. The `connect` option is added to control whether
these threads are started immediately, or when the client is first used.

For consistent behavior in PyMongo 2.x and PyMongo 3.x, code like this::

  >>> from pymongo.errors import ConnectionFailure
  >>> try:
  ...     client = MongoClient()
  ... except ConnectionFailure:
  ...     print("Server not available")
  >>>

can be changed to this with PyMongo 2.9 or later:

.. doctest::

  >>> from pymongo.errors import ConnectionFailure
  >>> client = MongoClient(connect=False)
  >>> try:
  ...     result = client.admin.command("ping")
  ... except ConnectionFailure:
  ...     print("Server not available")
  >>>

Any operation can be used to determine if the server is available. We choose
the "ping" command here because it is cheap and does not require auth, so
it is a simple way to check whether the server is available.

The max_pool_size parameter is removed
......................................

PyMongo 3 replaced the max_pool_size parameter with support for the MongoDB URI
`maxPoolSize` option. Code like this::

  >>> client = MongoClient(max_pool_size=10)

can be replaced by this with PyMongo 2.9 or later:

.. doctest::

  >>> client = MongoClient(maxPoolSize=10)
  >>> client = MongoClient("mongodb://localhost:27017/?maxPoolSize=10")

The "disconnect" method is removed
..................................

Code like this::

  >>> client.disconnect()

can be replaced by this with PyMongo 2.9 or later:

.. doctest::

  >>> client.close()

The host and port attributes are removed
........................................

Code like this::

  >>> host = client.host
  >>> port = client.port

can be replaced by this with PyMongo 2.9 or later:

.. doctest::

  >>> address = client.address
  >>> host, port = address or (None, None)

BSON
----

"as_class", "tz_aware", and "uuid_subtype" are removed
......................................................

The `as_class`, `tz_aware`, and `uuid_subtype` parameters have been
removed from the functions provided in :mod:`bson`. Furthermore, the
:func:`~bson.encode` and :func:`~bson.decode` functions have been added
as more performant alternatives to the :meth:`bson.BSON.encode` and
:meth:`bson.BSON.decode` methods. Code like this::

  >>> from bson import BSON
  >>> from bson.son import SON
  >>> encoded = BSON.encode({"a": 1}, as_class=SON)

can be replaced by this in PyMongo 2.9 or later:

.. doctest::

  >>> from bson import encode
  >>> from bson.codec_options import CodecOptions
  >>> from bson.son import SON
  >>> encoded = encode({"a": 1}, codec_options=CodecOptions(SON))

Removed features with no migration path
---------------------------------------

MasterSlaveConnection is removed
................................

Master slave deployments are no longer supported by MongoDB. Starting with
MongoDB 3.0 a replica set can have up to 50 members and that limit is likely
to be removed in later releases. We recommend migrating to replica sets
instead.

Requests are removed
....................

The client methods `start_request`, `in_request`, and `end_request` are
removed. Requests were designed to make read-your-writes consistency more
likely with the w=0 write concern. Additionally, a thread in a request used the
same member for all secondary reads in a replica set. To ensure
read-your-writes consistency in PyMongo 3.0, do not override the default write
concern with w=0, and do not override the default read preference of PRIMARY.

The "compile_re" option is removed
..................................

In PyMongo 3 regular expressions are never compiled to Python match objects.

The "use_greenlets" option is removed
.....................................

The `use_greenlets` option was meant to allow use of PyMongo with Gevent
without the use of gevent.monkey.patch_threads(). This option caused a lot
of confusion and made it difficult to support alternative asyncio libraries
like Eventlet. Users of Gevent should use gevent.monkey.patch_all() instead.

.. seealso:: :doc:`examples/gevent`
