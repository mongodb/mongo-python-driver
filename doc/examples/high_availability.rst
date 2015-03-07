High Availability and PyMongo
=============================

PyMongo makes it easy to write highly available applications whether
you use a `single replica set <http://dochub.mongodb.org/core/rs>`_
or a `large sharded cluster
<http://www.mongodb.org/display/DOCS/Sharding+Introduction>`_.

Connecting to a Replica Set
---------------------------

PyMongo makes working with `replica sets
<http://dochub.mongodb.org/core/rs>`_ easy. Here we'll launch a new
replica set and show how to handle both initialization and normal
connections with PyMongo.

.. mongodoc:: rs

Starting a Replica Set
~~~~~~~~~~~~~~~~~~~~~~

The main `replica set documentation
<http://dochub.mongodb.org/core/rs>`_ contains extensive information
about setting up a new replica set or migrating an existing MongoDB
setup, be sure to check that out. Here, we'll just do the bare minimum
to get a three node replica set setup locally.

.. warning:: Replica sets should always use multiple nodes in
   production - putting all set members on the same physical node is
   only recommended for testing and development.

We start three ``mongod`` processes, each on a different port and with
a different dbpath, but all using the same replica set name "foo".

.. code-block:: bash

  $ mkdir -p /data/db0 /data/db1 /data/db2
  $ mongod --port 27017 --dbpath /data/db0 --replSet foo

.. code-block:: bash

  $ mongod --port 27018 --dbpath /data/db1 --replSet foo

.. code-block:: bash

  $ mongod --port 27019 --dbpath /data/db2 --replSet foo

Initializing the Set
~~~~~~~~~~~~~~~~~~~~

At this point all of our nodes are up and running, but the set has yet
to be initialized. Until the set is initialized no node will become
the primary, and things are essentially "offline".

To initialize the set we need to connect to a single node and run the
initiate command::

  >>> from pymongo import MongoClient
  >>> c = MongoClient('localhost', 27017)

.. note:: We could have connected to any of the other nodes instead,
   but only the node we initiate from is allowed to contain any
   initial data.

After connecting, we run the initiate command to get things started::

  >>> config = {'_id': 'foo', 'members': [
  ...     {'_id': 0, 'host': 'localhost:27017'},
  ...     {'_id': 1, 'host': 'localhost:27018'},
  ...     {'_id': 2, 'host': 'localhost:27019'}]}
  >>> c.admin.command("replSetInitiate", config)
  {'info': 'Config now saved locally.  Should come online in about a minute.', 'ok': 1.0}

The three ``mongod`` servers we started earlier will now coordinate
and come online as a replica set.

Connecting to a Replica Set
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The initial connection as made above is a special case for an
uninitialized replica set. Normally we'll want to connect
differently. A connection to a replica set can be made using the
:meth:`~pymongo.mongo_client.MongoClient` constructor, specifying
one or more members of the set, along with the replica set name. Any of
the following connects to the replica set we just created::

  >>> MongoClient('localhost', replicaset='foo')
  MongoClient('localhost', 27017)
  >>> MongoClient('localhost:27018', replicaset='foo')
  MongoClient('localhost', 27018)
  >>> MongoClient('localhost', 27019, replicaset='foo')
  MongoClient('localhost', 27019)
  >>> MongoClient('mongodb://localhost:27017,localhost:27018/?replicaSet=foo')
  MongoClient(['localhost:27017', 'localhost:27018'])

The nodes passed to :meth:`~pymongo.mongo_client.MongoClient` are called
the *seeds*. As long as at least one of the seeds is online, MongoClient
discovers all the members in the replica set, and determines which is the
current primary and which are secondaries.

The :class:`~pymongo.mongo_client.MongoClient` constructor is non-blocking:
the constructor returns immediately while the client connects to the replica
set using background threads. Note how, if you create a client and immediately
print its string representation, the client only prints the single host it
knows about. If you wait a moment, it to discovers the whole replica set:

  >>> from time import sleep
  >>> c = MongoClient(replicaset='foo'); print c; sleep(0.1); print c
  MongoClient('localhost', 27017)
  MongoClient([u'localhost:27019', u'localhost:27017', u'localhost:27018'])

You need not wait for replica set discovery in your application, however.
If you need to do any operation with a MongoClient, such as a
:meth:`~pymongo.collection.Collection.find` or an
:meth:`~pymongo.collection.Collection.insert_one`, the client waits to discover
a suitable member before it attempts the operation.

Handling Failover
~~~~~~~~~~~~~~~~~

When a failover occurs, PyMongo will automatically attempt to find the
new primary node and perform subsequent operations on that node. This
can't happen completely transparently, however. Here we'll perform an
example failover to illustrate how everything behaves. First, we'll
connect to the replica set and perform a couple of basic operations::

  >>> db = MongoClient("localhost", replicaSet='foo').test
  >>> db.test.insert_one({"x": 1})
  ObjectId('...')
  >>> db.test.find_one()
  {u'x': 1, u'_id': ObjectId('...')}

By checking the host and port, we can see that we're connected to
*localhost:27017*, which is the current primary::

  >>> db.client.host
  'localhost'
  >>> db.client.port
  27017

Now let's bring down that node and see what happens when we run our
query again::

  >>> db.test.find_one()
  Traceback (most recent call last):
  pymongo.errors.AutoReconnect: ...

We get an :class:`~pymongo.errors.AutoReconnect` exception. This means
that the driver was not able to connect to the old primary (which
makes sense, as we killed the server), but that it will attempt to
automatically reconnect on subsequent operations. When this exception
is raised our application code needs to decide whether to retry the
operation or to simply continue, accepting the fact that the operation
might have failed.

On subsequent attempts to run the query we might continue to see this
exception. Eventually, however, the replica set will failover and
elect a new primary (this should take a couple of seconds in
general). At that point the driver will connect to the new primary and
the operation will succeed::

  >>> db.test.find_one()
  {u'x': 1, u'_id': ObjectId('...')}
  >>> db.client.host
  'localhost'
  >>> db.client.port
  27018

Bring the former primary back up. It will rejoin the set as a secondary.
Now we can move to the next section: distributing reads to secondaries.

.. _secondary-reads:

Secondary Reads
~~~~~~~~~~~~~~~

By default an instance of MongoClient sends queries to
the primary member of the replica set. To use secondaries for queries
we have to change the :class:`~pymongo.read_preferences.ReadPreference`::

  >>> from pymongo.read_preferences import ReadPreference
  >>> client = MongoClient(
  ...     'localhost:27017',
  ...     replicaSet='foo',
  ...     read_preference=ReadPreference.SECONDARY_PREFERRED)
  >>> db = client.test

Now all queries will be sent to the secondary members of the set. If there are
no secondary members the primary will be used as a fallback. If you have
queries you would prefer to never send to the primary you can specify that
using the ``SECONDARY`` read preference.

The Read preference can be set when you create a
:class:`~pymongo.mongo_client.MongoClient`, or when you execute a
:meth:`~pymongo.collection.Collection.find`,
:meth:`~pymongo.collection.Collection.find_one`,
or a command::

  >>> db.test.find_one(read_preference=ReadPreference.PRIMARY)
  {u'x': 1, u'_id': ObjectId('...')}
  >>> for doc in db.test.find(read_preference=ReadPreference.SECONDARY):
  ...     print doc
  {u'x': 1, u'_id': ObjectId('...')}
  >>> db.command('dbstats', read_preference=ReadPreference.NEAREST)
  {...}

Reads are configured using three options: **read_preference**, **tag_sets**,
and **local_threshold_ms**.

**read_preference**:

- ``PRIMARY``: Read from the primary. This is the default, and provides the
  strongest consistency. If no primary is available, raise
  :class:`~pymongo.errors.AutoReconnect`.

- ``PRIMARY_PREFERRED``: Read from the primary if available, or if there is
  none, read from a secondary matching your choice of ``tag_sets`` and
  ``local_threshold_ms``.

- ``SECONDARY``: Read from a secondary matching your choice of ``tag_sets`` and
  ``local_threshold_ms``. If no matching secondary is available,
  raise :class:`~pymongo.errors.AutoReconnect`.

- ``SECONDARY_PREFERRED``: Read from a secondary matching your choice of
  ``tag_sets`` and ``local_threshold_ms`` if available, otherwise
  from primary (regardless of the primary's tags and local threshold).

- ``NEAREST``: Read from any member matching your choice of ``tag_sets`` and
  ``local_threshold_ms``.

**tag_sets**:

Replica-set members can be `tagged
<http://www.mongodb.org/display/DOCS/Data+Center+Awareness>`_ according to any
criteria you choose. By default, PyMongo ignores tags when
choosing a member to read from, but your read preference can be configured with
a ``tag_sets`` parameter. ``tag_sets`` must be a list of dictionaries, each
dict providing tag values that the replica set member must match.
PyMongo tries each set of tags in turn until it finds a set of
tags with at least one matching member. For example, to prefer reads from the
New York data center, but fall back to the San Francisco data center, tag your
replica set members according to their location and create a
MongoClient like so:

  >>> from pymongo.read_preferences import Secondary
  >>> rsc = MongoClient(
  ...     'localhost:27017',
  ...     replicaSet='foo'
  ...     read_preference=Secondary(tag_sets=[{'dc': 'ny'}, {'dc': 'sf'}])
  ... )

MongoClient tries to find secondaries in New York, then San Francisco,
and raises :class:`~pymongo.errors.AutoReconnect` if none are available. As an
additional fallback, specify a final, empty tag set, ``{}``, which means "read
from any member that matches the mode, ignoring tags."

See :mod:`~pymongo.read_preferences` for more information.

.. _distributes reads to secondaries:

**local_threshold_ms**:

If multiple members match the mode and tag sets, PyMongo reads
from among the nearest members, chosen according to ping time. By default,
only members whose ping times are within 15 milliseconds of the nearest
are used for queries. You can choose to distribute reads among members with
higher latencies by setting ``local_threshold_ms`` to a larger
number. In that case, PyMongo distributes reads among matching
members within ``local_threshold_ms`` of the closest member's
ping time.

.. note:: ``local_threshold_ms`` is ignored when talking to a
  replica set *through* a mongos. The equivalent is the localThreshold_ command
  line option.

.. _localThreshold: http://docs.mongodb.org/manual/reference/mongos/#cmdoption--localThreshold

Health Monitoring
'''''''''''''''''

When MongoClient is initialized it launches background threads to
monitor the replica set for changes in:

* Health: detect when a member goes down or comes up, or if a different member
  becomes primary
* Configuration: detect when members are added or removed, and detect changes
  in members' tags
* Latency: track a moving average of each member's ping time

Replica-set monitoring ensures queries are continually routed to the proper
members as the state of the replica set changes.

.. _mongos-load-balancing:

mongos Load Balancing
---------------------

An instance of :class:`~pymongo.mongo_client.MongoClient` can be configured
with a list of mongos servers:

  >>> client = MongoClient('mongodb://host1,host2,host3')

Each member of the list must be a mongos server. The client continuously
monitors all the mongoses' availability, and its network latency to each.

PyMongo distributes operations evenly among the set of mongoses within its
``localThresholdMS`` (similar to how it `distributes reads to secondaries`_
in a replica set). By default the threshold is 15 ms.

The lowest-latency server, and all servers with latencies no more than
``localThresholdMS`` beyond the lowest-latency server's, receive
operations equally. For example, if we have three mongoses:

  - host1: 20 ms
  - host2: 35 ms
  - host3: 40 ms

By default the ``localThresholdMS`` is 15 ms, so PyMongo uses host1 and host2
evenly. It uses host1 because its network latency to the driver is shortest. It
uses host2 because its latency is within 15 ms of the lowest-latency server's.
But it excuses host3: host3 is 20ms beyond the lowest-latency server.

If we set ``localThresholdMS`` to 30 ms all servers are within the threshold:

  >>> client = MongoClient('mongodb://host1,host2,host3/?localThresholdMS=30')
