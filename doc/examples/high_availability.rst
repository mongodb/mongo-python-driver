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

.. note:: Replica sets require server version **>= 1.6.0**. Support
   for connecting to replica sets also requires PyMongo version **>=
   1.8.0**.

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
a different dbpath, but all using the same replica set name "foo". In
the example we use the hostname "morton.local", so replace that with
your hostname when running:

.. code-block:: bash

  $ hostname
  morton.local
  $ mongod --replSet foo/morton.local:27018,morton.local:27019 --rest

.. code-block:: bash

  $ mongod --port 27018 --dbpath /data/db1 --replSet foo/morton.local:27017 --rest

.. code-block:: bash

  $ mongod --port 27019 --dbpath /data/db2 --replSet foo/morton.local:27017 --rest

Initializing the Set
~~~~~~~~~~~~~~~~~~~~

At this point all of our nodes are up and running, but the set has yet
to be initialized. Until the set is initialized no node will become
the primary, and things are essentially "offline".

To initialize the set we need to connect to a single node and run the
initiate command. Since we don't have a primary yet, we'll need to
tell PyMongo that it's okay to connect to a slave/secondary::

  >>> from pymongo import MongoClient, ReadPreference
  >>> c = MongoClient("morton.local:27017",
                      read_preference=ReadPreference.SECONDARY)

.. note:: We could have connected to any of the other nodes instead,
   but only the node we initiate from is allowed to contain any
   initial data.

After connecting, we run the initiate command to get things started
(here we just use an implicit configuration, for more advanced
configuration options see the replica set documentation)::

  >>> c.admin.command("replSetInitiate")
  {u'info': u'Config now saved locally.  Should come online in about a minute.',
   u'info2': u'no configuration explicitly specified -- making one', u'ok': 1.0}

The three ``mongod`` servers we started earlier will now coordinate
and come online as a replica set.

Connecting to a Replica Set
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The initial connection as made above is a special case for an
uninitialized replica set. Normally we'll want to connect
differently. A connection to a replica set can be made using the
normal :meth:`~pymongo.mongo_client.MongoClient` constructor, specifying
one or more members of the set. For example, any of the following
will create a connection to the set we just created::

  >>> MongoClient("morton.local", replicaset='foo')
  MongoClient([u'morton.local:27019', 'morton.local:27017', u'morton.local:27018'])
  >>> MongoClient("morton.local:27018", replicaset='foo')
  MongoClient([u'morton.local:27019', u'morton.local:27017', 'morton.local:27018'])
  >>> MongoClient("morton.local", 27019, replicaset='foo')
  MongoClient(['morton.local:27019', u'morton.local:27017', u'morton.local:27018'])
  >>> MongoClient(["morton.local:27018", "morton.local:27019"])
  MongoClient(['morton.local:27019', u'morton.local:27017', 'morton.local:27018'])
  >>> MongoClient("mongodb://morton.local:27017,morton.local:27018,morton.local:27019")
  MongoClient(['morton.local:27019', 'morton.local:27017', 'morton.local:27018'])

The nodes passed to :meth:`~pymongo.mongo_client.MongoClient` are called
the *seeds*. If only one host is specified the `replicaset` parameter
must be used to indicate this isn't a connection to a single node.
As long as at least one of the seeds is online, the driver will be able
to "discover" all of the nodes in the set and make a connection to the
current primary.

Handling Failover
~~~~~~~~~~~~~~~~~

When a failover occurs, PyMongo will automatically attempt to find the
new primary node and perform subsequent operations on that node. This
can't happen completely transparently, however. Here we'll perform an
example failover to illustrate how everything behaves. First, we'll
connect to the replica set and perform a couple of basic operations::

  >>> db = MongoClient("morton.local", replicaSet='foo').test
  >>> db.test.save({"x": 1})
  ObjectId('...')
  >>> db.test.find_one()
  {u'x': 1, u'_id': ObjectId('...')}

By checking the host and port, we can see that we're connected to
*morton.local:27017*, which is the current primary::

  >>> db.connection.host
  'morton.local'
  >>> db.connection.port
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
  >>> db.connection.host
  'morton.local'
  >>> db.connection.port
  27018

MongoReplicaSetClient
~~~~~~~~~~~~~~~~~~~~~

Using a :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` instead
of a simple :class:`~pymongo.mongo_client.MongoClient` offers two key features:
secondary reads and replica set health monitoring. To connect using
:class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` just provide a
host:port pair and the name of the replica set::

  >>> from pymongo import MongoReplicaSetClient
  >>> MongoReplicaSetClient("morton.local:27017", replicaSet='foo')
  MongoReplicaSetClient([u'morton.local:27019', u'morton.local:27017', u'morton.local:27018'])

.. _secondary-reads:

Secondary Reads
'''''''''''''''

By default an instance of MongoReplicaSetClient will only send queries to
the primary member of the replica set. To use secondaries for queries
we have to change the :class:`~pymongo.read_preferences.ReadPreference`::

  >>> db = MongoReplicaSetClient("morton.local:27017", replicaSet='foo').test
  >>> from pymongo.read_preferences import ReadPreference
  >>> db.read_preference = ReadPreference.SECONDARY_PREFERRED

Now all queries will be sent to the secondary members of the set. If there are
no secondary members the primary will be used as a fallback. If you have
queries you would prefer to never send to the primary you can specify that
using the ``SECONDARY`` read preference::

  >>> db.read_preference = ReadPreference.SECONDARY

Read preference can be set on a client, database, collection, or on a
per-query basis, e.g.::

  >>> db.collection.find_one(read_preference=ReadPreference.PRIMARY)

Reads are configured using three options: **read_preference**, **tag_sets**,
and **secondary_acceptable_latency_ms**.

**read_preference**:

- ``PRIMARY``: Read from the primary. This is the default, and provides the
  strongest consistency. If no primary is available, raise
  :class:`~pymongo.errors.AutoReconnect`.

- ``PRIMARY_PREFERRED``: Read from the primary if available, or if there is
  none, read from a secondary matching your choice of ``tag_sets`` and
  ``secondary_acceptable_latency_ms``.

- ``SECONDARY``: Read from a secondary matching your choice of ``tag_sets`` and
  ``secondary_acceptable_latency_ms``. If no matching secondary is available,
  raise :class:`~pymongo.errors.AutoReconnect`.

- ``SECONDARY_PREFERRED``: Read from a secondary matching your choice of
  ``tag_sets`` and ``secondary_acceptable_latency_ms`` if available, otherwise
  from primary (regardless of the primary's tags and latency).

- ``NEAREST``: Read from any member matching your choice of ``tag_sets`` and
  ``secondary_acceptable_latency_ms``.

**tag_sets**:

Replica-set members can be `tagged
<http://www.mongodb.org/display/DOCS/Data+Center+Awareness>`_ according to any
criteria you choose. By default, MongoReplicaSetClient ignores tags when
choosing a member to read from, but it can be configured with the ``tag_sets``
parameter. ``tag_sets`` must be a list of dictionaries, each dict providing tag
values that the replica set member must match. MongoReplicaSetClient tries each
set of tags in turn until it finds a set of tags with at least one matching
member. For example, to prefer reads from the New York data center, but fall
back to the San Francisco data center, tag your replica set members according
to their location and create a MongoReplicaSetClient like so:

  >>> rsc = MongoReplicaSetClient(
  ...     "morton.local:27017",
  ...     replicaSet='foo'
  ...     read_preference=ReadPreference.SECONDARY,
  ...     tag_sets=[{'dc': 'ny'}, {'dc': 'sf'}]
  ... )

MongoReplicaSetClient tries to find secondaries in New York, then San Francisco,
and raises :class:`~pymongo.errors.AutoReconnect` if none are available. As an
additional fallback, specify a final, empty tag set, ``{}``, which means "read
from any member that matches the mode, ignoring tags."

**secondary_acceptable_latency_ms**:

If multiple members match the mode and tag sets, MongoReplicaSetClient reads
from among the nearest members, chosen according to ping time. By default,
only members whose ping times are within 15 milliseconds of the nearest
are used for queries. You can choose to distribute reads among members with
higher latencies by setting ``secondary_acceptable_latency_ms`` to a larger
number. In that case, MongoReplicaSetClient distributes reads among matching
members within ``secondary_acceptable_latency_ms`` of the closest member's
ping time.

.. note:: ``secondary_acceptable_latency_ms`` is ignored when talking to a
  replica set *through* a mongos. The equivalent is the localThreshold_ command
  line option.

.. _localThreshold: http://docs.mongodb.org/manual/reference/mongos/#cmdoption-mongos--localThreshold

Health Monitoring
'''''''''''''''''

When MongoReplicaSetClient is initialized it launches a background task to
monitor the replica set for changes in:

* Health: detect when a member goes down or comes up, or if a different member
  becomes primary
* Configuration: detect changes in tags
* Latency: track a moving average of each member's ping time

Replica-set monitoring ensures queries are continually routed to the proper
members as the state of the replica set changes.

It is critical to call
:meth:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient.close` to terminate
the monitoring task before your process exits.

.. _mongos-high-availability:

High Availability and mongos
----------------------------

An instance of :class:`~pymongo.mongo_client.MongoClient` can be configured
to automatically connect to a different mongos if the instance it is
currently connected to fails. If a failure occurs, PyMongo will attempt
to find the nearest mongos to perform subsequent operations. As with a
replica set this can't happen completely transparently, Here we'll perform
an example failover to illustrate how everything behaves. First, we'll
connect to a sharded cluster, using a seed list, and perform a couple of
basic operations::

  >>> db = MongoClient('morton.local:30000,morton.local:30001,morton.local:30002').test
  >>> db.test.save({"x": 1})
  ObjectId('...')
  >>> db.test.find_one()
  {u'x': 1, u'_id': ObjectId('...')}

Each member of the seed list passed to MongoClient must be a mongos. By checking
the host, port, and is_mongos attributes we can see that we're connected to
*morton.local:30001*, a mongos::

  >>> db.connection.host
  'morton.local'
  >>> db.connection.port
  30001
  >>> db.connection.is_mongos
  True

Now let's shut down that mongos instance and see what happens when we run our
query again::

  >>> db.test.find_one()
  Traceback (most recent call last):
  pymongo.errors.AutoReconnect: ...

As in the replica set example earlier in this document, we get
an :class:`~pymongo.errors.AutoReconnect` exception. This means
that the driver was not able to connect to the original mongos at port
30001 (which makes sense, since we shut it down), but that it will
attempt to connect to a new mongos on subsequent operations. When this
exception is raised our application code needs to decide whether to retry
the operation or to simply continue, accepting the fact that the operation
might have failed.

As long as one of the seed list members is still available the next
operation will succeed::

  >>> db.test.find_one()
  {u'x': 1, u'_id': ObjectId('...')}
  >>> db.connection.host
  'morton.local'
  >>> db.connection.port
  30002
  >>> db.connection.is_mongos
  True
