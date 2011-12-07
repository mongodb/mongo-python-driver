Connecting to a Replica Set
===========================

PyMongo makes working with `replica sets
<http://www.mongodb.org/display/DOCS/Replica+Sets>`_ easy. Here we'll launch
a new replica set and show how to handle both initialization and normal
connections with PyMongo.

.. note:: Replica sets require server version **>= 1.6.0**. Support
   for connecting to replica sets also requires PyMongo version **>=
   1.8.0**.

.. mongodoc:: Replica+Sets

Starting a Replica Set
----------------------

The main `replica set documentation
<http://www.mongodb.org/display/DOCS/Replica+Sets>`_ contains extensive information
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
--------------------

At this point all of our nodes are up and running, but the set has yet
to be initialized. Until the set is initialized no node will become
the primary, and things are essentially "offline".

To initialize the set we need to connect to a single node and run the
initiate command. Since we don't have a primary yet, we'll need to
tell PyMongo that it's okay to connect to a slave/secondary::

  >>> from pymongo import Connection
  >>> c = Connection("morton.local:27017", slave_okay=True)

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
---------------------------

The initial connection as made above is a special case for an
uninitialized replica set. Normally we'll want to connect
differently. A connection to a replica set can be made using the
normal :meth:`~pymongo.connection.Connection` constructor, specifying
one or more members of the set. For example, any of the following
will create a connection to the set we just created::

  >>> Connection("morton.local", replicaset='foo')
  Connection([u'morton.local:27019', 'morton.local:27017', u'morton.local:27018'])
  >>> Connection("morton.local:27018", replicaset='foo')
  Connection([u'morton.local:27019', u'morton.local:27017', 'morton.local:27018'])
  >>> Connection("morton.local", 27019, replicaset='foo')
  Connection(['morton.local:27019', u'morton.local:27017', u'morton.local:27018'])
  >>> Connection(["morton.local:27018", "morton.local:27019"])
  Connection(['morton.local:27019', u'morton.local:27017', 'morton.local:27018'])
  >>> Connection("mongodb://morton.local:27017,morton.local:27018,morton.local:27019")
  Connection(['morton.local:27019', 'morton.local:27017', 'morton.local:27018'])

The nodes passed to :meth:`~pymongo.connection.Connection` are called
the *seeds*. If only one host is specified the `replicaset` parameter
must be used to indicate this isn't a connection to a single node.
As long as at least one of the seeds is online, the driver will be able
to "discover" all of the nodes in the set and make a connection to the
current primary.

Handling Failover
-----------------

When a failover occurs, PyMongo will automatically attempt to find the
new primary node and perform subsequent operations on that node. This
can't happen completely transparently, however. Here we'll perform an
example failover to illustrate how everything behaves. First, we'll
connect to the replica set and perform a couple of basic operations::

  >>> db = Connection("morton.local", replicaSet='foo').test
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
  u'morton.local'
  >>> db.connection.port
  27018

ReplicaSetConnection
--------------------

In Pymongo-2.1 a new ReplicaSetConnection class was added that provides
some new features not supported in the original Connection class. The most
important of these is the ability to distribute queries to the secondary
members of a replica set. To connect using ReplicaSetConnection just
provide a host:port pair and the name of the replica set::

  >>> from pymongo import ReplicaSetConnection
  >>> ReplicaSetConnection("morton.local:27017", replicaSet='foo')
  ReplicaSetConnection([u'morton.local:27019', u'morton.local:27017', u'morton.local:27018'])

By default an instance of ReplicaSetConnection will only send queries to
the primary member of the replica set. To use secondary members for queries
we have to change the read preference::

  >>> db = ReplicaSetConnection("morton.local:27017", replicaSet='foo').test
  >>> from pymongo import ReadPreference
  >>> db.read_preference = ReadPreference.SECONDARY

Now all queries will be sent to the secondary members of the set. If there are
no secondary members the primary will be used as a fallback. If you have
queries you would prefer to never send to the primary you can specify that
using the SECONDARY_ONLY read preference::

  >>> db.read_preference = ReadPreference.SECONDARY_ONLY

Read preference can be set on a connection, database, collection, or on a
per-query basis.

