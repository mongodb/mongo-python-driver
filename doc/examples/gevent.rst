Gevent
======

PyMongo supports `Gevent <http://www.gevent.org/>`_. Simply call Gevent's
``monkey.patch_all()`` before loading any other modules:

.. doctest::

  >>> # You must call patch_all() *before* importing any other modules
  >>> from gevent import monkey; monkey.patch_all()
  >>> from pymongo import MongoClient
  >>> client = MongoClient()

PyMongo's Gevent support means
that :meth:`~pymongo.mongo_client.MongoClient.start_request()` ensures the
current greenlet (not merely the current thread) uses the same socket for all
operations until :meth:`~pymongo.mongo_client.MongoClient.end_request()` is called.
See the :doc:`requests documentation <requests>` for details on requests in
PyMongo.

Using Gevent With Threads
-------------------------

If you need to use standard Python threads in the same process as Gevent and
greenlets, run ``monkey.patch_socket()``, rather than
``monkey.patch_all()``, and create a
:class:`~pymongo.mongo_client.MongoClient` with ``use_greenlets=True``.
The :class:`~pymongo.mongo_client.MongoClient` will use a special greenlet-aware
connection pool.

.. doctest::

  >>> from gevent import monkey; monkey.patch_socket()
  >>> from pymongo import MongoClient
  >>> client = MongoClient(use_greenlets=True)

An instance of :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
created with ``use_greenlets=True`` will also use a greenlet-aware pool.
Additionally, it will use a background greenlet instead of a background thread
to monitor the state of the replica set.

.. doctest::

  >>> from gevent import monkey; monkey.patch_socket()
  >>> from pymongo.mongo_replica_set_client import MongoReplicaSetClient
  >>> rsc = MongoReplicaSetClient(
  ...     'mongodb://localhost:27017,localhost:27018,localhost:27019',
  ...     replicaSet='repl0', use_greenlets=True)

Setting ``use_greenlets`` is unnecessary under normal circumstances; simply call
``patch_all`` to use Gevent with PyMongo.