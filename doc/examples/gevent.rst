Gevent
===========================

.. testsetup::

  from pymongo import Connection, ReplicaSetConnection

PyMongo supports `Gevent <http://www.gevent.org/>`_. Primarily, this means that
:meth:`~pymongo.connection.Connection.start_request()` can ensure that the
current greenlet (not merely the current thread) uses the same socket for all
operations until :meth:`~pymongo.connection.Connection.end_request()` is called.
See the :doc:`requests documentation <requests>` for details on requests in
PyMongo.

Using Gevent Without Threads
----------------------------

Typically when using Gevent, you will run ``from gevent import monkey;
monkey.patch_all()`` early in your program's execution. From then on, all
thread-related Python functions will act on `greenlets
<http://pypi.python.org/pypi/greenlet>`_ instead of threads, and PyMongo will
treat greenlets as if they were threads transparently. Each greenlet will use a
socket exclusively by default.

.. doctest::

  >>> from gevent import monkey; monkey.patch_all()
  >>> connection = Connection()

Make sure you're using the latest version of Gevent to ensure that
thread-locals are patched to act like greenlet-locals, and be careful to call
patch_all() before loading any other modules.

Using Gevent With Threads
-------------------------

If you need to use standard Python threads in the same process as Gevent and
greenlets, you only need to run ``monkey.patch_socket()``, rather than
``monkey.patch_all()``, and create a
:class:`~pymongo.connection.Connection` with ``use_greenlets=True``.
The :class:`~pymongo.connection.Connection` will use a special greenlet-aware
connection pool that allocates a socket for each greenlet, ensuring consistent
reads in Gevent.

.. doctest::

  >>> from gevent import monkey; monkey.patch_socket()
  >>> connection = Connection(use_greenlets=True)

An instance of :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
created with ``use_greenlets=True`` will also use a greenlet-aware pool.
Additionally, it will use a background greenlet instead of a background thread
to monitor the state of the replica set.

Using :meth:`~pymongo.replica_set_connection.ReplicaSetConnection.start_request()`
with :class:`~pymongo.read_preferences.ReadPreference` PRIMARY ensures that the
current greenlet uses the same socket for all operations until a call to
:meth:`end_request()`.

You must `install Gevent <http://gevent.org/>`_ to use
:class:`~pymongo.replica_set_connection.ReplicaSetConnection`
with ``use_greenlets=True``

.. doctest::

  >>> from gevent import monkey; monkey.patch_socket()
  >>> rsc = ReplicaSetConnection(
  ...     'mongodb://localhost:27017,localhost:27018,localhost:27019',
  ...     replicaSet='repl0', use_greenlets=True)
