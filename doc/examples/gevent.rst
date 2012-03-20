Gevent
===========================

.. testsetup::

  from pymongo import Connection

PyMongo supports `Gevent <http://www.gevent.org/>`_. Primarily, this means that
:meth:`pymongo.connection.Connection.start_request()` can ensure that the
current greenlet (not merely the current thread) uses the same socket for all
operations until `:meth:pymongo.connection.Connection.end_request()`. See the
:doc:`requests documentation <requests>` for details.

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
thread-locals are patched to act like greenlet-locals.

Using Gevent With Threads
-------------------------

If you need to use standard Python threads in the same process as Gevent and
greenlets, you can run only ``monkey.patch_socket()``, and create a
:class:`pymongo.connection.Connection` with ``use_greenlets=True``. The
:class:`pymongo.connection.Connection` will use a special greenlet-aware
connection pool that allocates a socket for each greenlet, ensuring consistent
reads in Gevent.

.. doctest::

  >>> from gevent import monkey; monkey.patch_socket()
  >>> connection = Connection(use_greenlets=True)

