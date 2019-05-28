Gevent
======

PyMongo supports `Gevent <http://www.gevent.org/>`_. Simply call Gevent's
``monkey.patch_all()`` before loading any other modules:

.. doctest::

  >>> # You must call patch_all() *before* importing any other modules
  >>> from gevent import monkey
  >>> _ = monkey.patch_all()
  >>> from pymongo import MongoClient
  >>> client = MongoClient()

PyMongo uses thread and socket functions from the Python standard library.
Gevent's monkey-patching replaces those standard functions so that PyMongo
does asynchronous I/O with non-blocking sockets, and schedules operations
on greenlets instead of threads.

Avoid blocking in Hub.join
--------------------------

By default, PyMongo uses threads to discover and monitor your servers' topology
(see :ref:`health-monitoring`). If you execute ``monkey.patch_all()`` when
your application first begins, PyMongo automatically uses greenlets instead
of threads.

When shutting down, if your application calls :meth:`~gevent.hub.Hub.join` on
Gevent's :class:`~gevent.hub.Hub` without first terminating these background
greenlets, the call to :meth:`~gevent.hub.Hub.join` blocks indefinitely. You
therefore **must close or dereference** any active
:class:`~pymongo.mongo_client.MongoClient` before exiting.

An example solution to this issue in some application frameworks is a signal
handler to end background greenlets when your application receives SIGHUP:

.. code-block:: python

    import signal

    def graceful_reload(signum, traceback):
        """Explicitly close some global MongoClient object."""
        client.close()

    signal.signal(signal.SIGHUP, graceful_reload)

Applications using uWSGI prior to 1.9.16 are affected by this issue,
or newer uWSGI versions with the ``-gevent-wait-for-hub`` option.
See `the uWSGI changelog for details
<https://uwsgi-docs.readthedocs.io/en/latest/Changelog-1.9.16.html#important-change-in-the-gevent-plugin-shutdown-reload-procedure>`_.
