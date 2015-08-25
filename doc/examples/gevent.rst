Gevent
======

PyMongo supports `Gevent <http://www.gevent.org/>`_. Simply call Gevent's
``monkey.patch_all()`` before loading any other modules:

.. doctest::

  >>> # You must call patch_all() *before* importing any other modules
  >>> from gevent import monkey
  >>> monkey.patch_all()
  >>> from pymongo import MongoClient
  >>> client = MongoClient()

PyMongo uses thread and socket functions from the Python standard library.
Gevent's monkey-patching replaces those standard functions so that PyMongo
does asynchronous I/O with non-blocking sockets, and schedules operations
on greenlets instead of threads.

Consideration on gevent's hub termination
-----------------------------------------
PyMongo uses threads to monitor your servers' topology and to handle
graceful reconnections.

When shutting down your application gracefully, your code be may waiting
for any remaining greenlet to terminate before exiting.

This may become a **blocking operation** because monkey-patched threads
are considered as greenlets and this could prevent your application
to exit properly. You therefore **must close or dereference** any active
MongoClient object before exiting !

Code example, this function is called when your application is asked
to exit or gracefully reload itself by receiving a SIGHUP signal:

.. code-block:: python

    import signal

    def graceful_reload(signum, traceback):
        """Explicitly close our connection
        """
        client.close()

    signal.signal(signal.SIGHUP, graceful_reload)

**uWSGI** applications using the ``gevent-wait-for-hub`` option are directly
affected by this behaviour.
