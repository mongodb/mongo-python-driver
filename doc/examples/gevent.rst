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
When shutting down your application gracefully, your code be may waiting
for any remaining greenlet to terminate before exiting.
Because monkey-patched threads are greenlets and PyMongo uses quite a lot
of them, this may become a **blocking operation**  and may prevent your
application to exit properly. You therefore must close (or dereference)
any active MongoClient object before exiting !

**uWSGI** applications using the ``gevent-wait-for-hub`` option are directly
affected by this behaviour.
