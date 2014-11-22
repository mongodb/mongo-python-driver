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
