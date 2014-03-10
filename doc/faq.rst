Frequently Asked Questions
==========================

.. contents::

Is PyMongo thread-safe?
-----------------------

PyMongo is thread-safe and even provides built-in connection pooling
for threaded applications.

.. _connection-pooling:

How does connection pooling work in PyMongo?
--------------------------------------------

Every :class:`~pymongo.mongo_client.MongoClient` instance has a built-in
connection pool. The pool begins with one open connection. If necessary to
support concurrent access to MongoDB from multiple threads in your application,
the client opens new connections on demand.

By default, there is no thread-affinity for connections.

In versions before 2.6, the default ``max_pool_size`` was 10, and it did not
actually bound the number of open connections; it only determined the number
of connections that would be kept open when no longer in use.

Starting with PyMongo 2.6, the size of the connection pool is capped at
``max_pool_size``, which now defaults to 100. When a thread in your application
begins an operation on MongoDB, if all other connections are in use and the
pool has reached its maximum, the thread pauses, waiting for a connection to
be returned to the pool by another thread.

The default configuration for a :class:`~pymongo.mongo_client.MongoClient`
works for most applications::

    client = MongoClient(host, port)

Create this client **once** when your program starts up, and reuse it for all
operations. It is a common mistake to create a new client for each request,
which is very inefficient.

To support extremely high numbers of concurrent MongoDB operations within one
process, increase ``max_pool_size``::

    client = MongoClient(host, port, max_pool_size=200)

... or make it unbounded::

    client = MongoClient(host, port, max_pool_size=None)

By default, any number of threads are allowed to wait for connections to become
available, and they can wait any length of time. Override ``waitQueueMultiple``
to cap the number of waiting threads. E.g., to keep the number of waiters less
than or equal to 500::

    client = MongoClient(host, port, max_pool_size=50, waitQueueMultiple=10)

When 500 threads are waiting for a socket, the 501st that needs a connection
raises :exc:`~pymongo.errors.ExceededMaxWaiters`. Use this option to
bound the amount of queueing in your application during a load spike, at the
cost of additional exceptions.

Once the pool reaches its max size, additional threads are allowed to wait
indefinitely for connections to become available, unless you set
``waitQueueTimeoutMS``::

    client = MongoClient(host, port, waitQueueTimeoutMS=100)

A thread that waits more than 100ms (in this example) for a connection raises
:exc:`~pymongo.errors.ConnectionFailure`. Use this option if it is more
important to bound the duration of operations during a load spike than it is to
complete every operation.

When :meth:`~pymongo.mongo_client.MongoClient.disconnect` is called by any thread,
all sockets are closed.

:class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` maintains one
connection pool per server in your replica set.

.. seealso:: :doc:`examples/requests`

Does PyMongo support Python 3?
------------------------------

Starting with version 2.2 PyMongo supports Python 3.x where x >= 1. See the
:doc:`python3` for details.

Does PyMongo support asynchronous frameworks like Gevent, Tornado, or Twisted?
------------------------------------------------------------------------------

PyMongo fully supports :doc:`Gevent <examples/gevent>`.

To use MongoDB with `Tornado <http://www.tornadoweb.org/>`_ see the
`Motor <https://github.com/mongodb/motor>`_ project.

For `Twisted <http://twistedmatrix.com/>`_, see `TxMongo
<http://github.com/fiorix/mongo-async-python-driver>`_. Compared to PyMongo,
TxMongo is less stable, lacks features, and is less actively maintained.

What does *CursorNotFound* cursor id not valid at server mean?
--------------------------------------------------------------
Cursors in MongoDB can timeout on the server if they've been open for
a long time without any operations being performed on them. This can
lead to an :class:`~pymongo.errors.CursorNotFound` exception being
raised when attempting to iterate the cursor.

How do I change the timeout value for cursors?
----------------------------------------------
MongoDB doesn't support custom timeouts for cursors, but cursor
timeouts can be turned off entirely. Pass ``timeout=False`` to
:meth:`~pymongo.collection.Collection.find`.

How can I store :mod:`decimal.Decimal` instances?
-------------------------------------------------
MongoDB only supports IEEE 754 floating points - the same as the
Python float type. The only way PyMongo could store Decimal instances
would be to convert them to this standard, so you'd really only be
storing floats anyway - we force users to do this conversion
explicitly so that they are aware that it is happening.

I'm saving ``9.99`` but when I query my document contains ``9.9900000000000002`` - what's going on here?
--------------------------------------------------------------------------------------------------------
The database representation is ``9.99`` as an IEEE floating point (which
is common to MongoDB and Python as well as most other modern
languages). The problem is that ``9.99`` cannot be represented exactly
with a double precision floating point - this is true in some versions of
Python as well:

  >>> 9.99
  9.9900000000000002

The result that you get when you save ``9.99`` with PyMongo is exactly the
same as the result you'd get saving it with the JavaScript shell or
any of the other languages (and as the data you're working with when
you type ``9.99`` into a Python program).

Can you add attribute style access for documents?
-------------------------------------------------
This request has come up a number of times but we've decided not to
implement anything like this. The relevant `jira case
<http://jira.mongodb.org/browse/PYTHON-35>`_ has some information
about the decision, but here is a brief summary:

1. This will pollute the attribute namespace for documents, so could
   lead to subtle bugs / confusing errors when using a key with the
   same name as a dictionary method.

2. The only reason we even use SON objects instead of regular
   dictionaries is to maintain key ordering, since the server
   requires this for certain operations. So we're hesitant to
   needlessly complicate SON (at some point it's hypothetically
   possible we might want to revert back to using dictionaries alone,
   without breaking backwards compatibility for everyone).

3. It's easy (and Pythonic) for new users to deal with documents,
   since they behave just like dictionaries. If we start changing
   their behavior it adds a barrier to entry for new users - another
   class to learn.

What is the correct way to handle time zones with PyMongo?
----------------------------------------------------------

Prior to PyMongo version 1.7, the correct way is to only save naive
:class:`~datetime.datetime` instances, and to save all dates as
UTC. In versions >= 1.7, the driver will automatically convert aware
datetimes to UTC before saving them. By default, datetimes retrieved
from the server (no matter what version of the driver you're using)
will be naive and represent UTC. In newer versions of the driver you
can set the :class:`~pymongo.mongo_client.MongoClient` `tz_aware`
parameter to ``True``, which will cause all
:class:`~datetime.datetime` instances returned from that MongoClient to
be aware (UTC). This setting is recommended, as it can force
application code to handle timezones properly.

.. warning::

   Be careful not to save naive :class:`~datetime.datetime`
   instances that are not UTC (i.e. the result of calling
   :meth:`datetime.datetime.now`).

Something like :mod:`pytz` can be used to convert dates to localtime
after retrieving them from the database.

How can I save a :mod:`datetime.date` instance?
-----------------------------------------------
PyMongo doesn't support saving :mod:`datetime.date` instances, since
there is no BSON type for dates without times. Rather than having the
driver enforce a convention for converting :mod:`datetime.date`
instances to :mod:`datetime.datetime` instances for you, any
conversion should be performed in your client code.

.. _web-application-querying-by-objectid:

When I query for a document by ObjectId in my web application I get no result
-----------------------------------------------------------------------------
It's common in web applications to encode documents' ObjectIds in URLs, like::

  "/posts/50b3bda58a02fb9a84d8991e"

Your web framework will pass the ObjectId portion of the URL to your request
handler as a string, so it must be converted to :class:`~bson.objectid.ObjectId`
before it is passed to :meth:`~pymongo.collection.Collection.find_one`. It is a
common mistake to forget to do this conversion. Here's how to do it correctly
in Flask_ (other web frameworks are similar)::

  from pymongo import MongoClient
  from bson.objectid import ObjectId

  from flask import Flask, render_template

  client = MongoClient()
  app = Flask(__name__)

  @app.route("/posts/<_id>")
  def show_post(_id):
     # NOTE!: converting _id from string to ObjectId before passing to find_one
     post = client.db.posts.find_one({'_id': ObjectId(_id)})
     return render_template('post.html', post=post)

  if __name__ == "__main__":
      app.run()

.. _Flask: http://flask.pocoo.org/

.. seealso:: :ref:`querying-by-objectid`

How can I use PyMongo from Django?
----------------------------------
`Django <http://www.djangoproject.com/>`_ is a popular Python web
framework. Django includes an ORM, :mod:`django.db`. Currently,
there's no official MongoDB backend for Django.

`django-mongodb-engine <https://django-mongodb-engine.readthedocs.org/>`_
is an unofficial MongoDB backend that supports Django aggregations, (atomic)
updates, embedded objects, Map/Reduce and GridFS. It allows you to use most
of Django's built-in features, including the ORM, admin, authentication, site
and session frameworks and caching.

However, it's easy to use MongoDB (and PyMongo) from Django
without using a Django backend. Certain features of Django that require
:mod:`django.db` (admin, authentication and sessions) will not work
using just MongoDB, but most of what Django provides can still be
used.

One project which should make working with MongoDB and Django easier
is `mango <http://github.com/vpulim/mango>`_. Mango is a set of
MongoDB backends for Django sessions and authentication (bypassing
:mod:`django.db` entirely).

.. _using-with-mod-wsgi:

Does PyMongo work with **mod_wsgi**?
------------------------------------
Yes. See the configuration guide for :ref:`pymongo-and-mod_wsgi`.

How can I use something like Python's :mod:`json` module to encode my documents to JSON?
----------------------------------------------------------------------------------------
The :mod:`json` module won't work out of the box with all documents
from PyMongo as PyMongo supports some special types (like
:class:`~bson.objectid.ObjectId` and :class:`~bson.dbref.DBRef`)
that are not supported in JSON. We've added some utilities for working
with :mod:`json` and :mod:`simplejson` in the
:mod:`~bson.json_util` module.

.. _year-2038-problem:

Why do I get an error for dates on or after 2038?
-------------------------------------------------
On Unix systems, dates are represented as seconds from 1 January 1970 and
usually stored in the C :mod:`time_t` type. On most 32-bit operating systems
:mod:`time_t` is a signed 4 byte integer which means it can't handle dates
after 19 January 2038; this is known as the `year 2038 problem
<http://en.wikipedia.org/wiki/Year_2038_problem>`_. Neither MongoDB nor Python
uses :mod:`time_t` to represent dates internally so do not suffer from this
problem, but Python's :mod:`datetime.datetime.fromtimestamp()` does, which
means it is susceptible.

Previous to version 2.0, PyMongo used :mod:`datetime.datetime.fromtimestamp()`
in its pure Python :mod:`bson` module. Therefore, on 32-bit systems you may
get an error retrieving dates after 2038 from MongoDB using older versions
of PyMongo with the pure Python version of :mod:`bson`.

This problem was fixed in the pure Python implementation of :mod:`bson` by
commit ``b19ab334af2a29353529`` (8 June 2011 - PyMongo 2.0).

The C implementation of :mod:`bson` also used to suffer from this problem but
it was fixed in commit ``566bc9fb7be6f9ab2604`` (10 May 2010 - PyMongo 1.7).

Why do I get OverflowError decoding dates stored by another language's driver?
------------------------------------------------------------------------------
PyMongo decodes BSON datetime values to instances of Python's
:class:`datetime.datetime`. Instances of :class:`datetime.datetime` are
limited to years between :data:`datetime.MINYEAR` (usually 1) and
:data:`datetime.MAXYEAR` (usually 9999). Some MongoDB drivers (e.g. the PHP
driver) can store BSON datetimes with year values far outside those supported
by :class:`datetime.datetime`.

There are a few ways to work around this issue. One option is to filter
out documents with values outside of the range supported by
:class:`datetime.datetime`::

  >>> from datetime import datetime
  >>> coll = client.test.dates
  >>> cur = coll.find({'dt': {'$gte': datetime.min, '$lte': datetime.max}})

Another option, assuming you don't need the datetime field, is to filter out
just that field::

  >>> cur = coll.find({}, fields={'dt': False})

