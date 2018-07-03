Frequently Asked Questions
==========================

.. contents::

Is PyMongo thread-safe?
-----------------------

PyMongo is thread-safe and provides built-in connection pooling
for threaded applications.

.. _pymongo-fork-safe:

Is PyMongo fork-safe?
---------------------

PyMongo is not fork-safe. Care must be taken when using instances of
:class:`~pymongo.mongo_client.MongoClient` with ``fork()``. Specifically,
instances of MongoClient must not be copied from a parent process to
a child process. Instead, the parent process and each child process must
create their own instances of MongoClient. Instances of MongoClient copied from
the parent process have a high probability of deadlock in the child process due
to the inherent incompatibilities between ``fork()``, threads, and locks
described :ref:`below <pymongo-fork-safe-details>`. PyMongo will attempt to
issue a warning if there is a chance of this deadlock occurring.

.. _pymongo-fork-safe-details:

MongoClient spawns multiple threads to run background tasks such as monitoring
connected servers. These threads share state that is protected by instances of
:class:`~threading.Lock`, which are themselves `not fork-safe`_. The
driver is therefore subject to the same limitations as any other multithreaded
code that uses :class:`~threading.Lock` (and mutexes in general). One of these
limitations is that the locks become useless after ``fork()``. During the fork,
all locks are copied over to the child process in the same state as they were
in the parent: if they were locked, the copied locks are also locked. The child
created by ``fork()`` only has one thread, so any locks that were taken out by
other threads in the parent will never be released in the child. The next time
the child process attempts to acquire one of these locks, deadlock occurs.

For a long but interesting read about the problems of Python locks in
multithreaded contexts with ``fork()``, see http://bugs.python.org/issue6721.

.. _not fork-safe: http://bugs.python.org/issue6721

.. _connection-pooling:

How does connection pooling work in PyMongo?
--------------------------------------------

Every :class:`~pymongo.mongo_client.MongoClient` instance has a built-in
connection pool per server in your MongoDB topology. These pools open sockets
on demand to support the number of concurrent MongoDB operations that your
multi-threaded application requires. There is no thread-affinity for sockets.

The size of each connection pool is capped at ``maxPoolSize``, which defaults
to 100. If there are ``maxPoolSize`` connections to a server and all are in
use, the next request to that server will wait until one of the connections
becomes available.

The client instance opens one additional socket per server in your MongoDB
topology for monitoring the server's state.

For example, a client connected to a 3-node replica set opens 3 monitoring
sockets. It also opens as many sockets as needed to support a multi-threaded
application's concurrent operations on each server, up to ``maxPoolSize``. With
a ``maxPoolSize`` of 100, if the application only uses the primary (the
default), then only the primary connection pool grows and the total connections
is at most 103. If the application uses a
:class:`~pymongo.read_preferences.ReadPreference` to query the secondaries,
their pools also grow and the total connections can reach 303.

It is possible to set the minimum number of concurrent connections to each
server with ``minPoolSize``, which defaults to 0. The connection pool will be
initialized with this number of sockets. If sockets are closed due to any
network errors, causing the total number of sockets (both in use and idle) to
drop below the minimum, more sockets are opened until the minimum is reached.

The maximum number of milliseconds that a connection can remain idle in the
pool before being removed and replaced can be set with ``maxIdleTime``, which
defaults to `None` (no limit).

The default configuration for a :class:`~pymongo.mongo_client.MongoClient`
works for most applications::

    client = MongoClient(host, port)

Create this client **once** for each process, and reuse it for all
operations. It is a common mistake to create a new client for each request,
which is very inefficient.

To support extremely high numbers of concurrent MongoDB operations within one
process, increase ``maxPoolSize``::

    client = MongoClient(host, port, maxPoolSize=200)

... or make it unbounded::

    client = MongoClient(host, port, maxPoolSize=None)

By default, any number of threads are allowed to wait for sockets to become
available, and they can wait any length of time. Override ``waitQueueMultiple``
to cap the number of waiting threads. E.g., to keep the number of waiters less
than or equal to 500::

    client = MongoClient(host, port, maxPoolSize=50, waitQueueMultiple=10)

When 500 threads are waiting for a socket, the 501st that needs a socket
raises :exc:`~pymongo.errors.ExceededMaxWaiters`. Use this option to
bound the amount of queueing in your application during a load spike, at the
cost of additional exceptions.

Once the pool reaches its max size, additional threads are allowed to wait
indefinitely for sockets to become available, unless you set
``waitQueueTimeoutMS``::

    client = MongoClient(host, port, waitQueueTimeoutMS=100)

A thread that waits more than 100ms (in this example) for a socket raises
:exc:`~pymongo.errors.ConnectionFailure`. Use this option if it is more
important to bound the duration of operations during a load spike than it is to
complete every operation.

When :meth:`~pymongo.mongo_client.MongoClient.close` is called by any thread,
all idle sockets are closed, and all sockets that are in use will be closed as
they are returned to the pool.

Does PyMongo support Python 3?
------------------------------

PyMongo supports CPython 3.4+ and PyPy3.5+. See the :doc:`python3` for details.

Does PyMongo support asynchronous frameworks like Gevent, asyncio, Tornado, or Twisted?
---------------------------------------------------------------------------------------

PyMongo fully supports :doc:`Gevent <examples/gevent>`.

To use MongoDB with `asyncio <https://docs.python.org/3/library/asyncio.html>`_
or `Tornado <http://www.tornadoweb.org/>`_, see the
`Motor <https://github.com/mongodb/motor>`_ project.

For `Twisted <http://twistedmatrix.com/>`_, see `TxMongo
<https://github.com/twisted/txmongo>`_. Its stated mission is to keep feature
parity with PyMongo.

.. _writes-and-ids:

Why does PyMongo add an _id field to all of my documents?
---------------------------------------------------------

When a document is inserted to MongoDB using
:meth:`~pymongo.collection.Collection.insert_one`,
:meth:`~pymongo.collection.Collection.insert_many`, or
:meth:`~pymongo.collection.Collection.bulk_write`, and that document does not
include an ``_id`` field, PyMongo automatically adds one for you, set to an
instance of :class:`~bson.objectid.ObjectId`. For example::

  >>> my_doc = {'x': 1}
  >>> collection.insert_one(my_doc)
  <pymongo.results.InsertOneResult object at 0x7f3fc25bd640>
  >>> my_doc
  {'x': 1, '_id': ObjectId('560db337fba522189f171720')}

Users often discover this behavior when calling
:meth:`~pymongo.collection.Collection.insert_many` with a list of references
to a single document raises :exc:`~pymongo.errors.BulkWriteError`. Several
Python idioms lead to this pitfall::

  >>> doc = {}
  >>> collection.insert_many(doc for _ in range(10))
  Traceback (most recent call last):
  ...
  pymongo.errors.BulkWriteError: batch op errors occurred
  >>> doc
  {'_id': ObjectId('560f171cfba52279f0b0da0c')}

  >>> docs = [{}]
  >>> collection.insert_many(docs * 10)
  Traceback (most recent call last):
  ...
  pymongo.errors.BulkWriteError: batch op errors occurred
  >>> docs
  [{'_id': ObjectId('560f1933fba52279f0b0da0e')}]

PyMongo adds an ``_id`` field in this manner for a few reasons:

- All MongoDB documents are required to have an ``_id`` field.
- If PyMongo were to insert a document without an ``_id`` MongoDB would add one
  itself, but it would not report the value back to PyMongo.
- Copying the document to insert before adding the ``_id`` field would be
  prohibitively expensive for most high write volume applications.

If you don't want PyMongo to add an ``_id`` to your documents, insert only
documents that already have an ``_id`` field, added by your application.

Key order in subdocuments -- why does my query work in the shell but not PyMongo?
---------------------------------------------------------------------------------

.. testsetup:: key-order

  from bson.son import SON
  from pymongo.mongo_client import MongoClient

  collection = MongoClient().test.collection
  collection.drop()
  collection.insert_one({'_id': 1.0,
                         'subdocument': SON([('b', 1.0), ('a', 1.0)])})

The key-value pairs in a BSON document can have any order (except that ``_id``
is always first). The mongo shell preserves key order when reading and writing
data. Observe that "b" comes before "a" when we create the document and when it
is displayed:

.. code-block:: javascript

  > // mongo shell.
  > db.collection.insert( { "_id" : 1, "subdocument" : { "b" : 1, "a" : 1 } } )
  WriteResult({ "nInserted" : 1 })
  > db.collection.find()
  { "_id" : 1, "subdocument" : { "b" : 1, "a" : 1 } }

PyMongo represents BSON documents as Python dicts by default, and the order
of keys in dicts is not defined. That is, a dict declared with the "a" key
first is the same, to Python, as one with "b" first:

  >>> print({'a': 1.0, 'b': 1.0})
  {'a': 1.0, 'b': 1.0}
  >>> print({'b': 1.0, 'a': 1.0})
  {'a': 1.0, 'b': 1.0}

Therefore, Python dicts are not guaranteed to show keys in the order they are
stored in BSON. Here, "a" is shown before "b":

  >>> print(collection.find_one())
  {u'_id': 1.0, u'subdocument': {u'a': 1.0, u'b': 1.0}}

To preserve order when reading BSON, use the :class:`~bson.son.SON` class,
which is a dict that remembers its key order. First, get a handle to the
collection, configured to use :class:`~bson.son.SON` instead of dict:

.. doctest:: key-order
  :options: +NORMALIZE_WHITESPACE

  >>> from bson import CodecOptions, SON
  >>> opts = CodecOptions(document_class=SON)
  >>> opts
  CodecOptions(document_class=<class 'bson.son.SON'>,
               tz_aware=False,
               uuid_representation=PYTHON_LEGACY,
               unicode_decode_error_handler='strict',
               tzinfo=None)
  >>> collection_son = collection.with_options(codec_options=opts)

Now, documents and subdocuments in query results are represented with
:class:`~bson.son.SON` objects:

.. doctest:: key-order

  >>> print(collection_son.find_one())
  SON([(u'_id', 1.0), (u'subdocument', SON([(u'b', 1.0), (u'a', 1.0)]))])

The subdocument's actual storage layout is now visible: "b" is before "a".

Because a dict's key order is not defined, you cannot predict how it will be
serialized **to** BSON. But MongoDB considers subdocuments equal only if their
keys have the same order. So if you use a dict to query on a subdocument it may
not match:

  >>> collection.find_one({'subdocument': {'a': 1.0, 'b': 1.0}}) is None
  True

Swapping the key order in your query makes no difference:

  >>> collection.find_one({'subdocument': {'b': 1.0, 'a': 1.0}}) is None
  True

... because, as we saw above, Python considers the two dicts the same.

There are two solutions. First, you can match the subdocument field-by-field:

  >>> collection.find_one({'subdocument.a': 1.0,
  ...                      'subdocument.b': 1.0})
  {u'_id': 1.0, u'subdocument': {u'a': 1.0, u'b': 1.0}}

The query matches any subdocument with an "a" of 1.0 and a "b" of 1.0,
regardless of the order you specify them in Python or the order they are stored
in BSON. Additionally, this query now matches subdocuments with additional
keys besides "a" and "b", whereas the previous query required an exact match.

The second solution is to use a :class:`~bson.son.SON` to specify the key order:

  >>> query = {'subdocument': SON([('b', 1.0), ('a', 1.0)])}
  >>> collection.find_one(query)
  {u'_id': 1.0, u'subdocument': {u'a': 1.0, u'b': 1.0}}

The key order you use when you create a :class:`~bson.son.SON` is preserved
when it is serialized to BSON and used as a query. Thus you can create a
subdocument that exactly matches the subdocument in the collection.

.. seealso:: `MongoDB Manual entry on subdocument matching
   <http://docs.mongodb.org/manual/tutorial/query-documents/#embedded-documents>`_.

What does *CursorNotFound* cursor id not valid at server mean?
--------------------------------------------------------------
Cursors in MongoDB can timeout on the server if they've been open for
a long time without any operations being performed on them. This can
lead to an :class:`~pymongo.errors.CursorNotFound` exception being
raised when attempting to iterate the cursor.

How do I change the timeout value for cursors?
----------------------------------------------
MongoDB doesn't support custom timeouts for cursors, but cursor
timeouts can be turned off entirely. Pass ``no_cursor_timeout=True`` to
:meth:`~pymongo.collection.Collection.find`.

How can I store :mod:`decimal.Decimal` instances?
-------------------------------------------------

PyMongo >= 3.4 supports the Decimal128 BSON type introduced in MongoDB 3.4.
See :mod:`~bson.decimal128` for more information.

MongoDB <= 3.2 only supports IEEE 754 floating points - the same as the
Python float type. The only way PyMongo could store Decimal instances to
these versions of MongoDB would be to convert them to this standard, so
you'd really only be storing floats anyway - we force users to do this
conversion explicitly so that they are aware that it is happening.

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

See :doc:`examples/datetimes` for examples on how to handle
:class:`~datetime.datetime` objects correctly.

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

`django-mongodb-engine <https://django-mongodb-engine.readthedocs.io/>`_
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

Does PyMongo work with PythonAnywhere?
--------------------------------------
No. PyMongo creates Python threads which
`PythonAnywhere <https://www.pythonanywhere.com>`_ does not support. For more
information see `PYTHON-1495 <https://jira.mongodb.org/browse/PYTHON-1495>`_.

How can I use something like Python's :mod:`json` module to encode my documents to JSON?
----------------------------------------------------------------------------------------
:mod:`~bson.json_util` is PyMongo's built in, flexible tool for using
Python's :mod:`json` module with BSON documents and `MongoDB Extended JSON
<https://docs.mongodb.com/manual/reference/mongodb-extended-json/>`_. The
:mod:`json` module won't work out of the box with all documents from PyMongo
as PyMongo supports some special types (like :class:`~bson.objectid.ObjectId`
and :class:`~bson.dbref.DBRef`) that are not supported in JSON.

`python-bsonjs <https://pypi.python.org/pypi/python-bsonjs>`_ is a fast
BSON to MongoDB Extended JSON converter built on top of
`libbson <https://github.com/mongodb/libbson>`_. `python-bsonjs` does not
depend on PyMongo and can offer a nice performance improvement over
:mod:`~bson.json_util`. `python-bsonjs` works best with PyMongo when using
:class:`~bson.raw_bson.RawBSONDocument`.

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

  >>> cur = coll.find({}, projection={'dt': False})

.. _multiprocessing:

Using PyMongo with Multiprocessing
----------------------------------

On Unix systems the multiprocessing module spawns processes using ``fork()``.
Care must be taken when using instances of
:class:`~pymongo.mongo_client.MongoClient` with ``fork()``. Specifically,
instances of MongoClient must not be copied from a parent process to a child
process. Instead, the parent process and each child process must create their
own instances of MongoClient. For example::

  # Each process creates its own instance of MongoClient.
  def func():
      db = pymongo.MongoClient().mydb
      # Do something with db.

  proc = multiprocessing.Process(target=func)
  proc.start()

**Never do this**::

  client = pymongo.MongoClient()

  # Each child process attempts to copy a global MongoClient
  # created in the parent process. Never do this.
  def func():
    db = client.mydb
    # Do something with db.

  proc = multiprocessing.Process(target=func)
  proc.start()

Instances of MongoClient copied from the parent process have a high probability
of deadlock in the child process due to
:ref:`inherent incompatibilities between fork(), threads, and locks
<pymongo-fork-safe-details>`. PyMongo will attempt to issue a warning if there
is a chance of this deadlock occurring.

.. seealso:: :ref:`pymongo-fork-safe`
