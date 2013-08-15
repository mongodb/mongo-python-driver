Requests
========

PyMongo supports the idea of a *request*: a series of operations executed with
a single socket, which are guaranteed to be processed on the server in the same
order as they ran on the client.

Requests are not usually necessary with PyMongo.
By default, the methods :meth:`~pymongo.collection.Collection.insert`,
:meth:`~pymongo.collection.Collection.update`,
:meth:`~pymongo.collection.Collection.save`, and
:meth:`~pymongo.collection.Collection.remove` block until they receive
acknowledgment from the server, so ordered execution is already guaranteed. You
can be certain the next :meth:`~pymongo.collection.Collection.find` or
:meth:`~pymongo.collection.Collection.count`, for example, is executed on the
server after the writes complete. This is called "read-your-writes
consistency."

An example of when a request is necessary is if a series of documents are
inserted with ``w=0`` for performance reasons, and you want to query those
documents immediately afterward: With ``w=0`` the writes can queue up at the
server and might not be immediately visible in query results. Wrapping the
inserts and queries within
:meth:`~pymongo.mongo_client.MongoClient.start_request` and
:meth:`~pymongo.mongo_client.MongoClient.end_request` forces a query to be on
the same socket as the inserts, so the query won't execute until the inserts
are complete on the server side.

Example
-------

Let's consider a collection of web-analytics counters. We want to count the
number of page views our site has served for each combination of browser,
region, and OS, and then show the user the number of page views from his or her
region, *including* the user's own visit. We have three ways to do so reliably:

1. Simply update the counters with an acknowledged write (the default), and
then ``find`` all counters for the visitor's region. This will ensure that the
``update`` completes before the ``find`` begins, but it comes with a performance
penalty that may be unacceptable for analytics.

2. Create the :class:`~pymongo.mongo_client.MongoClient` with ``w=0`` and
``auto_start_request=True`` to do unacknowledged writes and ensure each thread
gets its own socket.

3. Explicitly call :meth:`~pymongo.mongo_client.MongoClient.start_request`,
then do the unacknowledged updates and the queries within the request. This
third method looks like:

.. testsetup::

  from pymongo import MongoClient
  client = MongoClient()
  counts = client.requests_example.counts
  counts.drop()
  region, browser, os = 'US', 'Firefox', 'Mac OS X'

.. doctest::

  >>> client = MongoClient()
  >>> counts = client.requests_example.counts
  >>> region, browser, os = 'US', 'Firefox', 'Mac OS X'
  >>> request = client.start_request()
  >>> try:
  ...   counts.update(
  ...     {'region': region, 'browser': browser, 'os': os},
  ...     {'$inc': {'n': 1 }},
  ...     upsert=True,
  ...     w=0) # unacknowledged write
  ...
  ...   # This always runs after update has completed:
  ...   count = sum([p['n'] for p in counts.find({'region': region})])
  ... finally:
  ...   request.end()
  >>> print count
  1

Requests can also be used as context managers, with the `with statement
<http://docs.python.org/reference/compound_stmts.html#index-15>`_, which makes
the previous example more terse:

.. doctest::

  >>> client.in_request()
  False
  >>> with client.start_request():
  ...   # MongoClient is now in request
  ...   counts.update(
  ...     {'region': region, 'browser': browser, 'os': os},
  ...     {'$inc': {'n': 1 }},
  ...     upsert=True,
  ...     safe=False)
  ...   print sum([p['n'] for p in counts.find({'region': region})])
  2
  >>> client.in_request() # request automatically ended
  False

Requests And ``max_pool_size``
------------------------------

A thread in a request retains exclusive access to a socket until its request
ends or the thread dies; thus, applications in which more than 100 threads are
in requests at once should disable the ``max_pool_size`` option::

    client = MongoClient(host, port, max_pool_size=None)

Failure to increase or disable ``max_pool_size`` in such an application can
leave threads forever waiting for sockets.

See :ref:`connection-pooling`
