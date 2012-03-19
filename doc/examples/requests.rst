Requests Example
===========================

PyMongo supports the idea of a *request*: a series of operations executed with
a single socket, which are guaranteed to be processed on the server in the
same order as they ran on the client. This can be useful, for example, if you
want to do a series of :meth:`~pymongo.collection.Collection.insert`
operations with ``safe=False``, and still be certain the next
:meth:`~pymongo.collection.Collection.find` is executed on the server after
the inserts complete. (Another way to guarantee ordering is to pass
``safe=True`` to `insert`, with the additional advantage that errors on the
server will be reported to the client.)

By default, PyMongo starts a request for each thread when the thread first
runs an operation on MongoDB. The thread will continue to use the same socket
exclusively, and no other thread will use this socket, until the thread calls
:meth:`~pymongo.connection.Connection.end_request` or it terminates. At that
point, the socket is returned to the connection pool for use by other threads.

When to Turn Off auto_start_request
-----------------------------------

If your application has many threads running simultaneously, but those threads
rarely access MongoDB, you can improve performance and reduce the number of
connections by creating a :class:`~pymongo.connection.Connection` with
``auto_start_request=False``.

An example of an application that would benefit from turning off
``auto_start_request`` is a web crawler that spawns many threads. Each thread
fetches web pages from the Internet, analyzes them, and inserts the results
into MongoDB. Since each thread would spend much more time downloading and
analyzing pages than it does using its connection to MongoDB, it could use
fewer connections, and thus put less load on MongoDB, if it creates a
:class:`~pymongo.connection.Connection` with ``auto_start_request=False``.

Without ``auto_start_request`` you can still
guarantee ordering, either by creating the
:class:`~pymongo.connection.Connection` with ``safe=True``, or by passing
``safe=True`` to each :meth:`~pymongo.connection.Connection.insert` or
:meth:`~pymongo.connection.Connection.update`, or by executing only the series
of operations in which ordering is important within a request.

When to Use start_request Explicitly
------------------------------------

If your application will benefit from turning off ``auto_start_request`` but
still needs to make unsafe writes, and then read the result of those writes,
then you should use :meth:`~pymongo.connection.Connection.start_request` and
:meth:`~pymongo.connection.Connection.end_request` around the section of code
that requires this form of consistency.

.. testsetup::

  from pymongo import Connection
  connection = Connection(auto_start_request=False)
  db = connection.requests_example
  counts = db.counts
  counts.drop()

Example
-------

Let's consider a collection of web-analytics counters. We want to count the
number of page views our site has served for each combination of browser,
region, and OS, and then show the user an accurate count of the number
of page views from his or her region. We have three ways to do so reliably:

1. Simply update the counters with ``safe=True``, and then `find` all counters
for the visitor's region. This will ensure PyMongo reports an error if any
occurs, but it comes with a performance penalty that may be unacceptable for
analytics.

2. Create the :class:`~pymongo.connection.Connection` with
``auto_start_request=True`` to ensure each thread gets its own socket.

3. Create the :class:`~pymongo.connection.Connection` with
``auto_start_request=False`` and explicitly call
:meth:`~pymongo.connection.Connection.start_request` before executing the
updates and queries. This third method looks like this:

.. doctest::

  >>> connection.start_request()
  >>> try:
  ...   counts.update(
  ...     {'region': region, 'browser': browser, 'os': os},
  ...     {'$inc': {'n': 1 }},
  ...     upsert=True, safe=False)
  ...   # always runs after update has completed
  ...   print sum([p['count'] for p in posts.find({'region': region})])
