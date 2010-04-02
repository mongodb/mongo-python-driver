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

Every :class:`~pymongo.connection.Connection` instance has built-in
connection pooling. Each thread gets its own socket reserved on its
first operation. Those sockets are held until
:meth:`~pymongo.connection.Connection.end_request` is called by that
thread or :meth:`~pymongo.connection.Connection.disconnect` is called
by any thread.

Calling :meth:`~pymongo.connection.Connection.end_request` allows the
socket to be returned to the pool, and to be used by other threads
instead of creating a new socket. Judicious use of this method is
important for applications with many threads or with long running
threads that make few calls to PyMongo operations.

How can I use PyMongo with an asynchronous socket library like `twisted <http://twistedmatrix.com/>`_?
------------------------------------------------------------------------------------------------------

Currently there is no great way to use PyMongo in conjunction with
asynchronous socket frameworks like `twisted
<http://twistedmatrix.com/>`_ or `tornado
<http://www.tornadoweb.org/>`_. PyMongo provides built-in connection
pooling, so some of the benefits of those frameworks can be achieved
just by writing multi-threaded code that shares a
:class:`~pymongo.connection.Connection`.

There is work in progress towards creating an `asynchronous Python
driver <http://github.com/fiorix/mongo-async-python-driver>`_ for
MongoDB using the Twisted framework, this project is currently less
stable than PyMongo however.

What does *OperationFailure* cursor id not valid at server mean?
---------------------------------------------------------------------------------------
Cursors in MongoDB can timeout on the server if they've been open for
a long time without any operations being performed on them. This can
lead to an :class:`~pymongo.errors.OperationFailure` exception being
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
with a double precision floating point - this is true in Python as
well:

.. doctest::

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
Currently the correct way is to only save naive
:class:`~datetime.datetime` instances, and to save all dates as
UTC. Unfortunately, Python time zone handling is less than elegant so
it is quite difficult for the driver to do anything smarter than this.

Something like :mod:`pytz` can be used to convert dates to localtime
after retrieving them from the database.

How can I save a :mod:`datetime.date` instance?
-----------------------------------------------
PyMongo doesn't support saving :mod:`datetime.date` instances, since
there is no BSON type for dates without times. Rather than having the
driver enforce a convention for converting :mod:`datetime.date`
instances to :mod:`datetime.datetime` instances for you, any
conversion should be performed in your client code.

How can I use PyMongo from a web framework like Django?
-------------------------------------------------------
`Django <http://www.djangoproject.com/>`_ is a popular Python web
framework. Django includes an ORM, :mod:`django.db`. Currently,
MongoDB is not supported as a back-end for :mod:`django.db`.

That being said, it's easy to use MongoDB (and PyMongo) from Django
without using such a project. Certain features of Django that require
:mod:`django.db` (admin, authentication and sessions) will not work
using just MongoDB, but most of what Django provides can still be
used. This is similar to using Django on top of the `App Engine
datastore <http://code.google.com/appengine/articles/django.html>`_.

We have written a demo `Django + MongoDB project
<http://github.com/mdirolf/DjanMon/tree/master>`_. The README for that
project describes some of what you need to do to use MongoDB from
Django. The main point is that your persistence code will go directly
into your views, rather than being defined in separate models. The
README also gives instructions for how to change settings.py to
disable the features that won't work with MongoDB.

One project which should make working with MongoDB and Django easier
is `mango <http://github.com/vpulim/mango>`_. Mango is a set of
MongoDB backends for Django sessions and authentication (bypassing
:mod:`django.db` entirely).

.. _using-with-mod-wsgi:

Does PyMongo work with **mod_wsgi**?
------------------------------------
`mod_wsgi <http://code.google.com/p/modwsgi/>`_ is a popular Apache
module used for hosting Python applications conforming to the `wsgi
<http://www.wsgi.org/>`_ specification. There is a potential issue
when deploying PyMongo applications with mod_wsgi involving PyMongo's
C extension and mod_wsgi's multiple sub interpreters.

One tricky issue that we've seen when deploying PyMongo applications
with mod_wsgi is documented `here <http://code.google.com/p/modwsgi/wiki/ApplicationIssues>`_, in the **Multiple Python Sub Interpreters** section. When running PyMongo with the C extension enabled it is possible to see strange failures when encoding due to the way mod_wsgi handles module reloading with multiple sub interpreters. There are several possible ways to work around this issue:

1. Force all WSGI applications to run in the same application group.
2. Run mod_wsgi in daemon mode with different WSGI applications assigned to their own daemon processes.
3. Install PyMongo :ref:`without the C extension <install-no-c>` (this will carry a performance penalty, but is the most immediate solution to this problem).

How can I use something like Python's :mod:`json` module to encode my documents to JSON?
----------------------------------------------------------------------------------------
The :mod:`json` module won't work out of the box with all documents
from PyMongo as PyMongo supports some special types (like
:class:`~pymongo.objectid.ObjectId` and :class:`~pymongo.dbref.DBRef`)
that are not supported in JSON. We've added some utilities for working
with :mod:`json` and :mod:`simplejson` in the
:mod:`~pymongo.json_util` module.
