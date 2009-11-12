Frequently Asked Questions
==========================

.. contents::

Is PyMongo thread-safe?
-----------------------
PyMongo is thread-safe and even provides built-in connection pooling
for threaded applications. See the documentation for
:class:`~pymongo.connection.Connection`, notably the `pool_size`
parameter.

How can I use PyMongo with an asynchronous socket library like `twisted <http://twistedmatrix.com/>`_?
------------------------------------------------------------------------------------------------------
Currently there is no great way to use PyMongo in conjunction with
asynchronous socket frameworks like `twisted
<http://twistedmatrix.com/>`_ or `tornado
<http://www.tornadoweb.org/>`_. One way to get the same benefits those
frameworks provide using PyMongo is to write multi-threaded code and
use PyMongo's built in connection pooling.

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
well::

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
.. todo:: move django docs here

We've written a `short guide
<http://www.mongodb.org/display/DOCS/Django+and+MongoDB>`_ on using
Django and MongoDB.

How can I use something like Python's :mod:`json` module to encode my documents to JSON?
----------------------------------------------------------------------------------------
The :mod:`json` module won't work out of the box with all documents
from PyMongo as PyMongo supports some special types (like
:class:`~pymongo.objectid.ObjectId` and :class:`~pymongo.dbref.DBRef`)
that are not supported in JSON. We've added some utilities for working
with :mod:`json` and :mod:`simplejson` in the
:mod:`~pymongo.json_util` module.
