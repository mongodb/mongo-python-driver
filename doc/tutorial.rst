Tutorial
========

.. testsetup::

  from pymongo import Connection
  connection = Connection()
  connection.drop_database('test-database')

This tutorial is intended as an introduction to working with
**MongoDB** and **PyMongo**.

Prerequisites
-------------
Before we start, make sure that you have the **PyMongo** distribution
:doc:`installed <installation>`. In the Python shell, the following should run without
raising an exception:

.. doctest::

  >>> import pymongo

This tutorial also assumes that a MongoDB instance is running on the
default host and port. Assuming you have `downloaded and installed
<http://www.mongodb.org/display/DOCS/Getting+Started>`_ MongoDB, you
can start it like so:

.. code-block:: bash

  $ mongod

Making a Connection
-------------------
The first step when working with **PyMongo** is to create a
:class:`~pymongo.connection.Connection` to the running **mongod**
instance. Doing so is easy:

.. doctest::

  >>> from pymongo import Connection
  >>> connection = Connection()

The above code will connect on the default host and port. We can also
specify the host and port explicitly, as follows:

.. doctest::

  >>> connection = Connection('localhost', 27017)

Getting a Database
------------------
A single instance of MongoDB can support multiple independent
`databases <http://www.mongodb.org/display/DOCS/Databases>`_. When
working with PyMongo you access databases using attribute style access
on :class:`~pymongo.connection.Connection` instances:

.. doctest::

  >>> db = connection.test_database

If your database name is such that using attribute style access won't
work (like ``test-database``), you can use dictionary style access
instead:

.. doctest::

  >>> db = connection['test-database']

Getting a Collection
--------------------
A `collection <http://www.mongodb.org/display/DOCS/Collections>`_ is a
group of documents stored in MongoDB, and can be thought of as roughly
the equivalent of a table in a relational database. Getting a
collection in PyMongo works the same as getting a database:

.. doctest::

  >>> collection = db.test_collection

or (using dictionary style access):

.. doctest::

  >>> collection = db['test-collection']

An important note about collections (and databases) in MongoDB is that
they are created lazily - none of the above commands have actually
performed any operations on the MongoDB server. Collections and
databases are created when the first document is inserted into them.

Documents
---------
Data in MongoDB is represented (and stored) using JSON-style
documents. In PyMongo we use dictionaries to represent documents. As
an example, the following dictionary might be used to represent a blog
post:

.. doctest::

  >>> import datetime
  >>> post = {"author": "Mike",
  ...         "text": "My first blog post!",
  ...         "tags": ["mongodb", "python", "pymongo"],
  ...         "date": datetime.datetime.utcnow()}

Note that documents can contain native Python types (like
:class:`datetime.datetime` instances) which will be automatically
converted to and from the appropriate `BSON
<http://www.mongodb.org/display/DOCS/BSON>`_ types.

.. todo:: link to table of Python <-> BSON types

Inserting a Document
--------------------
To insert a document into a collection we can use the
:meth:`~pymongo.collection.Collection.insert` method:

.. doctest::

  >>> posts = db.posts
  >>> posts.insert(post)
  ObjectId('...')

When a document is inserted a special key, ``"_id"``, is automatically
added if the document doesn't already contain an ``"_id"`` key. The value
of ``"_id"`` must be unique across the
collection. :meth:`~pymongo.collection.Collection.insert` returns the
value of ``"_id"`` for the inserted document. For more information, see the
`documentation on _id
<http://www.mongodb.org/display/DOCS/Object+IDs>`_.

.. todo:: notes on the differences between save and insert

After inserting the first document, the *posts* collection has
actually been created on the server. We can verify this by listing all
of the collections in our database:

.. doctest::

  >>> db.collection_names()
  [u'posts', u'system.indexes']

.. note:: The *system.indexes* collection is a special internal
   collection that was created automatically.


Getting a Single Document With :meth:`~pymongo.collection.Collection.find_one`
------------------------------------------------------------------------------
The most basic type of query that can be performed in MongoDB is
:meth:`~pymongo.collection.Collection.find_one`. This method returns a
single document matching a query (or ``None`` if there are no
matches). It is useful when you know there is only one matching
document, or are only interested in the first match. Here we use
:meth:`~pymongo.collection.Collection.find_one` to get the first
document from the posts collection:

.. doctest::

  >>> posts.find_one()
  {u'date': datetime.datetime(...), u'text': u'My first blog post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'mongodb', u'python', u'pymongo']}

The result is a dictionary matching the one that we inserted previously.

.. note:: The returned document contains an ``"_id"``, which was
   automatically added on insert.

:meth:`~pymongo.collection.Collection.find_one` also supports querying
on specific elements that the resulting document must match. To limit
our results to a document with author "Mike" we do:

.. doctest::

  >>> posts.find_one({"author": "Mike"})
  {u'date': datetime.datetime(...), u'text': u'My first blog post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'mongodb', u'python', u'pymongo']}

If we try with a different author, like "Eliot", we'll get no result:

.. doctest::

  >>> posts.find_one({"author": "Eliot"})

Bulk Inserts
------------
In order to make querying a little more interesting, let's insert a
few more documents. In addition to inserting a single document, we can
also perform *bulk insert* operations, by passing an iterable as the
first argument to :meth:`~pymongo.collection.Collection.insert`. This
will insert each document in the iterable, sending only a single
command to the server:

.. doctest::

  >>> new_posts = [{"author": "Mike",
  ...               "text": "Another post!",
  ...               "tags": ["bulk", "insert"],
  ...               "date": datetime.datetime(2009, 11, 12, 11, 14)},
  ...              {"author": "Eliot",
  ...               "title": "MongoDB is fun",
  ...               "text": "and pretty easy too!",
  ...               "date": datetime.datetime(2009, 11, 10, 10, 45)}]
  >>> posts.insert(new_posts)
  [ObjectId('...'), ObjectId('...')]

There are a couple of interesting things to note about this example:

  - The call to :meth:`~pymongo.collection.Collection.insert` now
    returns two :class:`~pymongo.objectid.ObjectId` instances, one for
    each inserted document.
  - ``new_posts[1]`` has a different "shape" than the other posts -
    there is no ``"tags"`` field and we've added a new field,
    ``"title"``. This is what we mean when we say that MongoDB is
    *schema-free*.

Querying for More Than One Document
-----------------------------------
To get more than a single document as the result of a query we use the
:meth:`~pymongo.collection.Collection.find`
method. :meth:`~pymongo.collection.Collection.find` returns a
:class:`~pymongo.cursor.Cursor` instance, which allows us to iterate
over all matching documents. For example, we can iterate over every
document in the ``posts`` collection:

.. doctest::

  >>> for post in posts.find():
  ...   post
  ...
  {u'date': datetime.datetime(...), u'text': u'My first blog post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'mongodb', u'python', u'pymongo']}
  {u'date': datetime.datetime(2009, 11, 12, 11, 14), u'text': u'Another post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'bulk', u'insert']}
  {u'date': datetime.datetime(2009, 11, 10, 10, 45), u'text': u'and pretty easy too!', u'_id': ObjectId('...'), u'author': u'Eliot', u'title': u'MongoDB is fun'}

Just like we did with :meth:`~pymongo.collection.Collection.find_one`,
we can pass a document to :meth:`~pymongo.collection.Collection.find`
to limit the returned results. Here, we get only those documents whose
author is "Mike":

.. doctest::

  >>> for post in posts.find({"author": "Mike"}):
  ...   post
  ...
  {u'date': datetime.datetime(...), u'text': u'My first blog post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'mongodb', u'python', u'pymongo']}
  {u'date': datetime.datetime(2009, 11, 12, 11, 14), u'text': u'Another post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'bulk', u'insert']}

Counting
--------
If we just want to know how many documents match a query we can
perform a :meth:`~pymongo.cursor.Cursor.count` operation instead of a
full query. We can get a count of all of the documents in a
collection:

.. doctest::

  >>> posts.count()
  3

or just of those documents that match a specific query:

.. doctest::

  >>> posts.find({"author": "Mike"}).count()
  2

Range Queries
-------------
MongoDB supports many different types of `advanced queries
<http://www.mongodb.org/display/DOCS/Advanced+Queries>`_. As an
example, lets perform a query where we limit results to posts older
than a certain date, but also sort the results by author:

.. doctest::

  >>> d = datetime.datetime(2009, 11, 12, 12)
  >>> for post in posts.find({"date": {"$lt": d}}).sort("author"):
  ...   post
  ...
  {u'date': datetime.datetime(2009, 11, 10, 10, 45), u'text': u'and pretty easy too!', u'_id': ObjectId('...'), u'author': u'Eliot', u'title': u'MongoDB is fun'}
  {u'date': datetime.datetime(2009, 11, 12, 11, 14), u'text': u'Another post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'bulk', u'insert']}

Here we use the special ``"$lt"`` operator to do a range query, and
also call :meth:`~pymongo.cursor.Cursor.sort` to sort the results
by author.

Indexing
--------
To make the above query fast we can add a compound index on
``"date"`` and ``"author"``. To start, lets use the
:meth:`~pymongo.cursor.Cursor.explain` method to get some information
about how the query is being performed without the index:

.. doctest::

  >>> posts.find({"date": {"$lt": d}}).sort("author").explain()["cursor"]
  u'BasicCursor'
  >>> posts.find({"date": {"$lt": d}}).sort("author").explain()["nscanned"]
  3.0

We can see that the query is using the *BasicCursor* and scanning over
all 3 documents in the collection. Now let's add a compound index and
look at the same information:

.. doctest::

  >>> from pymongo import ASCENDING, DESCENDING
  >>> posts.create_index([("date", DESCENDING), ("author", ASCENDING)])
  u'date_-1_author_1'
  >>> posts.find({"date": {"$lt": d}}).sort("author").explain()["cursor"]
  u'BtreeCursor date_-1_author_1'
  >>> posts.find({"date": {"$lt": d}}).sort("author").explain()["nscanned"]
  2.0

Now the query is using a *BtreeCursor* (the index) and only scanning
over the 2 matching documents.

.. seealso:: The MongoDB documentation on `indexes <http://www.mongodb.org/display/DOCS/Indexes>`_
