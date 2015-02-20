Tutorial
========

.. testsetup::

  from pymongo import MongoClient
  client = MongoClient()
  client.drop_database('test-database')

This tutorial is intended as an introduction to working with
**MongoDB** and **PyMongo**.

Prerequisites
-------------
Before we start, make sure that you have the **PyMongo** distribution
:doc:`installed <installation>`. In the Python shell, the following
should run without raising an exception:

.. doctest::

  >>> import pymongo

This tutorial also assumes that a MongoDB instance is running on the
default host and port. Assuming you have `downloaded and installed
<http://www.mongodb.org/display/DOCS/Getting+Started>`_ MongoDB, you
can start it like so:

.. code-block:: bash

  $ mongod

Making a Connection with MongoClient
------------------------------------
The first step when working with **PyMongo** is to create a
:class:`~pymongo.mongo_client.MongoClient` to the running **mongod**
instance. Doing so is easy:

.. doctest::

  >>> from pymongo import MongoClient
  >>> client = MongoClient()

The above code will connect on the default host and port. We can also
specify the host and port explicitly, as follows:

.. doctest::

  >>> client = MongoClient('localhost', 27017)

Or use the MongoDB URI format:

.. doctest::

  >>> client = MongoClient('mongodb://localhost:27017/')

Getting a Database
------------------
A single instance of MongoDB can support multiple independent
`databases <http://www.mongodb.org/display/DOCS/Databases>`_. When
working with PyMongo you access databases using attribute style access
on :class:`~pymongo.mongo_client.MongoClient` instances:

.. doctest::

  >>> db = client.test_database

If your database name is such that using attribute style access won't
work (like ``test-database``), you can use dictionary style access
instead:

.. doctest::

  >>> db = client['test-database']

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
:meth:`~pymongo.collection.Collection.insert_one` method:

.. doctest::

  >>> posts = db.posts
  >>> post_id = posts.insert_one(post).inserted_id
  >>> post_id
  ObjectId('...')

When a document is inserted a special key, ``"_id"``, is automatically
added if the document doesn't already contain an ``"_id"`` key. The value
of ``"_id"`` must be unique across the
collection. :meth:`~pymongo.collection.Collection.insert_one` returns an
instance of :class:`~pymongo.results.InsertOneResult`. For more information
on ``"_id"``, see the `documentation on _id
<http://www.mongodb.org/display/DOCS/Object+IDs>`_.

After inserting the first document, the *posts* collection has
actually been created on the server. We can verify this by listing all
of the collections in our database:

.. doctest::

  >>> db.collection_names(include_system_collections=False)
  [u'posts']

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
  >>>

.. _querying-by-objectid:

Querying By ObjectId
--------------------
We can also find a post by its ``_id``, which in our example is an ObjectId:

.. doctest::

  >>> post_id
  ObjectId(...)
  >>> posts.find_one({"_id": post_id})
  {u'date': datetime.datetime(...), u'text': u'My first blog post!', u'_id': ObjectId('...'), u'author': u'Mike', u'tags': [u'mongodb', u'python', u'pymongo']}

Note that an ObjectId is not the same as its string representation:

.. doctest::

  >>> post_id_as_str = str(post_id)
  >>> posts.find_one({"_id": post_id_as_str}) # No result
  >>>

A common task in web applications is to get an ObjectId from the
request URL and find the matching document. It's necessary in this
case to **convert the ObjectId from a string** before passing it to
``find_one``::

  from bson.objectid import ObjectId

  # The web framework gets post_id from the URL and passes it as a string
  def get(post_id):
      # Convert from string to ObjectId:
      document = client.db.collection.find_one({'_id': ObjectId(post_id)})

.. seealso:: :ref:`web-application-querying-by-objectid`

A Note On Unicode Strings
-------------------------
You probably noticed that the regular Python strings we stored earlier look
different when retrieved from the server (e.g. u'Mike' instead of 'Mike').
A short explanation is in order.

MongoDB stores data in `BSON format <http://bsonspec.org>`_. BSON strings are
UTF-8 encoded so PyMongo must ensure that any strings it stores contain only
valid UTF-8 data. Regular strings (<type 'str'>) are validated and stored
unaltered. Unicode strings (<type 'unicode'>) are encoded UTF-8 first. The
reason our example string is represented in the Python shell as u'Mike' instead
of 'Mike' is that PyMongo decodes each BSON string to a Python unicode string,
not a regular str.

`You can read more about Python unicode strings here
<http://docs.python.org/howto/unicode.html>`_.

Bulk Inserts
------------
In order to make querying a little more interesting, let's insert a
few more documents. In addition to inserting a single document, we can
also perform *bulk insert* operations, by passing a list as the
first argument to :meth:`~pymongo.collection.Collection.insert_many`.
This will insert each document in the list, sending only a single
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
  >>> result = posts.insert_many(new_posts)
  >>> result.inserted_ids
  [ObjectId('...'), ObjectId('...')]

There are a couple of interesting things to note about this example:

  - The result from :meth:`~pymongo.collection.Collection.insert_many` now
    returns two :class:`~bson.objectid.ObjectId` instances, one for
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
  ...   print post
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
  3

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
  2

Now the query is using a *BtreeCursor* (the index) and only scanning
over the 2 matching documents.

.. seealso:: The MongoDB documentation on `indexes <http://www.mongodb.org/display/DOCS/Indexes>`_
