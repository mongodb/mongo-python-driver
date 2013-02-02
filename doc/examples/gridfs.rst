GridFS Example
==============

.. testsetup::

  from pymongo import MongoClient
  client = MongoClient()
  client.drop_database('gridfs_example')

This example shows how to use :mod:`gridfs` to store large binary
objects (e.g. files) in MongoDB.

.. seealso:: The API docs for :mod:`gridfs`.

.. seealso:: `This blog post
   <http://dirolf.com/2010/03/29/new-gridfs-implementation-for-pymongo.html>`_
   for some motivation behind this API.

Setup
-----

We start by creating a :class:`~gridfs.GridFS` instance to use:

.. doctest::

  >>> from pymongo import MongoClient
  >>> import gridfs
  >>>
  >>> db = MongoClient().gridfs_example
  >>> fs = gridfs.GridFS(db)

Every :class:`~gridfs.GridFS` instance is created with and will
operate on a specific :class:`~pymongo.database.Database` instance.

Saving and Retrieving Data
--------------------------

The simplest way to work with :mod:`gridfs` is to use its key/value
interface (the :meth:`~gridfs.GridFS.put` and
:meth:`~gridfs.GridFS.get` methods). To write data to GridFS, use
:meth:`~gridfs.GridFS.put`:

.. doctest::

  >>> a = fs.put("hello world")

:meth:`~gridfs.GridFS.put` creates a new file in GridFS, and returns
the value of the file document's ``"_id"`` key. Given that ``"_id"``
we can use :meth:`~gridfs.GridFS.get` to get back the contents of the
file:

.. doctest::

  >>> fs.get(a).read()
  'hello world'

:meth:`~gridfs.GridFS.get` returns a file-like object, so we get the
file's contents by calling :meth:`~gridfs.grid_file.GridOut.read`.

In addition to putting a :class:`str` as a GridFS file, we can also
put any file-like object (an object with a :meth:`read`
method). GridFS will handle reading the file in chunk-sized segments
automatically. We can also add additional attributes to the file as
keyword arguments:

.. doctest::

  >>> b = fs.put(fs.get(a), filename="foo", bar="baz")
  >>> out = fs.get(b)
  >>> out.read()
  'hello world'
  >>> out.filename
  u'foo'
  >>> out.bar
  u'baz'
  >>> out.upload_date
  datetime.datetime(...)

The attributes we set in :meth:`~gridfs.GridFS.put` are stored in the
file document, and retrievable after calling
:meth:`~gridfs.GridFS.get`. Some attributes (like ``"filename"``) are
special and are defined in the GridFS specification - see that
document for more details.
