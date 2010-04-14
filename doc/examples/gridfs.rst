GridFS Example
==============

.. warning::

   This example is out of date, and documents the API for GridFS in
   PyMongo versions < 1.6. If you are using a version of PyMongo that
   is >= 1.6 please see `this blog post
   <http://dirolf.com/2010/03/29/new-gridfs-implementation-for-pymongo.html>`_
   for an overview of how the new API works.

.. testsetup::

  from pymongo import Connection
  connection = Connection()
  connection.drop_database('gridfs_example')

This example shows how to use :mod:`gridfs` to store large binary
objects (e.g. files) in MongoDB.

.. seealso:: The API docs for :mod:`gridfs`.

Setup
-----

We start by creating a :class:`~gridfs.GridFS` instance to use:

.. doctest::

  >>> from pymongo import Connection
  >>> import gridfs
  >>>
  >>> db = Connection().gridfs_example
  >>> fs = gridfs.GridFS(db)

Every :class:`~gridfs.GridFS` instance is created with and will
operate on a specific :class:`~pymongo.database.Database` instance.

Saving and Retrieving Data
--------------------------

The :mod:`gridfs` module exposes a file-like interface that should be
familiar to most Python programmers. We can open a file for writing
and insert some data:

.. doctest::

  >>> f = fs.open("hello.txt", "w")
  >>> f.write("hello ")
  >>> f.write("world")
  >>> f.close()

We can then read back the data that was just inserted:

.. doctest::

  >>> g = fs.open("hello.txt")
  >>> g.read()
  'hello world'
  >>> g.close()

It's important that :meth:`~gridfs.grid_file.GridFile.close` gets
called for every file that gets opened. If you're using a Python
interpreter that supports the ``with`` statement doing so is easy:

.. doctest::

  >>> with fs.open("hello.txt") as g:
  ...   g.read()
  ...
  'hello world'
