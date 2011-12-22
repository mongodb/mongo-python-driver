Tools
=====
Many tools have been written for working with **PyMongo**. If you know
of or have created a tool for working with MongoDB from Python please
list it here.

.. note:: We try to keep this list current. As such, projects that
   have not been updated recently or appear to be unmaintained will
   occasionally be removed from the list or moved to the back (to keep
   the list from becoming too intimidating).

   If a project gets removed that is still being developed or is in active use
   please let us know or add it back.

ORM-like Layers
---------------
Some people have found that they prefer to work with a layer that
has more features than PyMongo provides. Often, things like models and
validation are desired. To that end, several different ORM-like layers
have been written by various authors.

It is our recommendation that new users begin by working directly with
PyMongo, as described in the rest of this documentation. Many people
have found that the features of PyMongo are enough for their
needs. Even if you eventually come to the decision to use one of these
layers, the time spent working directly with the driver will have
increased your understanding of how MongoDB actually works.

* `MongoKit <http://namlook.github.com/mongokit/>`_ is a python module that
  brings structured schema and validation layer on top of the great pymongo
  driver.
  For more information, see
  `the tutorial <http://namlook.github.com/mongokit/tutorial.html>`_.
* `Ming <http://merciless.sourceforge.net/>`_ (the Merciless) was developed by
  `SourceForge <http://sourceforge.net/>`_ in the course of their migration to
  MongoDB.
  For more information, see
  `the tutorial <http://merciless.sourceforge.net/orm.html>`_.
* `MongoAlchemy <http://mongoalchemy.org>`_ is inspired by the popular
  `SQLAlchemy <http://sqlalchemy.org>`_ database toolkit.
  For more information, see `the tutorial <http://mongoalchemy.org/tutorial.html>`_.
* `MongoEngine <http://mongoengine.org/>`_ allows you to define schemas
  for documents and query collections using syntax inspired by the Django
  ORM.For more information, see `the tutorial
  <http://mongoengine.org/docs/v0.5/tutorial.html>`_.
* `minimongo <http://pypi.python.org/pypi/minimongo>`_  is a lightweight,
  schemaless, Pythonic Object-Oriented interface to MongoDB. For more
  information see `the example <http://pypi.python.org/pypi/minimongo#example>`_.

Framework Tools
---------------
This section lists tools and adapters that have been designed to work with
various Python frameworks and libraries.

* `Django MongoDB Engine
  <http://django-mongodb.org/>`_ is a MongoDB database backend for Django that
  completely integrates with its ORM. For more information
  `see the tutorial <http://django-mongodb.org/tutorial.html>`_.
* `mango <http://github.com/vpulim/mango>`_ provides MongoDB backends for
  Django sessions and authentication (bypassing :mod:`django.db` entirely).
* `mongodb_beaker <http://pypi.python.org/pypi/mongodb_beaker>`_ is a
  project to enable using MongoDB as a backend for `beaker's
  <http://beaker.groovie.org/>`_ caching / session system.
  `The source is on github <http://github.com/bwmcadams/mongodb_beaker>`_.
* `MongoLog <http://github.com/andreisavu/mongodb-log/>`_ is a Python logging
  handler that stores logs in MongoDB using a capped collection.
* `c5t <http://bitbucket.org/percious/c5t/>`_ is a content-management system
  using TurboGears and MongoDB.
* `rod.recipe.mongodb <http://pypi.python.org/pypi/rod.recipe.mongodb/>`_ is a
  ZC Buildout recipe for downloading and installing MongoDB.
* `repoze-what-plugins-mongodb
  <http://code.google.com/p/repoze-what-plugins-mongodb/>`_ is a project
  working to support a plugin for using MongoDB as a backend for
  :mod:`repoze.what`.

Alternative Drivers
-------------------
These are alternatives to PyMongo.

* `asyncmongo <https://github.com/bitly/asyncmongo>`_ is an asynchronous library
  for accessing mongo which is built on the tornado ioloop.
* `TxMongo <http://github.com/fiorix/mongo-async-python-driver>`_ is an
  asynchronous Python driver for MongoDB, although it is not currently
  recommended for production use.
