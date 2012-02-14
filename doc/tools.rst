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

Humongolus
   `Humongolus <https://github.com/entone/Humongolus>`_ is a lightweight ORM framework 
   for Python and MongoDB. The name comes from the combination of MongoDB 
   and `Homunculus <http://en.wikipedia.org/wiki/Homunculus>`_ (the concept of a miniature though fully formed human body). 
   Humongolus allows you to create models/schemas with robust validation. 
   It attempts to be as pythonic as possible and exposes the pymongo cursor objects whenever possible. 
   The code is available for download at `GitHub <https://github.com/entone/Humongolus>`_. 
   Tutorials and usage examples are also available at GitHub.

MongoKit
  The `MongoKit <http://github.com/namlook/mongokit>`_ framework
  is an ORM-like layer on top of PyMongo. There is also a MongoKit
  `google group <http://groups.google.com/group/mongokit>`_.

Ming
  `Ming <http://merciless.sourceforge.net/>`_ (the Merciless) is a
  library that allows you to enforce schemas on a MongoDB database in
  your Python application. It was developed by `SourceForge
  <http://sourceforge.net/>`_ in the course of their migration to
  MongoDB. See the `introductory blog post
  <http://blog.pythonisito.com/2009/12/ming-01-released-python-library-for.html>`_
  for more details.

MongoAlchemy
  `MongoAlchemy <http://mongoalchemy.org>`_ is another ORM-like layer on top of
  PyMongo. Its API is inspired by `SQLAlchemy <http://sqlalchemy.org>`_. The
  code is available `on github <http://github.com/jeffjenkins/MongoAlchemy>`_;
  for more information, see `the tutorial <http://mongoalchemy.org/tutorial.html>`_.

MongoEngine
  `MongoEngine <http://mongoengine.org/>`_ is another ORM-like
  layer on top of PyMongo. It allows you to define schemas for
  documents and query collections using syntax inspired by the Django
  ORM. The code is available on `github
  <http://github.com/hmarr/mongoengine>`_; for more information, see
  the `tutorial <http://mongoengine.org/docs/v0.5/tutorial.html>`_.

Minimongo
  `minimongo <http://pypi.python.org/pypi/minimongo>`_ is a lightweight,
  pythonic interface to MongoDB.  It retains pymongo's query and update API,
  and provides a number of additional features, including a simple
  document-oriented interface, connection pooling, index management, and
  collection & database naming helpers. The `source is on github
  <http://github.com/slacy/minimongo>`_.

Framework Tools
---------------
This section lists tools and adapters that have been designed to work with
various Python frameworks and libraries.

* `Django MongoDB Engine
  <http://django-mongodb.org/>`_ is a MongoDB
  database backend for Django that completely integrates with its ORM.
  For more information `see the tutorial <http://django-mongodb.org/tutorial.html>`_.
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
