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

PyMODM
   `PyMODM <https://pypi.python.org/pypi/pymodm>`_ is an ORM-like framework on top
   of PyMongo. PyMODM is maintained by engineers at MongoDB, Inc. and is quick
   to adopt new MongoDB features. PyMODM is a "core" ODM, meaning that it
   provides simple, extensible functionality that can be leveraged by other
   libraries to target platforms like Django. At the same time, PyMODM is
   powerful enough to be used for developing applications on its own.  Complete
   documentation is available on `readthedocs
   <https://pymodm.readthedocs.io/en/stable/>`_ in addition to a `Gitter channel
   <https://gitter.im/mongodb/pymodm>`_ for discussing the project.

Humongolus
   `Humongolus <https://github.com/entone/Humongolus>`_ is a lightweight ORM
   framework for Python and MongoDB. The name comes from the combination of
   MongoDB and `Homunculus <http://en.wikipedia.org/wiki/Homunculus>`_ (the
   concept of a miniature though fully formed human body). Humongolus allows
   you to create models/schemas with robust validation. It attempts to be as
   pythonic as possible and exposes the pymongo cursor objects whenever
   possible. The code is available for download
   `at GitHub <https://github.com/entone/Humongolus>`_. Tutorials and usage
   examples are also available at GitHub.

Ming
  `Ming <http://merciless.sourceforge.net/>`_ (the Merciless) is a
  library that allows you to enforce schemas on a MongoDB database in
  your Python application. It was developed by `SourceForge
  <http://sourceforge.net/>`_ in the course of their migration to
  MongoDB. See the `introductory blog post
  <http://blog.pythonisito.com/2009/12/ming-01-released-python-library-for.html>`_
  for more details.

MongoEngine
  `MongoEngine <http://mongoengine.org/>`_ is another ORM-like
  layer on top of PyMongo. It allows you to define schemas for
  documents and query collections using syntax inspired by the Django
  ORM. The code is available on `GitHub
  <http://github.com/mongoengine/mongoengine>`_; for more information, see
  the `tutorial <http://docs.mongoengine.org/en/latest/tutorial.html>`_.

MotorEngine
  `MotorEngine <https://motorengine.readthedocs.io/>`_ is a port of
  MongoEngine to Motor, for asynchronous access with Tornado.
  It implements the same modeling APIs to be data-portable, meaning that a
  model defined in MongoEngine can be read in MotorEngine. The source is
  `available on GitHub <http://github.com/heynemann/motorengine>`_.

uMongo
  `uMongo <https://umongo.readthedocs.io/>`_ is a Python MongoDB ODM.
  Its inception comes from two needs: the lack of async ODM and the
  difficulty to do document (un)serialization with existing ODMs.
  Works with multiple drivers: PyMongo, TxMongo, motor_asyncio, and
  mongomock.  The source `is available on GitHub
  <https://github.com/Scille/umongo>`_

No longer maintained
""""""""""""""""""""

MongoKit
  The `MongoKit <http://github.com/namlook/mongokit>`_ framework
  is an ORM-like layer on top of PyMongo. There is also a MongoKit
  `google group <http://groups.google.com/group/mongokit>`_.

MongoAlchemy
  `MongoAlchemy <http://mongoalchemy.org>`_ is another ORM-like layer on top of
  PyMongo. Its API is inspired by `SQLAlchemy <http://sqlalchemy.org>`_. The
  code is available `on GitHub <http://github.com/jeffjenkins/MongoAlchemy>`_;
  for more information, see `the tutorial <http://mongoalchemy.org/tutorial.html>`_.

Minimongo
  `minimongo <http://pypi.python.org/pypi/minimongo>`_ is a lightweight,
  pythonic interface to MongoDB.  It retains pymongo's query and update API,
  and provides a number of additional features, including a simple
  document-oriented interface, connection pooling, index management, and
  collection & database naming helpers. The `source is on GitHub
  <https://github.com/MiniMongo/minimongo>`_.

Manga
  `Manga <http://pypi.python.org/pypi/manga>`_ aims to be a simpler ORM-like
  layer on top of PyMongo. The syntax for defining schema is inspired by the
  Django ORM, but Pymongo's query language is maintained. The source `is on
  GitHub <http://github.com/wladston/manga>`_.

Framework Tools
---------------
This section lists tools and adapters that have been designed to work with
various Python frameworks and libraries.

* `Djongo <https://nesdis.github.io/djongo/>`_ is a connector for using
  Django with MongoDB as the database backend. Use the Django Admin GUI to add and
  modify documents in MongoDB.
  The `Djongo Source Code <https://github.com/nesdis/djongo>`_ is hosted on GitHub
  and the `Djongo package <https://pypi.python.org/pypi/djongo>`_ is on pypi.
* `Django MongoDB Engine
  <https://django-mongodb-engine.readthedocs.io/en/latest/>`_ is a MongoDB
  database backend for Django that completely integrates with its ORM.
  For more information `see the tutorial
  <https://django-mongodb-engine.readthedocs.io/en/latest/tutorial.html>`_.
* `mango <http://github.com/vpulim/mango>`_ provides MongoDB backends for
  Django sessions and authentication (bypassing :mod:`django.db` entirely).
* `Django MongoEngine
  <https://github.com/MongoEngine/django-mongoengine>`_ is a MongoDB backend for
  Django, an `example:
  <https://github.com/MongoEngine/django-mongoengine/tree/master/example/tumblelog>`_.
  For more information `<http://docs.mongoengine.org/en/latest/django.html>`_
* `mongodb_beaker <http://pypi.python.org/pypi/mongodb_beaker>`_ is a
  project to enable using MongoDB as a backend for `beaker's
  <http://beaker.groovie.org/>`_ caching / session system.
  `The source is on GitHub <http://github.com/bwmcadams/mongodb_beaker>`_.
* `Log4Mongo <https://github.com/log4mongo/log4mongo-python>`_ is a flexible
  Python logging handler that can store logs in MongoDB using normal and capped
  collections.
* `MongoLog <http://github.com/puentesarrin/mongodb-log/>`_ is a Python logging
  handler that stores logs in MongoDB using a capped collection.
* `c5t <http://bitbucket.org/percious/c5t/>`_ is a content-management system
  using TurboGears and MongoDB.
* `rod.recipe.mongodb <http://pypi.python.org/pypi/rod.recipe.mongodb/>`_ is a
  ZC Buildout recipe for downloading and installing MongoDB.
* `repoze-what-plugins-mongodb
  <http://code.google.com/p/repoze-what-plugins-mongodb/>`_ is a project
  working to support a plugin for using MongoDB as a backend for
  :mod:`repoze.what`.
* `mongobox <http://github.com/theorm/mongobox>`_ is a tool to run a sandboxed
  MongoDB instance from within a python app.
* `Flask-MongoAlchemy <http://github.com/cobrateam/flask-mongoalchemy/>`_ Add
  Flask support for MongoDB using MongoAlchemy.
* `Flask-MongoKit <http://github.com/jarus/flask-mongokit/>`_ Flask extension
  to better integrate MongoKit into Flask.
* `Flask-PyMongo <http://github.com/dcrosta/flask-pymongo/>`_ Flask-PyMongo
  bridges Flask and PyMongo.

Alternative Drivers
-------------------
These are alternatives to PyMongo.

* `Motor <https://github.com/mongodb/motor>`_ is a full-featured, non-blocking
  MongoDB driver for Python Tornado applications.
* `TxMongo <https://github.com/twisted/txmongo>`_ is an asynchronous Twisted
  Python driver for MongoDB.
* `MongoMock <https://github.com/mongomock/mongomock>`_ is a small
  library to help testing Python code that interacts with MongoDB via
  Pymongo.
