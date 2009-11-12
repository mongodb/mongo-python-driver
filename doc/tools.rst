Tools
=====
Many tools have been written for working with **PyMongo**. If you know
of or have created a tool for working with MongoDB from Python please
list it here.

ORM-like Layers
---------------
MongoKit
  The `MongoKit <http://bitbucket.org/namlook/mongokit/>`_ framework
  is an ORM-like layer on top of PyMongo. There is also a MongoKit
  `google group <http://groups.google.com/group/mongokit>`_.

pymongo-bongo
  `pymongo-bongo <http://pypi.python.org/pypi/pymongo-bongo/>`_ is a
  project to add some syntactic sugar on top of PyMongo. It is open
  source and the code is available on `github
  <http://github.com/svetlyak40wt/pymongo-bongo>`_.

mongodb-object
  `mongodb-object
  <http://github.com/marcboeker/mongodb-object/tree/master>`_ is a
  "cocktail of the Django ORM mixed with JavaScript dot object
  notation". It features some interesting notation for document
  traversal and querying, see the README for more info.

django-mongodb
  `django-mongodb <http://bitbucket.org/kpot/django-mongodb/>`_ is a
  project working towards creating a MongoDB backend for
  :mod:`django.db`.

mongo-mapper
  `mongo-mapper
  <http://github.com/jeffjenkins/mongo-mapper/tree/master>`_ is
  another ORM-like layer on top of PyMongo with a minimalist attitude.

MongoMagic
  `MongoMagic <http://bitbucket.org/bottiger/mongomagic/wiki/Home>`_
  is another project to provide an ORM-like layer for PyMongo. Its
  first feature is a :class:`Document` class that provides attribute style
  access (similar to JavaScript).

Other Tools
-----------
mongodb_beaker
  `mongodb_beaker <http://pypi.python.org/pypi/mongodb_beaker>`_ is a
  project to enable using MongoDB as a backend for `beaker's
  <http://beaker.groovie.org/>`_ caching / session system. The
  `source is on bitbucket
  <http://bitbucket.org/bwmcadams/mongodb_beaker/>`_.

rod.recipe.mongodb
  `rod.recipe.mongodb
  <http://pypi.python.org/pypi/rod.recipe.mongodb/>`_ is a ZC Buildout
  recipe for downloading and installing MongoDB.

repoze-what-plugins-mongodb
  `repoze-what-plugins-mongodb
  <http://code.google.com/p/repoze-what-plugins-mongodb/>`_ is a
  project working to support a plugin for using MongoDB as a backend
  for :mod:`repoze.what`.

MongoLog
  `MongoLog <http://github.com/andreisavu/mongodb-log/tree/master>`_
  is a Python logging handler that stores logs in MongoDB using a
  capped collection.

Alternative Drivers
-------------------
PyMonga
  `PyMonga <http://github.com/fiorix/mongo-async-python-driver>`_ is
  an asynchronous Python driver for MongoDB, although it is not
  currently recommended for production use.
