=======
PyMongo
=======
:Info: See `the mongo site <http://www.mongodb.org>`_ for more information. See `github <http://github.com/mongodb/mongo-python-driver/tree>`_ for the latest source.
:Author: Mike Dirolf
:Maintainer: Bernie Hackett <bernie@mongodb.com>

About
=====

The PyMongo distribution contains tools for interacting with MongoDB
database from Python.  The ``bson`` package is an implementation of
the `BSON format <http://bsonspec.org>`_ for Python. The ``pymongo``
package is a native Python driver for MongoDB. The ``gridfs`` package
is a `gridfs
<http://www.mongodb.org/display/DOCS/GridFS+Specification>`_
implementation on top of ``pymongo``.

Support / Feedback
==================

For issues with, questions about, or feedback for PyMongo, please look into
our `support channels <http://www.mongodb.org/about/support>`_. Please
do not email any of the PyMongo developers directly with issues or
questions - you're more likely to get an answer on the `mongodb-user
<http://groups.google.com/group/mongodb-user>`_ list on Google Groups.

Bugs / Feature Requests
=======================

Think you’ve found a bug? Want to see a new feature in PyMongo? Please open a
case in our issue management tool, JIRA:

- `Create an account and login <https://jira.mongodb.org>`_.
- Navigate to `the PYTHON project <https://jira.mongodb.org/browse/PYTHON>`_.
- Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.

Bug reports in JIRA for all driver projects (i.e. PYTHON, CSHARP, JAVA) and the
Core Server (i.e. SERVER) project are **public**.

How To Ask For Help
-------------------

Please include all of the following information when opening an issue:

- Detailed steps to reproduce the problem, including full traceback, if possible.
- The exact python version used, with patch level::

  $ python -c "import sys; print(sys.version)"

- The exact version of PyMongo used, with patch level::

  $ python -c "import pymongo; print(pymongo.version); print(pymongo.has_c())"

- The operating system and version (e.g. Windows 7, OSX 10.8, ...)
- Web framework or asynchronous network library used, if any, with version (e.g.
  Django 1.7, mod_wsgi 4.3.0, gevent 1.0.1, Tornado 4.0.2, ...)

Security Vulnerabilities
------------------------

If you’ve identified a security vulnerability in a driver or any other
MongoDB project, please report it according to the `instructions here
<http://docs.mongodb.org/manual/tutorial/create-a-vulnerability-report>`_.

Installation
============

If you have `setuptools
<http://pythonhosted.org/setuptools/>`_ installed you
should be able to do **easy_install pymongo** to install
PyMongo. Otherwise you can download the project source and do **python
setup.py install** to install.

Do **not** install the "bson" package. PyMongo comes with its own bson package;
doing "easy_install bson" installs a third-party package that is incompatible
with PyMongo.

Dependencies
============

The PyMongo distribution is supported and tested on Python 2.x (where
x >= 6) and Python 3.x (where x >= 2). PyMongo versions before 3.0 also
support Python 2.4, 2.5, and 3.1.

Optional packages:

- `backports.pbkdf2 <https://pypi.python.org/pypi/backports.pbkdf2/>`_,
  improves authentication performance with SCRAM-SHA-1, the default
  authentication mechanism for MongoDB 3.0+. It especially improves
  performance on Python older than 2.7.8, or on Python 3 before Python 3.4.
- `pykerberos <https://pypi.python.org/pypi/pykerberos>`_ is required for
  the GSSAPI authentication mechanism.
- `Monotime <https://pypi.python.org/pypi/Monotime>`_ adds support for
  a monotonic clock, which improves reliability in environments
  where clock adjustments are frequent. Not needed in Python 3.3+.
- `wincertstore <https://pypi.python.org/pypi/wincertstore>`_ adds support
  for verifying server SSL certificates using Windows provided CA
  certificates on older versions of python. Not needed or used with versions
  of Python 2 beginning with 2.7.9, or versions of Python 3 beginning with
  3.4.0.
- `certifi <https://pypi.python.org/pypi/certifi>`_ adds support for
  using the Mozilla CA bundle with SSL to verify server certificates. Not
  needed or used with versions of Python 2 beginning with 2.7.9 on any OS,
  versions of Python 3 beginning with Python 3.4.0 on Windows, or versions
  of Python 3 beginning with Python 3.2.0 on operating systems other than
  Windows.


Additional dependencies are:

- (to generate documentation) sphinx_
- (to run the tests under Python 2.6) unittest2_

Examples
========
Here's a basic example (for more see the *examples* section of the docs):

.. code-block:: pycon

  >>> import pymongo
  >>> client = pymongo.MongoClient("localhost", 27017)
  >>> db = client.test
  >>> db.name
  u'test'
  >>> db.my_collection
  Collection(Database(MongoClient('localhost', 27017), u'test'), u'my_collection')
  >>> db.my_collection.insert_one({"x": 10}).inserted_id
  ObjectId('4aba15ebe23f6b53b0000000')
  >>> db.my_collection.insert_one({"x": 8}).inserted_id
  ObjectId('4aba160ee23f6b543e000000')
  >>> db.my_collection.insert_one({"x": 11}).inserted_id
  ObjectId('4aba160ee23f6b543e000002')
  >>> db.my_collection.find_one()
  {u'x': 10, u'_id': ObjectId('4aba15ebe23f6b53b0000000')}
  >>> for item in db.my_collection.find():
  ...     print item["x"]
  ...
  10
  8
  11
  >>> db.my_collection.create_index("x")
  u'x_1'
  >>> for item in db.my_collection.find().sort("x", pymongo.ASCENDING):
  ...     print item["x"]
  ...
  8
  10
  11
  >>> [item["x"] for item in db.my_collection.find().limit(2).skip(1)]
  [8, 11]

Documentation
=============

You will need sphinx_ installed to generate the
documentation. Documentation can be generated by running **python
setup.py doc**. Generated documentation can be found in the
*doc/build/html/* directory.

Testing
=======

The easiest way to run the tests is to run **python setup.py test** in
the root of the distribution. Note that you will need unittest2_ to
run the tests under Python 2.6.

To verify that PyMongo works with Gevent's monkey-patching::

    $ python green_framework_test.py gevent

Or with Eventlet's::

    $ python green_framework_test.py eventlet

.. _sphinx: http://sphinx.pocoo.org/
.. _unittest2: https://pypi.python.org/pypi/unittest2
