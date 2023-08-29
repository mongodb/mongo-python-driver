=======
PyMongo
=======
:Info: See `the mongo site <http://www.mongodb.org>`_ for more information. See `GitHub <http://github.com/mongodb/mongo-python-driver>`_ for the latest source.
:Documentation: Available at `pymongo.readthedocs.io <https://pymongo.readthedocs.io/en/stable/>`_
:Author: The MongoDB Python Team

About
=====

The PyMongo distribution contains tools for interacting with MongoDB
database from Python.  The ``bson`` package is an implementation of
the `BSON format <http://bsonspec.org>`_ for Python. The ``pymongo``
package is a native Python driver for MongoDB. The ``gridfs`` package
is a `gridfs
<https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst/>`_
implementation on top of ``pymongo``.

PyMongo supports MongoDB 3.6, 4.0, 4.2, 4.4, 5.0, 6.0, and 7.0.

Support / Feedback
==================

For issues with, questions about, or feedback for PyMongo, please look into
our `support channels <https://support.mongodb.com/welcome>`_. Please
do not email any of the PyMongo developers directly with issues or
questions - you're more likely to get an answer on `StackOverflow <https://stackoverflow.com/questions/tagged/mongodb>`_
(using a "mongodb" tag).

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
<https://www.mongodb.com/docs/manual/tutorial/create-a-vulnerability-report/>`_.

Installation
============

PyMongo can be installed with `pip <http://pypi.python.org/pypi/pip>`_::

  $ python -m pip install pymongo

Or ``easy_install`` from
`setuptools <http://pypi.python.org/pypi/setuptools>`_::

  $ python -m easy_install pymongo

You can also download the project source and do::

  $ pip install .

Do **not** install the "bson" package from pypi. PyMongo comes with its own
bson package; doing "easy_install bson" installs a third-party package that
is incompatible with PyMongo.

Dependencies
============

PyMongo supports CPython 3.7+ and PyPy3.7+.

Required dependencies:

Support for mongodb+srv:// URIs requires `dnspython
<https://pypi.python.org/pypi/dnspython>`_

Optional dependencies:

GSSAPI authentication requires `pykerberos
<https://pypi.python.org/pypi/pykerberos>`_ on Unix or `WinKerberos
<https://pypi.python.org/pypi/winkerberos>`_ on Windows. The correct
dependency can be installed automatically along with PyMongo::

  $ python -m pip install "pymongo[gssapi]"

MONGODB-AWS authentication requires `pymongo-auth-aws
<https://pypi.org/project/pymongo-auth-aws/>`_::

  $ python -m pip install "pymongo[aws]"

OCSP (Online Certificate Status Protocol) requires `PyOpenSSL
<https://pypi.org/project/pyOpenSSL/>`_, `requests
<https://pypi.org/project/requests/>`_, `service_identity
<https://pypi.org/project/service_identity/>`_ and may
require `certifi
<https://pypi.python.org/pypi/certifi>`_::

  $ python -m pip install "pymongo[ocsp]"

Wire protocol compression with snappy requires `python-snappy
<https://pypi.org/project/python-snappy>`_::

  $ python -m pip install "pymongo[snappy]"

Wire protocol compression with zstandard requires `zstandard
<https://pypi.org/project/zstandard>`_::

  $ python -m pip install "pymongo[zstd]"

Client-Side Field Level Encryption requires `pymongocrypt
<https://pypi.org/project/pymongocrypt/>`_ and
`pymongo-auth-aws <https://pypi.org/project/pymongo-auth-aws/>`_::

  $ python -m pip install "pymongo[encryption]"

You can install all dependencies automatically with the following
command::

  $ python -m pip install "pymongo[gssapi,aws,ocsp,snappy,zstd,encryption]"

Additional dependencies are:

- (to generate documentation or run tests) tox_

Examples
========
Here's a basic example (for more see the *examples* section of the docs):

.. code-block:: pycon

  >>> import pymongo
  >>> client = pymongo.MongoClient("localhost", 27017)
  >>> db = client.test
  >>> db.name
  'test'
  >>> db.my_collection
  Collection(Database(MongoClient('localhost', 27017), 'test'), 'my_collection')
  >>> db.my_collection.insert_one({"x": 10}).inserted_id
  ObjectId('4aba15ebe23f6b53b0000000')
  >>> db.my_collection.insert_one({"x": 8}).inserted_id
  ObjectId('4aba160ee23f6b543e000000')
  >>> db.my_collection.insert_one({"x": 11}).inserted_id
  ObjectId('4aba160ee23f6b543e000002')
  >>> db.my_collection.find_one()
  {'x': 10, '_id': ObjectId('4aba15ebe23f6b53b0000000')}
  >>> for item in db.my_collection.find():
  ...     print(item["x"])
  ...
  10
  8
  11
  >>> db.my_collection.create_index("x")
  'x_1'
  >>> for item in db.my_collection.find().sort("x", pymongo.ASCENDING):
  ...     print(item["x"])
  ...
  8
  10
  11
  >>> [item["x"] for item in db.my_collection.find().limit(2).skip(1)]
  [8, 11]

Documentation
=============

Documentation is available at `pymongo.readthedocs.io <https://pymongo.readthedocs.io/en/stable/>`_.

Documentation can be generated by running **tox -m doc**. Generated documentation can be found in the
*doc/build/html/* directory.

Learning Resources
==================

MongoDB Learn - `Python courses <https://learn.mongodb.com/catalog?labels=%5B%22Language%22%5D&values=%5B%22Python%22%5D>`_.
`Python Articles on Developer Center <https://www.mongodb.com/developer/languages/python/>`_.

Testing
=======

The easiest way to run the tests is to run **tox -m test** in
the root of the distribution.

To verify that PyMongo works with Gevent's monkey-patching::

    $ python green_framework_test.py gevent

Or with Eventlet's::

    $ python green_framework_test.py eventlet

.. _tox: https://tox.wiki/en/latest/index.html
