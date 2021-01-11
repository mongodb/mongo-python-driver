PyMongo 4 Migration Guide
=========================

.. contents::

.. testsetup::

  from pymongo import MongoClient, ReadPreference
  client = MongoClient()
  database = client.my_database
  collection = database.my_collection

PyMongo 4.0 brings a number of improvements as well as some backward breaking
changes. This guide provides a roadmap for migrating an existing application
from PyMongo 3.x to 4.x or writing libraries that will work with both
PyMongo 3.x and 4.x.

PyMongo 3
---------

The first step in any successful migration involves upgrading to, or
requiring, at least that latest version of PyMongo 3.x. If your project has a
requirements.txt file, add the line "pymongo >= 3.12, < 4.0" until you have
completely migrated to PyMongo 4. Most of the key new methods and options from
PyMongo 4.0 are backported in PyMongo 3.12 making migration much easier.

.. note:: Users of PyMongo 2.X who wish to upgrade to 4.x must first upgrade
   to PyMongo 3.x by following the :doc:`migrate-to-pymongo3`.

Python 3.6+
-----------

PyMongo 4.0 drops support for Python 2.7, 3.4, and 3.5. Users who wish to
upgrade to 4.x must first upgrade to Python 3.6+. Users upgrading from
Python 2 should consult the :doc:`python3`.

Enable Deprecation Warnings
---------------------------

:exc:`DeprecationWarning` is raised by most methods removed in PyMongo 4.0.
Make sure you enable runtime warnings to see where deprecated functions and
methods are being used in your application::

  python -Wd <your application>

Warnings can also be changed to errors::

  python -Wd -Werror <your application>

.. note:: Not all deprecated features raise :exc:`DeprecationWarning` when
  used. See `Removed features with no migration path`_.

Database
--------

The "eval" method and SystemJS class are removed
................................................

Removed :meth:`~pymongo.database.Database.eval` and
:class:`~pymongo.database.SystemJS`. The eval command was deprecated in
MongoDB 3.0 and removed in MongoDB 4.2.

Code like this::

  >>> result = database.eval('function (x) {return x;}', 3)

can be changed to this with MongoDB <= 4.0::

  >>> from bson.code import Code
  >>> result = database.command('eval', Code('function (x) {return x;}'), args=[3]).get('retval')

Removed features with no migration path
---------------------------------------
