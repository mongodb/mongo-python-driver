Copying a Database
==================

MongoDB >= 4.2
--------------

Starting in MongoDB version 4.2, the server removes the deprecated ``copydb`` command.
As an alternative, users can use ``mongodump`` and ``mongorestore`` (with the ``mongorestore``
options ``--nsFrom`` and ``--nsTo``).

For example, to copy the ``test`` database from a local instance running on the
default port 27017 to the ``examples`` database on the same instance, you can:

#. Use ``mongodump`` to dump the test database to an archive ``mongodump-test-db``::

    mongodump --archive="mongodump-test-db" --db=test

#. Use ``mongorestore`` with ``--nsFrom`` and ``--nsTo`` to restore (with database name change)
   from the archive::

    mongorestore --archive="mongodump-test-db" --nsFrom='test.*' --nsTo='examples.*'

Include additional options as necessary, such as to specify the uri or host, username,
password and authentication database.

For more info about using ``mongodump`` and ``mongorestore`` see the `Copy a Database`_ example
in the official ``mongodump`` documentation.

MongoDB <= 4.0
--------------

When using MongoDB <= 4.0, it is possible to use the deprecated ``copydb`` command
to copy a database. To copy a database within a single ``mongod`` process, or
between ``mongod`` servers, connect to the target ``mongod`` and use the
:meth:`~pymongo.database.Database.command` method::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('target.example.com')
  >>> client.admin.command('copydb',
                           fromdb='source_db_name',
                           todb='target_db_name')

To copy from a different mongod server that is not password-protected::

  >>> client.admin.command('copydb',
                           fromdb='source_db_name',
                           todb='target_db_name',
                           fromhost='source.example.com')

If the target server is password-protected, authenticate to the "admin"
database::

  >>> client = MongoClient('target.example.com',
  ...                      username='administrator',
  ...                      password='pwd')
  >>> client.admin.command('copydb',
                           fromdb='source_db_name',
                           todb='target_db_name',
                           fromhost='source.example.com')

See the :doc:`authentication examples </examples/authentication>`.

If the **source** server is password-protected, use the `copyDatabase
function in the mongo shell`_.

Versions of PyMongo before 3.0 included a ``copy_database`` helper method,
but it has been removed.

.. _copyDatabase function in the mongo shell:
   http://mongodb.com/docs/manual/reference/method/db.copyDatabase/

.. _Copy a Database:
   https://www.mongodb.com/docs/database-tools/mongodump/#std-label-mongodump-example-copy-clone-database
