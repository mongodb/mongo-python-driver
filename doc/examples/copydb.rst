Copying a Database
==================

To copy a database within a single mongod process, or between mongod
servers, simply connect to the target mongod and use the
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
database first::

  >>> client.admin.authenticate('administrator', 'pwd')
  True
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
   http://docs.mongodb.org/manual/reference/method/db.copyDatabase/
