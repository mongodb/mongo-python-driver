Copying a Database
==================

Raw command
-----------

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

``copy_database`` method
------------------------

The current version of PyMongo provides a helper method,
:meth:`~pymongo.mongo_client.MongoClient.copy_database`, to copy a database
from a password-protected mongod server to the target server.
This method is deprecated and will be removed in PyMongo 3.0.
Use the `copyDatabase function in the mongo shell`_ instead.

Until the method is removed from PyMongo, you can copy a database from a
password-protected server like so::

  >>> client = MongoClient('target.example.com')
  >>> client.copy_database(from_name='source_db_name',
                           to_name='target_db_name',
                           from_host='source.example.com',
                           username='jesse',
                           password='pwd',
                           mechanism='SCRAM-SHA-1')

Provide the username and password of a user who is authorized to read the
source database on the source host. Again, if the target database is also
password-protected, authenticate to the "admin" database first.

The mechanism can be "MONGODB-CR" or "SCRAM-SHA-1". Use SCRAM-SHA-1 if the
target and source hosts are both MongoDB 2.8 or later, otherwise use
MONGODB-CR.

If no mechanism is specified, PyMongo tries to use MONGODB-CR when
connected to a pre-2.8 version of MongoDB, and SCRAM-SHA-1 when connected to
a recent version. However, since PyMongo cannot determine the MongoDB
version of the **source** host, it is better if you specify a mechanism
yourself.

.. _copyDatabase function in the mongo shell:
   http://docs.mongodb.org/manual/reference/method/db.copyDatabase/
