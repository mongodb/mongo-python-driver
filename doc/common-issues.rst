Frequently Encountered Issues
=============================

Also see the :ref:`TLSErrors` section.

.. contents::

Server reports wire version X, PyMongo requires Y
-------------------------------------------------

When one attempts to connect to a <=3.4 version server, PyMongo will throw the following error::

  >>> client.admin.command('ping')
  ...
  pymongo.errors.ConfigurationError: Server at localhost:27017 reports wire version 5, but this version of PyMongo requires at least 6 (MongoDB 3.6).

This is caused by the driver being too new for the server it is being run against.
To resolve this issue either upgrade your database to version >= 3.6 or downgrade to PyMongo 3.x which supports MongoDB >= 2.6.


'Cursor' object has no attribute '_Cursor__killed'
--------------------------------------------------

On versions of PyMongo <3.9, when supplying invalid arguments the constructor of Cursor,
there will be a TypeError raised, and an AttributeError printed to ``stderr``. The AttributeError is not relevant,
instead look at the TypeError for debugging information::

  >>> coll.find(wrong=1)
  Exception ignored in: <function Cursor.__del__ at 0x1048129d8>
  ...
  AttributeError: 'Cursor' object has no attribute '_Cursor__killed'
  ...
  TypeError: __init__() got an unexpected keyword argument 'wrong'

To fix this, make sure that you are supplying the correct keyword arguments.
In addition, you can also upgrade to PyMongo >=3.9, which will remove the spurious error.


MongoClient fails ConfigurationError
------------------------------------

This is a common issue stemming from using incorrect keyword argument names.

  >>> client = MongoClient(wrong=1)
  ...
  pymongo.errors.ConfigurationError: Unknown option wrong

To fix this, check your spelling and make sure that the keyword argument you are specifying exists.


DeprecationWarning: count is deprecated
---------------------------------------

PyMongo no longer supports :meth:`pymongo.cursor.count`.
Instead, use :meth:`pymongo.collection.count_documents`::

  >>> client = MongoClient()
  >>> d = datetime.datetime(2009, 11, 12, 12)
  >>> list(client.db.coll.find({"date": {"$lt": d}}, limit=2))
  [{'_id': ObjectId('6247b058cebb8b179b7039f8'), 'date': datetime.datetime(1, 1, 1, 0, 0)}, {'_id': ObjectId('6247b059cebb8b179b7039f9'), 'date': datetime.datetime(1, 1, 1, 0, 0)}]
  >>> client.db.coll.count_documents({"date": {"$lt": d}}, limit=2)
  2

Note that this is NOT the same as ``Cursor.count_documents`` (which does not exist),
this is a method of the Collection class, so you must call it on a collection object
or you will receive the following error::

  >>> Cursor(MongoClient().db.coll).count()
  Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
  AttributeError: 'Cursor' object has no attribute 'count'
  >>>

Timeout when accessing MongoDB from PyMongo with tunneling
----------------------------------------------------------

When attempting to connect to a replica set MongoDB instance over an SSH tunnel you
will receive the following error::

  File "/Library/Python/2.7/site-packages/pymongo/collection.py", line 1560, in count
    return self._count(cmd, collation, session)
    File "/Library/Python/2.7/site-packages/pymongo/collection.py", line 1504, in _count
    with self._socket_for_reads() as (sock_info, slave_ok):
    File "/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/contextlib.py", line 17, in __enter__
    return self.gen.next()
    File "/Library/Python/2.7/site-packages/pymongo/mongo_client.py", line 982, in _socket_for_reads
    server = topology.select_server(read_preference)
    File "/Library/Python/2.7/site-packages/pymongo/topology.py", line 224, in select_server
    address))
    File "/Library/Python/2.7/site-packages/pymongo/topology.py", line 183, in select_servers
    selector, server_timeout, address)
    File "/Library/Python/2.7/site-packages/pymongo/topology.py", line 199, in _select_servers_loop
    self._error_message(selector))
  pymongo.errors.ServerSelectionTimeoutError: localhost:27017: timed out

This is due to the fact that PyMongo discovers replica set members using the response from the isMaster command which
then contains the address and ports of the other members. However, these addresses and ports will not be accessible through the SSH tunnel. Thus, this behavior is unsupported.
You can, however, connect directly to a single MongoDB node using the directConnection=True option with SSH tunneling.
