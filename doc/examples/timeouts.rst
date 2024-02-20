
.. _timeout-example:

Client Side Operation Timeout
=============================

PyMongo 4.2 introduced :meth:`~pymongo.timeout` and the ``timeoutMS``
URI and keyword argument to :class:`~pymongo.mongo_client.MongoClient`.
These features allow applications to more easily limit the amount of time that
one or more operations can execute before control is returned to the app. This
timeout applies to all of the work done to execute the operation, including
but not limited to server selection, connection checkout, serialization, and
server-side execution.

Basic Usage
-----------

The following example uses :meth:`~pymongo.timeout` to configure a 10-second
timeout for an :meth:`~pymongo.collection.Collection.insert_one` operation::

  import pymongo
  with pymongo.timeout(10):
      coll.insert_one({"name": "Nunu"})

The :meth:`~pymongo.timeout` applies to all pymongo operations within the block.
The following example ensures that both the ``insert`` and the ``find`` complete
within 10 seconds total, or raise a timeout error::

  with pymongo.timeout(10):
      coll.insert_one({"name": "Nunu"})
      coll.find_one({"name": "Nunu"})

When nesting :func:`~pymongo.timeout`, the nested deadline is capped by the outer
deadline. The deadline can only be shortened, not extended.
When exiting the block, the previous deadline is restored::

  with pymongo.timeout(5):
      coll.find_one()  # Uses the 5 second deadline.
      with pymongo.timeout(3):
          coll.find_one() # Uses the 3 second deadline.
      coll.find_one()  # Uses the original 5 second deadline.
      with pymongo.timeout(10):
          coll.find_one()  # Still uses the original 5 second deadline.
      coll.find_one()  # Uses the original 5 second deadline.

Timeout errors
--------------

When the :meth:`~pymongo.timeout` with-statement is entered, a deadline is set
for the entire block. When that deadline is exceeded, any blocking pymongo operation
will raise a timeout exception. For example::

  try:
      with pymongo.timeout(10):
          coll.insert_one({"name": "Nunu"})
          time.sleep(10)
          # The deadline has now expired, the next operation will raise
          # a timeout exception.
          coll.find_one({"name": "Nunu"})
  except PyMongoError as exc:
      if exc.timeout:
          print(f"block timed out: {exc!r}")
      else:
          print(f"failed with non-timeout error: {exc!r}")

The :attr:`pymongo.errors.PyMongoError.timeout` property (added in PyMongo 4.2)
will be ``True`` when the error was caused by a timeout and ``False`` otherwise.

The timeoutMS URI option
------------------------

PyMongo 4.2 also added support for the ``timeoutMS`` URI and keyword argument to
:class:`~pymongo.mongo_client.MongoClient`. When this option is configured, the
client will automatically apply the timeout to each API call. For example::

  client = MongoClient("mongodb://localhost/?timeoutMS=10000")
  coll = client.test.test
  coll.insert_one({"name": "Nunu"})  # Uses a 10-second timeout.
  coll.find_one({"name": "Nunu"})  # Also uses a 10-second timeout.

The above is roughly equivalent to::

  client = MongoClient()
  coll = client.test.test
  with pymongo.timeout(10):
      coll.insert_one({"name": "Nunu"})
  with pymongo.timeout(10):
      coll.find_one({"name": "Nunu"})

pymongo.timeout overrides timeoutMS
-----------------------------------

:meth:`~pymongo.timeout` overrides ``timeoutMS``; within a
:meth:`~pymongo.timeout` block a client's ``timeoutMS`` option is ignored::

  client = MongoClient("mongodb://localhost/?timeoutMS=10000")
  coll = client.test.test
  coll.insert_one({"name": "Nunu"})  # Uses the client's 10-second timeout.
  # pymongo.timeout overrides the client's timeoutMS.
  with pymongo.timeout(20):
      coll.insert_one({"name": "Nunu"})  # Uses the 20-second timeout.
  with pymongo.timeout(5):
      coll.find_one({"name": "Nunu"})  # Uses the 5-second timeout.

pymongo.timeout is thread safe
------------------------------

:meth:`~pymongo.timeout` is thread safe; the timeout only applies to current
thread and multiple threads can configure different timeouts in parallel.

pymongo.timeout is asyncio safe
-------------------------------

:meth:`~pymongo.timeout` is asyncio safe; the timeout only applies to current
Task and multiple Tasks can configure different timeouts concurrently.
:meth:`~pymongo.timeout` can be used identically in
`Motor <https://github.com/mongodb/motor>`_, for example::

  import motor.motor_asyncio
  client = motor.motor_asyncio.AsyncIOMotorClient()
  coll = client.test.test
  with pymongo.timeout(10):
      await coll.insert_one({"name": "Nunu"})
      await coll.find_one({"name": "Nunu"})

Troubleshooting
---------------

There are many timeout errors that can be raised depending on when the timeout
expires. In code, these can be identified with the :attr:`pymongo.errors.PyMongoError.timeout`
property. Some specific timeout errors examples are described below.

When the client was unable to find an available server to run the operation
within the given timeout::

  pymongo.errors.ServerSelectionTimeoutError: No servers found yet, Timeout: -0.00202266700216569s, Topology Description: <TopologyDescription id: 63698e87cebfd22ab1bd2ae0, topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, rtt: None>]>

When either the client was unable to establish a connection within the given
timeout or the operation was sent but the server was not able to respond in time::

  pymongo.errors.NetworkTimeout: localhost:27017: timed out

When the server cancelled the operation because it exceeded the given timeout.
Note that the operation may have partially completed on the server (depending
on the operation)::

  pymongo.errors.ExecutionTimeout: operation exceeded time limit, full error: {'ok': 0.0, 'errmsg': 'operation exceeded time limit', 'code': 50, 'codeName': 'MaxTimeMSExpired'}

When the client cancelled the operation because it was not possible to complete
within the given timeout::

  pymongo.errors.ExecutionTimeout: operation would exceed time limit, remaining timeout:0.00196 <= network round trip time:0.00427

When the client attempted a write operation but the server could not replicate
that write (according to the configured write concern) within the given timeout::

  pymongo.errors.WTimeoutError: operation exceeded time limit, full error: {'code': 50, 'codeName': 'MaxTimeMSExpired', 'errmsg': 'operation exceeded time limit', 'errInfo': {'writeConcern': {'w': 1, 'wtimeout': 0}}}

The same error as above but for :meth:`~pymongo.collection.Collection.insert_many`
or :meth:`~pymongo.collection.Collection.bulk_write`::

  pymongo.errors.BulkWriteError: batch op errors occurred, full error: {'writeErrors': [], 'writeConcernErrors': [{'code': 50, 'codeName': 'MaxTimeMSExpired', 'errmsg': 'operation exceeded time limit', 'errInfo': {'writeConcern': {'w': 1, 'wtimeout': 0}}}], 'nInserted': 2, 'nUpserted': 0, 'nMatched': 0, 'nModified': 0, 'nRemoved': 0, 'upserted': []}
