Bulk Write Operations
=====================

.. testsetup::

  from pymongo import MongoClient
  client = MongoClient()
  client.drop_database('bulk_example')

This tutorial explains how to take advantage of PyMongo's bulk
write operation features. Executing write operations in batches
reduces the number of network round trips, increasing write
throughput.

Bulk Insert
-----------

.. versionadded:: 2.6

A batch of documents can be inserted by passing a list or generator
to the :meth:`~pymongo.collection.Collection.insert` method. PyMongo
will automatically split the batch into smaller sub-batches based on
the maximum message size accepted by MongoDB, supporting very large
bulk insert operations.

.. doctest::

  >>> import pymongo
  >>> db = pymongo.MongoClient().bulk_example
  >>> db.test.insert(({'i': i} for i in xrange(10000)))
  [...]
  >>> db.test.count()
  10000

Mixed Bulk Write Operations
---------------------------

.. versionadded:: 2.7

PyMongo also supports executing mixed bulk write operations. A batch
of insert, update, and remove operations can be executed together using
the bulk write operations API.

.. note::

  Though the following API will work with all versions of MongoDB, it is
  designed to be used with MongoDB versions >= 2.6. Much better bulk insert
  performance can be achieved with older versions of MongoDB through the
  :meth:`~pymongo.collection.Collection.insert` method.

.. _ordered_bulk:

Ordered Bulk Write Operations
.............................

Ordered bulk write operations are batched and sent to the server in the
order provided for serial execution. The return value is a document
describing the type and count of operations performed.

.. doctest::

  >>> from pprint import pprint
  >>>
  >>> bulk = db.test.initialize_ordered_bulk_op()
  >>> # Remove all documents from the previous example.
  ...
  >>> bulk.find({}).remove()
  >>> bulk.insert({'_id': 1})
  >>> bulk.insert({'_id': 2})
  >>> bulk.insert({'_id': 3})
  >>> bulk.find({'_id': 1}).update({'$set': {'foo': 'bar'}})
  >>> bulk.find({'_id': 4}).upsert().update({'$inc': {'j': 1}})
  >>> bulk.find({'j': 1}).replace_one({'j': 2})
  >>> result = bulk.execute()
  >>> pprint(result)
  {'nInserted': 3,
   'nMatched': 2,
   'nModified': 2,
   'nRemoved': 10000,
   'nUpserted': 1,
   'upserted': [{u'_id': 4, u'index': 5}],
   'writeConcernErrors': [],
   'writeErrors': []}
  >>>

.. warning:: ``nModified`` is only reported by MongoDB 2.6 and later. When
  connected to an earlier server version, or in certain mixed version sharding
  configurations, PyMongo omits this field from the results of a bulk
  write operation.

The first write failure that occurs (e.g. duplicate key error) aborts the
remaining operations, and PyMongo raises
:class:`~pymongo.errors.BulkWriteError`. The :attr:`details` attibute of
the exception instance provides the execution results up until the failure
occurred and details about the failure - including the operation that caused
the failure.

.. doctest::

  >>> from pymongo.errors import BulkWriteError
  >>> bulk = db.test.initialize_ordered_bulk_op()
  >>> bulk.find({'j': 2}).replace_one({'i': 5})
  >>> # Violates the unique key constraint on _id.
  ...
  >>> bulk.insert({'_id': 4})
  >>> bulk.find({'i': 5}).remove_one()
  >>> try:
  ...     bulk.execute()
  ... except BulkWriteError as bwe:
  ...     pprint(bwe.details)
  ... 
  {'nInserted': 0,
   'nMatched': 1,
   'nModified': 1,
   'nRemoved': 0,
   'nUpserted': 0,
   'upserted': [],
   'writeConcernErrors': [],
   'writeErrors': [{u'code': 11000,
                    u'errmsg': u'insertDocument :: caused by :: 11000 E11000 duplicate key error index: bulk_example.test.$_id_  dup key: { : 4 }',
                    u'index': 1,
                    u'op': {'_id': 4}}]}
  >>>

.. _unordered_bulk:

Unordered Bulk Write Operations
...............................

Unordered bulk write operations are batched and sent to the server in
**arbitrary order** where they may be executed in parallel. Any errors
that occur are reported after all operations are attempted.

In the next example the first and third operations fail due to the unique
constraint on _id. Since we are doing unordered execution the second
and fourth operations succeed.

.. doctest::

  >>> bulk = db.test.initialize_unordered_bulk_op()
  >>> bulk.insert({'_id': 1})
  >>> bulk.find({'_id': 2}).remove_one()
  >>> bulk.insert({'_id': 3})
  >>> bulk.find({'_id': 4}).replace_one({'i': 1})
  >>> try:
  ...     bulk.execute()
  ... except BulkWriteError as bwe:
  ...     pprint(bwe.details)
  ... 
  {'nInserted': 0,
   'nMatched': 1,
   'nModified': 1,
   'nRemoved': 1,
   'nUpserted': 0,
   'upserted': [],
   'writeConcernErrors': [],
   'writeErrors': [{u'code': 11000,
                    u'errmsg': u'insertDocument :: caused by :: 11000 E11000 duplicate key error index: bulk_example.test.$_id_  dup key: { : 1 }',
                    u'index': 0,
                    u'op': {'_id': 1}},
                   {u'code': 11000,
                    u'errmsg': u'insertDocument :: caused by :: 11000 E11000 duplicate key error index: bulk_example.test.$_id_  dup key: { : 3 }',
                    u'index': 2,
                    u'op': {'_id': 3}}]}
  >>>

Write Concern
.............

By default bulk operations are executed with the
:attr:`~pymongo.collection.Collection.write_concern` of the collection they
are executed against. A custom write concern can be passed to the
:meth:`~pymongo.bulk.BulkOperationBuilder.execute` method. Write concern
errors (e.g. wtimeout) will be reported after all operations are attempted,
regardless of execution order.

.. doctest::

  >>> bulk = db.test.initialize_ordered_bulk_op()
  >>> bulk.insert({'a': 0})
  >>> bulk.insert({'a': 1})
  >>> bulk.insert({'a': 2})
  >>> bulk.insert({'a': 3})
  >>> try:
  ...     bulk.execute({'w': 4, 'wtimeout': 1})
  ... except BulkWriteError as bwe:
  ...     pprint(bwe.details)
  ... 
  {'nInserted': 4,
   'nMatched': 0,
   'nModified': 0,
   'nRemoved': 0,
   'nUpserted': 0,
   'upserted': [],
   'writeConcernErrors': [{u'code': 64,
                           u'errInfo': {u'wtimeout': True},
                           u'errmsg': u'waiting for replication timed out'}],
   'writeErrors': []}
  >>>

