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

A batch of documents can be inserted by passing a list to the
:meth:`~pymongo.collection.Collection.insert_many` method. PyMongo
will automatically split the batch into smaller sub-batches based on
the maximum message size accepted by MongoDB, supporting very large
bulk insert operations.

.. doctest::

  >>> import pymongo
  >>> db = pymongo.MongoClient().bulk_example
  >>> db.test.insert_many([{'i': i} for i in range(10000)]).inserted_ids
  [...]
  >>> db.test.count_documents({})
  10000

Mixed Bulk Write Operations
---------------------------

.. versionadded:: 2.7

PyMongo also supports executing mixed bulk write operations. A batch
of insert, update, and remove operations can be executed together using
the bulk write operations API.

.. _ordered_bulk:

Ordered Bulk Write Operations
.............................

Ordered bulk write operations are batched and sent to the server in the
order provided for serial execution. The return value is an instance of
:class:`~pymongo.results.BulkWriteResult` describing the type and count
of operations performed.

.. doctest::
  :options: +NORMALIZE_WHITESPACE

  >>> from pprint import pprint
  >>> from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
  >>> result = db.test.bulk_write([
  ...     DeleteMany({}),  # Remove all documents from the previous example.
  ...     InsertOne({'_id': 1}),
  ...     InsertOne({'_id': 2}),
  ...     InsertOne({'_id': 3}),
  ...     UpdateOne({'_id': 1}, {'$set': {'foo': 'bar'}}),
  ...     UpdateOne({'_id': 4}, {'$inc': {'j': 1}}, upsert=True),
  ...     ReplaceOne({'j': 1}, {'j': 2})])
  >>> pprint(result.bulk_api_result)
  {'nInserted': 3,
   'nMatched': 2,
   'nModified': 2,
   'nRemoved': 10000,
   'nUpserted': 1,
   'upserted': [{'_id': 4, 'index': 5}],
   'writeConcernErrors': [],
   'writeErrors': []}

The first write failure that occurs (e.g. duplicate key error) aborts the
remaining operations, and PyMongo raises
:class:`~pymongo.errors.BulkWriteError`. The :attr:`details` attibute of
the exception instance provides the execution results up until the failure
occurred and details about the failure - including the operation that caused
the failure.

.. doctest::
  :options: +NORMALIZE_WHITESPACE

  >>> from pymongo import InsertOne, DeleteOne, ReplaceOne
  >>> from pymongo.errors import BulkWriteError
  >>> requests = [
  ...     ReplaceOne({'j': 2}, {'i': 5}),
  ...     InsertOne({'_id': 4}),  # Violates the unique key constraint on _id.
  ...     DeleteOne({'i': 5})]
  >>> try:
  ...     db.test.bulk_write(requests)
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
   'writeErrors': [{'code': 11000,
                    'errmsg': '...E11000...duplicate key error...',
                    'index': 1,...
                    'op': {'_id': 4}}]}

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
  :options: +NORMALIZE_WHITESPACE

  >>> requests = [
  ...     InsertOne({'_id': 1}),
  ...     DeleteOne({'_id': 2}),
  ...     InsertOne({'_id': 3}),
  ...     ReplaceOne({'_id': 4}, {'i': 1})]
  >>> try:
  ...     db.test.bulk_write(requests, ordered=False)
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
   'writeErrors': [{'code': 11000,
                    'errmsg': '...E11000...duplicate key error...',
                    'index': 0,...
                    'op': {'_id': 1}},
                   {'code': 11000,
                    'errmsg': '...E11000...duplicate key error...',
                    'index': 2,...
                    'op': {'_id': 3}}]}

Write Concern
.............

Bulk operations are executed with the
:attr:`~pymongo.collection.Collection.write_concern` of the collection they
are executed against. Write concern errors (e.g. wtimeout) will be reported
after all operations are attempted, regardless of execution order.

::
  >>> from pymongo import WriteConcern
  >>> coll = db.get_collection(
  ...     'test', write_concern=WriteConcern(w=3, wtimeout=1))
  >>> try:
  ...     coll.bulk_write([InsertOne({'a': i}) for i in range(4)])
  ... except BulkWriteError as bwe:
  ...     pprint(bwe.details)
  ...
  {'nInserted': 4,
   'nMatched': 0,
   'nModified': 0,
   'nRemoved': 0,
   'nUpserted': 0,
   'upserted': [],
   'writeConcernErrors': [{'code': 64...
                           'errInfo': {'wtimeout': True},
                           'errmsg': 'waiting for replication timed out'}],
   'writeErrors': []}
