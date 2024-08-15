Client Bulk Write Operations
=============================

.. testsetup::

  from pymongo import MongoClient

  client = MongoClient()
  client.drop_database("client_bulk_example")
  db = client.client_bulk_example
  client.db.drop_collection("test_one")
  client.db.drop_collection("test_two")
  client.db.drop_collection("test_three")
  client.db.drop_collection("test_four")
  client.db.drop_collection("test_five")
  client.db.drop_collection("test_six")

The :meth:`~pymongo.mongo_client.MongoClient.bulk_write`
method has been added to :class:`~pymongo.mongo_client.MongoClient` in PyMongo 4.9.
This method enables users to perform batches of write operations **across
multiple namespaces** in a minimized number of round trips, and
to receive detailed results for each operation performed.

.. note:: This method requires MongoDB server version 8.0+.

Basic Usage
------------

A list of insert, update, and delete operations can be passed into the
:meth:`~pymongo.mongo_client.MongoClient.bulk_write` method. Each request
must include the namespace on which to perform the operation.

PyMongo will automatically split the given requests into smaller sub-batches based on
the maximum message size accepted by MongoDB, supporting very large bulk write operations.

The return value is an instance of
:class:`~pymongo.results.ClientBulkWriteResult`.

.. _summary_client_bulk:

Summary Results
.................

By default, the returned :class:`~pymongo.results.ClientBulkWriteResult` instance will contain a
summary of the types of operations performed in the bulk write, along with their respective counts.

.. doctest::
  :options: +NORMALIZE_WHITESPACE

  >>> from pymongo import InsertOne, DeleteOne, UpdateOne
  >>> models = [
  ...     InsertOne(namespace="db.test_one", document={"_id": 1}),
  ...     InsertOne(namespace="db.test_two", document={"_id": 2}),
  ...     DeleteOne(namespace="db.test_one", filter={"_id": 1}),
  ...     UpdateOne(
  ...         namespace="db.test_two",
  ...         filter={"_id": 4},
  ...         update={"$inc": {"j": 1}},
  ...         upsert=True,
  ...     ),
  ... ]
  >>> result = client.bulk_write(models)
  >>> result.inserted_count
  2
  >>> result.deleted_count
  1
  >>> result.modified_count
  0
  >>> result.upserted_count
  1

.. _verbose_client_bulk:

Verbose Results
.................

If the ``verbose_results`` parameter is set to True, the returned :class:`~pymongo.results.ClientBulkWriteResult`
instance will also include detailed results about each successful operation performed as part of the bulk write.

.. doctest::
  :options: +NORMALIZE_WHITESPACE

  >>> from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateMany
  >>> models = [
  ...     DeleteMany(
  ...         namespace="db.test_two", filter={}
  ...     ),  # Delete all documents from the previous example
  ...     InsertOne(namespace="db.test_one", document={"_id": 1}),
  ...     InsertOne(namespace="db.test_one", document={"_id": 2}),
  ...     InsertOne(namespace="db.test_two", document={"_id": 3}),
  ...     UpdateMany(namespace="db.test_one", filter={}, update={"$set": {"foo": "bar"}}),
  ...     ReplaceOne(
  ...         namespace="db.test_two", filter={"j": 1}, replacement={"_id": 4}, upsert=True
  ...     ),
  ... ]
  >>> result = client.bulk_write(models, verbose_results=True)
  >>> result.delete_results
  {0: DeleteResult({'ok': 1.0, 'idx': 0, 'n': 2}, ...)}
  >>> result.insert_results
  {1: InsertOneResult(1, ...),
   2: InsertOneResult(2, ...),
   3: InsertOneResult(3, ...)}
  >>> result.update_results
  {4: UpdateResult({'ok': 1.0, 'idx': 4, 'n': 2, 'nModified': 2}, ...),
   5: UpdateResult({'ok': 1.0, 'idx': 5, 'n': 1, 'nModified': 0, 'upserted': {'_id': 4}}, ...)}


Handling Errors
----------------

If any errors occur during the bulk write, a :class:`~pymongo.errors.ClientBulkWriteException` will be raised.
If a server, connection, or network error occurred, the ``error`` field of the exception will contain
that error.

Individual write errors or write concern errors get recorded in the ``write_errors`` and ``write_concern_errors`` fields of the exception.
The ``partial_result`` field gets populated with the results of any operations that were successfully completed before the exception was raised.

.. _ordered_client_bulk:

Ordered Operations
....................

In an ordered bulk write (the default), if an individual write fails, no further operations will get executed.
For example, a duplicate key error on the third operation below aborts the remaining two operations.

.. doctest::
  :options: +NORMALIZE_WHITESPACE

  >>> from pymongo import InsertOne, DeleteOne
  >>> from pymongo.errors import ClientBulkWriteException
  >>> models = [
  ...     InsertOne(namespace="db.test_three", document={"_id": 3}),
  ...     InsertOne(namespace="db.test_four", document={"_id": 4}),
  ...     InsertOne(namespace="db.test_three", document={"_id": 3}),  # Duplicate _id
  ...     InsertOne(namespace="db.test_four", document={"_id": 5}),
  ...     DeleteOne(namespace="db.test_three", filter={"_id": 3}),
  ... ]
  >>> try:
  ...     client.bulk_write(models)
  ... except ClientBulkWriteException as cbwe:
  ...     exception = cbwe
  ...
  >>> exception.write_errors
  [{'ok': 0.0,
    'idx': 2,
    'code': 11000,
    'errmsg': 'E11000 duplicate key error ... dup key: { _id: 3 }', ...
    'op': {'insert': 'db.test_three', 'document': {'_id': 3}}}]
  >>> exception.partial_result.inserted_count
  2
  >>> exception.partial_result.deleted_count
  0

.. _unordered_client_bulk:

Unordered Operations
.....................

If the ``ordered`` parameter is set to False, all operations in the bulk write will be attempted, regardless of any individual write errors that occur.
For example, the fourth and fifth write operations below get executed successfully, despite the duplicate key error on the third operation.

.. doctest::
  :options: +NORMALIZE_WHITESPACE

  >>> from pymongo import InsertOne, DeleteOne
  >>> from pymongo.errors import ClientBulkWriteException
  >>> models = [
  ...     InsertOne(namespace="db.test_five", document={"_id": 5}),
  ...     InsertOne(namespace="db.test_six", document={"_id": 6}),
  ...     InsertOne(namespace="db.test_five", document={"_id": 5}),  # Duplicate _id
  ...     InsertOne(namespace="db.test_six", document={"_id": 7}),
  ...     DeleteOne(namespace="db.test_five", filter={"_id": 5}),
  ... ]
  >>> try:
  ...     client.bulk_write(models, ordered=False)
  ... except ClientBulkWriteException as cbwe:
  ...     exception = cbwe
  ...
  >>> exception.write_errors
  [{'ok': 0.0,
    'idx': 2,
    'code': 11000,
    'errmsg': 'E11000 duplicate key error ... dup key: { _id: 5 }', ...
    'op': {'insert': 'db.test_five', 'document': {'_id': 5}}}]
  >>> exception.partial_result.inserted_count
  3
  >>> exception.partial_result.deleted_count
  1
