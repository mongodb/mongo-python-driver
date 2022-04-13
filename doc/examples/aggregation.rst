Aggregation Examples
====================

There are several methods of performing aggregations in MongoDB.  These
examples cover the new aggregation framework, using map reduce and using the
group method.

.. testsetup::

  from pymongo import MongoClient
  client = MongoClient()
  client.drop_database('aggregation_example')

Setup
-----
To start, we'll insert some example data which we can perform
aggregations on:

.. doctest::

  >>> from pymongo import MongoClient
  >>> db = MongoClient().aggregation_example
  >>> result = db.things.insert_many([{"x": 1, "tags": ["dog", "cat"]},
  ...                                 {"x": 2, "tags": ["cat"]},
  ...                                 {"x": 2, "tags": ["mouse", "cat", "dog"]},
  ...                                 {"x": 3, "tags": []}])
  >>> result.inserted_ids
  [ObjectId('...'), ObjectId('...'), ObjectId('...'), ObjectId('...')]

.. _aggregate-examples:

Aggregation Framework
---------------------

This example shows how to use the
:meth:`~pymongo.collection.Collection.aggregate` method to use the aggregation
framework.  We'll perform a simple aggregation to count the number of
occurrences for each tag in the ``tags`` array, across the entire collection.
To achieve this we need to pass in three operations to the pipeline.
First, we need to unwind the ``tags`` array, then group by the tags and
sum them up, finally we sort by count.

As python dictionaries don't maintain order you should use :class:`~bson.son.SON`
or :class:`collections.OrderedDict` where explicit ordering is required
eg "$sort":

.. note::

    aggregate requires server version **>= 2.1.0**.

.. doctest::

  >>> from bson.son import SON
  >>> pipeline = [
  ...     {"$unwind": "$tags"},
  ...     {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
  ...     {"$sort": SON([("count", -1), ("_id", -1)])}
  ... ]
  >>> import pprint
  >>> pprint.pprint(list(db.things.aggregate(pipeline)))
  [{'_id': 'cat', 'count': 3},
   {'_id': 'dog', 'count': 2},
   {'_id': 'mouse', 'count': 1}]

To run an explain plan for this aggregation use the
:meth:`~pymongo.database.Database.command` method::

  >>> db.command('aggregate', 'things', pipeline=pipeline, explain=True)
  {'ok': 1.0, 'stages': [...]}

As well as simple aggregations the aggregation framework provides projection
capabilities to reshape the returned data. Using projections and aggregation,
you can add computed fields, create new virtual sub-objects, and extract
sub-fields into the top-level of results.

.. seealso:: The full documentation for MongoDB's `aggregation framework
    <http://mongodb.com/docs/manual/applications/aggregation>`_
