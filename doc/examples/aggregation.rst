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
  >>> list(db.things.aggregate(pipeline))
  [{u'count': 3, u'_id': u'cat'}, {u'count': 2, u'_id': u'dog'}, {u'count': 1, u'_id': u'mouse'}]

To run an explain plan for this aggregation use the
:meth:`~pymongo.database.Database.command` method::

  >>> db.command('aggregate', 'things', pipeline=pipeline, explain=True)
  {u'ok': 1.0, u'stages': [...]}

As well as simple aggregations the aggregation framework provides projection
capabilities to reshape the returned data. Using projections and aggregation,
you can add computed fields, create new virtual sub-objects, and extract
sub-fields into the top-level of results.

.. seealso:: The full documentation for MongoDB's `aggregation framework
    <http://docs.mongodb.org/manual/applications/aggregation>`_

Map/Reduce
----------

Another option for aggregation is to use the map reduce framework.  Here we
will define **map** and **reduce** functions to also count the number of
occurrences for each tag in the ``tags`` array, across the entire collection.

Our **map** function just emits a single `(key, 1)` pair for each tag in
the array:

.. doctest::

  >>> from bson.code import Code
  >>> mapper = Code("""
  ...               function () {
  ...                 this.tags.forEach(function(z) {
  ...                   emit(z, 1);
  ...                 });
  ...               }
  ...               """)

The **reduce** function sums over all of the emitted values for a given key:

.. doctest::

  >>> reducer = Code("""
  ...                function (key, values) {
  ...                  var total = 0;
  ...                  for (var i = 0; i < values.length; i++) {
  ...                    total += values[i];
  ...                  }
  ...                  return total;
  ...                }
  ...                """)

.. note:: We can't just return ``values.length`` as the **reduce** function
   might be called iteratively on the results of other reduce steps.

Finally, we call :meth:`~pymongo.collection.Collection.map_reduce` and
iterate over the result collection:

.. doctest::

  >>> result = db.things.map_reduce(mapper, reducer, "myresults")
  >>> for doc in result.find():
  ...   print doc
  ...
  {u'_id': u'cat', u'value': 3.0}
  {u'_id': u'dog', u'value': 2.0}
  {u'_id': u'mouse', u'value': 1.0}

Advanced Map/Reduce
-------------------

PyMongo's API supports all of the features of MongoDB's map/reduce engine.
One interesting feature is the ability to get more detailed results when
desired, by passing `full_response=True` to
:meth:`~pymongo.collection.Collection.map_reduce`. This returns the full
response to the map/reduce command, rather than just the result collection:

.. doctest::

  >>> db.things.map_reduce(mapper, reducer, "myresults", full_response=True)
  {u'counts': {u'input': 4, u'reduce': 2, u'emit': 6, u'output': 3}, u'timeMillis': ..., u'ok': ..., u'result': u'...'}

All of the optional map/reduce parameters are also supported, simply pass them
as keyword arguments. In this example we use the `query` parameter to limit the
documents that will be mapped over:

.. doctest::

  >>> result = db.things.map_reduce(mapper, reducer, "myresults", query={"x": {"$lt": 2}})
  >>> for doc in result.find():
  ...   print doc
  ...
  {u'_id': u'cat', u'value': 1.0}
  {u'_id': u'dog', u'value': 1.0}

With MongoDB 1.8.0 or newer you can use :class:`~bson.son.SON` or
:class:`collections.OrderedDict` to specify a different database to store the
result collection:

.. doctest::

  >>> from bson.son import SON
  >>> db.things.map_reduce(mapper, reducer, out=SON([("replace", "results"), ("db", "outdb")]), full_response=True)
  {u'counts': {u'input': 4, u'reduce': 2, u'emit': 6, u'output': 3}, u'timeMillis': ..., u'ok': ..., u'result': {u'db': ..., u'collection': ...}}

.. seealso:: The full list of options for MongoDB's `map reduce engine <http://www.mongodb.org/display/DOCS/MapReduce>`_

Group
-----

The :meth:`~pymongo.collection.Collection.group` method provides some of the
same functionality as SQL's GROUP BY.  Simpler than a map reduce you need to
provide a key to group by, an initial value for the aggregation and a
reduce function.

.. note:: Doesn't work with sharded MongoDB configurations, use aggregation or
          map/reduce instead of group().

Here we are doing a simple group and count of the occurrences of ``x`` values:

.. doctest::

  >>> from bson.code import Code
  >>> reducer = Code("""
  ...                function(obj, prev){
  ...                  prev.count++;
  ...                }
  ...                """)
  ...
  >>> results = db.things.group(key={"x":1}, condition={}, initial={"count": 0}, reduce=reducer)
  >>> for doc in results:
  ...   print doc
  {u'count': 1.0, u'x': 1.0}
  {u'count': 2.0, u'x': 2.0}
  {u'count': 1.0, u'x': 3.0}

.. seealso:: The full list of options for MongoDB's `group method <http://www.mongodb.org/display/DOCS/Aggregation#Aggregation-Group>`_
