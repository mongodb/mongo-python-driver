Map/Reduce Example
==================

.. testsetup::

  from pymongo import Connection
  connection = Connection()
  connection.drop_database('map_reduce_example')

This example shows how to use the
:meth:`~pymongo.collection.Collection.map_reduce` method to perform
map/reduce style aggregations on your data.

.. note:: Map/Reduce requires server version **>= 1.1.1**. The PyMongo
   :meth:`~pymongo.collection.Collection.map_reduce` helper requires
   PyMongo version **>= 1.2**.

Setup
-----
To start, we'll insert some example data which we can perform
map/reduce queries on:

.. doctest::

  >>> from pymongo import Connection
  >>> db = Connection().map_reduce_example
  >>> db.things.insert({"x": 1, "tags": ["dog", "cat"]})
  ObjectId('...')
  >>> db.things.insert({"x": 2, "tags": ["cat"]})
  ObjectId('...')
  >>> db.things.insert({"x": 3, "tags": ["mouse", "cat", "dog"]})
  ObjectId('...')
  >>> db.things.insert({"x": 4, "tags": []})
  ObjectId('...')

Basic Map/Reduce
----------------
Now we'll define our **map** and **reduce** functions. In this case
we're performing the same operation as in the `MongoDB Map/Reduce
documentation <http://www.mongodb.org/display/DOCS/MapReduce>`_ -
counting the number of occurrences for each tag in the ``tags`` array,
across the entire collection.

Our **map** function just emits a single `(key, 1)` pair for each tag in
the array:

.. doctest::

  >>> from pymongo.code import Code
  >>> map = Code("function () {"
  ...            "  this.tags.forEach(function(z) {"
  ...            "    emit(z, 1);"
  ...            "  });"
  ...            "}")

The **reduce** function sums over all of the emitted values for a given key:

.. doctest::

  >>> reduce = Code("function (key, values) {"
  ...               "  var total = 0;"
  ...               "  for (var i = 0; i < values.length; i++) {"
  ...               "    total += values[i];"
  ...               "  }"
  ...               "  return total;"
  ...               "}")

.. note:: We can't just return ``values.length`` as the **reduce** function
   might be called iteratively on the results of other reduce steps.

Finally, we call :meth:`~pymongo.collection.Collection.map_reduce` and
iterate over the result collection:

.. doctest::

  >>> result = db.things.map_reduce(map, reduce)
  >>> for doc in result.find():
  ...   print doc
  ...
  {u'_id': u'cat', u'value': 3.0}
  {u'_id': u'dog', u'value': 2.0}
  {u'_id': u'mouse', u'value': 1.0}

Advanced Map/Reduce
-------------------

PyMongo's API supports all of the features of MongoDB's map/reduce engine. One interesting feature is the ability to get more detailed results when desired, by passing `full_response=True` to :meth:`~pymongo.collection.Collection.map_reduce`. This returns the full response to the map/reduce command, rather than just the result collection:

.. doctest::

  >>> db.things.map_reduce(map, reduce, full_response=True)
  {u'counts': {u'input': 4, u'emit': 6, u'output': 3}, u'timeMillis': ..., u'ok': 1.0, u'result': u'...'}

All of the optional map/reduce parameters are also supported, simply pass them as keyword arguments. In this example we use the `query` parameter to limit the documents that will be mapped over:

.. doctest::

  >>> result = db.things.map_reduce(map, reduce, query={"x": {"$lt": 3}})
  >>> for doc in result.find():
  ...   print doc
  ...
  {u'_id': u'cat', u'value': 2.0}
  {u'_id': u'dog', u'value': 1.0}

.. seealso:: The full list of options for MongoDB's `map reduce engine <http://www.mongodb.org/display/DOCS/MapReduce>`_
