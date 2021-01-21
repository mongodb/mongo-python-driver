Geospatial Indexing Example
===========================

.. testsetup::

  from pymongo import MongoClient
  client = MongoClient()
  client.drop_database('geo_example')

This example shows how to create and use a :data:`~pymongo.GEO2D`
index in PyMongo. To create a spherical (earth-like) geospatial index use :data:`~pymongo.GEOSPHERE` instead.

.. mongodoc:: geo

Creating a Geospatial Index
---------------------------

Creating a geospatial index in pymongo is easy:

.. doctest::

  >>> from pymongo import MongoClient, GEO2D
  >>> db = MongoClient().geo_example
  >>> db.places.create_index([("loc", GEO2D)])
  'loc_2d'

Inserting Places
----------------

Locations in MongoDB are represented using either embedded documents
or lists where the first two elements are coordinates. Here, we'll
insert a couple of example locations:

.. doctest::

  >>> result = db.places.insert_many([{"loc": [2, 5]},
  ...                                 {"loc": [30, 5]},
  ...                                 {"loc": [1, 2]},
  ...                                 {"loc": [4, 4]}])  # doctest: +ELLIPSIS
  >>> result.inserted_ids
  [ObjectId('...'), ObjectId('...'), ObjectId('...'), ObjectId('...')]

.. note:: If specifying latitude and longitude coordinates in :data:`~pymongo.GEOSPHERE`, list the **longitude** first and then **latitude**.

Querying
--------

Using the geospatial index we can find documents near another point:

.. doctest::

  >>> import pprint
  >>> for doc in db.places.find({"loc": {"$near": [3, 6]}}).limit(3):
  ...   pprint.pprint(doc)
  ...
  {'_id': ObjectId('...'), 'loc': [2, 5]}
  {'_id': ObjectId('...'), 'loc': [4, 4]}
  {'_id': ObjectId('...'), 'loc': [1, 2]}

.. note:: If using :data:`pymongo.GEOSPHERE`, using $nearSphere is recommended.

The $maxDistance operator requires the use of :class:`~bson.son.SON`:

.. doctest::

  >>> from bson.son import SON
  >>> query = {"loc": SON([("$near", [3, 6]), ("$maxDistance", 100)])}
  >>> for doc in db.places.find(query).limit(3):
  ...   pprint.pprint(doc)
  ...
  {'_id': ObjectId('...'), 'loc': [2, 5]}
  {'_id': ObjectId('...'), 'loc': [4, 4]}
  {'_id': ObjectId('...'), 'loc': [1, 2]}

It's also possible to query for all items within a given rectangle
(specified by lower-left and upper-right coordinates):

.. doctest::

  >>> query = {"loc": {"$within": {"$box": [[2, 2], [5, 6]]}}}
  >>> for doc in db.places.find(query).sort('_id'):
  ...     pprint.pprint(doc)
  {'_id': ObjectId('...'), 'loc': [2, 5]}
  {'_id': ObjectId('...'), 'loc': [4, 4]}

Or circle (specified by center point and radius):

.. doctest::

  >>> query = {"loc": {"$within": {"$center": [[0, 0], 6]}}}
  >>> for doc in db.places.find(query).sort('_id'):
  ...   pprint.pprint(doc)
  ...
  {'_id': ObjectId('...'), 'loc': [2, 5]}
  {'_id': ObjectId('...'), 'loc': [1, 2]}
  {'_id': ObjectId('...'), 'loc': [4, 4]}

geoNear queries are also supported using :class:`~bson.son.SON`::

  >>> from bson.son import SON
  >>> db.command(SON([('geoNear', 'places'), ('near', [1, 2])]))
  {'ok': 1.0, 'stats': ...}

.. warning:: Starting in MongoDB version 4.0, MongoDB deprecates the **geoNear** command. Use one of the following operations instead.

  * $geoNear - aggregation stage.
  * $near - query operator.
  * $nearSphere - query operator.
