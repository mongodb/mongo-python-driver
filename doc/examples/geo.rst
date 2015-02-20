Geospatial Indexing Example
===========================

.. testsetup::

  from pymongo import MongoClient
  client = MongoClient()
  client.drop_database('geo_example')

This example shows how to create and use a :data:`~pymongo.GEO2D`
index in PyMongo.

.. mongodoc:: geo

Creating a Geospatial Index
---------------------------

Creating a geospatial index in pymongo is easy:

.. doctest::

  >>> from pymongo import MongoClient, GEO2D
  >>> db = MongoClient().geo_example
  >>> db.places.create_index([("loc", GEO2D)])
  u'loc_2d'

Inserting Places
----------------

Locations in MongoDB are represented using either embedded documents
or lists where the first two elements are coordinates. Here, we'll
insert a couple of example locations:

.. doctest::

  >>> result = db.places.insert_many([{"loc": [2, 5]},
  ...                                 {"loc": [30, 5]},
  ...                                 {"loc": [1, 2]},
  ...                                 {"loc": [4, 4]}])
  >>> result.inserted_ids
  [ObjectId('...'), ObjectId('...'), ObjectId('...'), ObjectId('...')]

Querying
--------

Using the geospatial index we can find documents near another point:

.. doctest::

  >>> for doc in db.places.find({"loc": {"$near": [3, 6]}}).limit(3):
  ...   repr(doc)
  ...
  "{u'loc': [2, 5], u'_id': ObjectId('...')}"
  "{u'loc': [4, 4], u'_id': ObjectId('...')}"
  "{u'loc': [1, 2], u'_id': ObjectId('...')}"

The $maxDistance operator requires the use of :class:`~bson.son.SON`:

.. doctest::

  >>> from bson.son import SON
  >>> for doc in db.places.find({"loc": SON([("$near", [3, 6]), ("$maxDistance", 100)])}).limit(3):
  ...   repr(doc)
  ...
  "{u'loc': [2, 5], u'_id': ObjectId('...')}"
  "{u'loc': [4, 4], u'_id': ObjectId('...')}"
  "{u'loc': [1, 2], u'_id': ObjectId('...')}"

It's also possible to query for all items within a given rectangle
(specified by lower-left and upper-right coordinates):

.. doctest::

  >>> for doc in db.places.find({"loc": {"$within": {"$box": [[2, 2], [5, 6]]}}}):
  ...   repr(doc)
  ...
  "{u'loc': [2, 5], u'_id': ObjectId('...')}"
  "{u'loc': [4, 4], u'_id': ObjectId('...')}"

Or circle (specified by center point and radius):

.. doctest::

  >>> for doc in db.places.find({"loc": {"$within": {"$center": [[0, 0], 6]}}}):
  ...   repr(doc)
  ...
  "{u'loc': [1, 2], u'_id': ObjectId('...')}"
  "{u'loc': [2, 5], u'_id': ObjectId('...')}"
  "{u'loc': [4, 4], u'_id': ObjectId('...')}"

geoNear queries are also supported using :class:`~bson.son.SON`::

  >>> from bson.son import SON
  >>> db.command(SON([('geoNear', 'places'), ('near', [1, 2])]))
  {u'ok': 1.0, u'stats': ...}

