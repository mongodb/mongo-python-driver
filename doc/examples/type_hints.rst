
.. _type_hints-example:

Type Hints
===========

PyMongo ships with::

  from pymongo import MongoClient
  from bson.binary import UuidRepresentation
  from uuid import uuid4

  # use the 'standard' representation for cross-language compatibility.
  client = MongoClient(uuidRepresentation='standard')
  collection = client.get_database('uuid_db').get_collection('uuid_coll')
