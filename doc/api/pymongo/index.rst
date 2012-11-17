:mod:`pymongo` -- Python driver for MongoDB
===========================================

.. automodule:: pymongo
   :synopsis: Python driver for MongoDB

   .. autodata:: version
   .. data:: MongoClient

      Alias for :class:`pymongo.mongo_client.MongoClient`.

   .. data:: MongoReplicaSetClient

      Alias for :class:`pymongo.mongo_replica_set_client.MongoReplicaSetClient`.

   .. autoclass:: pymongo.read_preferences.ReadPreference
   .. autofunction:: has_c

Sub-modules:

.. toctree::
   :maxdepth: 2

   connection
   database
   collection
   cursor
   errors
   master_slave_connection
   message
   mongo_client
   mongo_replica_set_client
   pool
   replica_set_connection
   son_manipulator
   cursor_manager
   uri_parser
