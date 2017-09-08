:mod:`pymongo` -- Python driver for MongoDB
===========================================

.. automodule:: pymongo
   :synopsis: Python driver for MongoDB

   .. autodata:: version
   .. data:: MongoClient

      Alias for :class:`pymongo.mongo_client.MongoClient`.

   .. data:: MongoReplicaSetClient

      Alias for :class:`pymongo.mongo_replica_set_client.MongoReplicaSetClient`.

   .. data:: ReadPreference

      Alias for :class:`pymongo.read_preferences.ReadPreference`.

   .. autofunction:: has_c
   .. data:: MIN_SUPPORTED_WIRE_VERSION

      The minimum wire protocol version PyMongo supports.

   .. data:: MAX_SUPPORTED_WIRE_VERSION

      The maximum wire protocol version PyMongo supports.

Sub-modules:

.. toctree::
   :maxdepth: 2

   database
   change_stream
   client_session
   collation
   collection
   command_cursor
   cursor
   bulk
   errors
   message
   monitoring
   mongo_client
   mongo_replica_set_client
   operations
   pool
   read_concern
   read_preferences
   results
   son_manipulator
   uri_parser
   write_concern
