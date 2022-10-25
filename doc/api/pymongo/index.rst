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

   bulk
   change_stream
   client_options
   client_session
   collation
   collection
   command_cursor
   cursor
   cursor_manager
   database
   driver_info
   encryption
   encryption_options
   errors
   message
   mongo_client
   mongo_replica_set_client
   monitoring
   operations
   pool
   read_concern
   read_preferences
   results
   son_manipulator
   server_api
   server_description
   topology_description
   uri_parser
   write_concern
   event_loggers
