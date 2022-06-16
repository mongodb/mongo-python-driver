:mod:`pymongo` -- Python driver for MongoDB
===========================================

.. automodule:: pymongo
   :synopsis: Python driver for MongoDB

   .. autodata:: version
   .. data:: MongoClient

      Alias for :class:`pymongo.mongo_client.MongoClient`.

   .. data:: ReadPreference

      Alias for :class:`pymongo.read_preferences.ReadPreference`.

   .. autofunction:: has_c
   .. data:: MIN_SUPPORTED_WIRE_VERSION

      The minimum wire protocol version PyMongo supports.

   .. data:: MAX_SUPPORTED_WIRE_VERSION

      The maximum wire protocol version PyMongo supports.

   .. autofunction:: timeout

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
   database
   driver_info
   encryption
   encryption_options
   errors
   mongo_client
   monitoring
   operations
   pool
   read_concern
   read_preferences
   results
   server_api
   server_description
   topology_description
   uri_parser
   write_concern
   event_loggers
