:mod:`mongo_client` -- Tools for connecting to MongoDB
======================================================

.. automodule:: pymongo.mongo_client
   :synopsis: Tools for connecting to MongoDB

   .. autoclass:: pymongo.mongo_client.MongoClient([host='localhost'[, port=27017[, max_pool_size=100[, document_class=dict[, tz_aware=False[, **kwargs]]]]]])

      .. automethod:: disconnect
      .. automethod:: close
      .. automethod:: alive

      .. describe:: c[db_name] || c.db_name

         Get the `db_name` :class:`~pymongo.database.Database` on :class:`MongoClient` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid database name is used.

      .. autoattribute:: host
      .. autoattribute:: port
      .. autoattribute:: is_primary
      .. autoattribute:: is_mongos
      .. autoattribute:: max_pool_size
      .. autoattribute:: nodes
      .. autoattribute:: auto_start_request
      .. autoattribute:: document_class
      .. autoattribute:: tz_aware
      .. autoattribute:: max_bson_size
      .. autoattribute:: max_message_size
      .. autoattribute:: min_wire_version
      .. autoattribute:: max_wire_version
      .. autoattribute:: read_preference
      .. autoattribute:: tag_sets
      .. autoattribute:: secondary_acceptable_latency_ms
      .. autoattribute:: write_concern
      .. autoattribute:: uuid_subtype
      .. autoattribute:: is_locked
      .. automethod:: database_names
      .. automethod:: drop_database
      .. automethod:: copy_database(from_name, to_name[, from_host=None[, username=None[, password=None]]])
      .. automethod:: get_default_database
      .. automethod:: server_info
      .. automethod:: start_request
      .. automethod:: end_request
      .. automethod:: close_cursor
      .. automethod:: kill_cursors
      .. automethod:: set_cursor_manager
      .. automethod:: fsync
      .. automethod:: unlock
