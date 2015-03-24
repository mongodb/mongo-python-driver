:mod:`mongo_replica_set_client` -- Tools for connecting to a MongoDB replica set
================================================================================

.. automodule:: pymongo.mongo_replica_set_client
   :synopsis: Tools for connecting to a MongoDB replica set

   .. autoclass:: pymongo.mongo_replica_set_client.MongoReplicaSetClient(hosts_or_uri, document_class=dict, tz_aware=False, connect=True, **kwargs)

      .. automethod:: close

      .. describe:: c[db_name] || c.db_name

         Get the `db_name` :class:`~pymongo.database.Database` on :class:`MongoReplicaSetClient` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid database name is used.

      .. autoattribute:: primary
      .. autoattribute:: secondaries
      .. autoattribute:: arbiters
      .. autoattribute:: max_pool_size
      .. autoattribute:: max_bson_size
      .. autoattribute:: max_message_size
      .. autoattribute:: local_threshold_ms
      .. autoattribute:: codec_options
      .. autoattribute:: read_preference
      .. autoattribute:: write_concern
      .. automethod:: database_names
      .. automethod:: drop_database
      .. automethod:: get_default_database
      .. automethod:: get_database
      .. automethod:: close_cursor
      .. automethod:: kill_cursors
      .. automethod:: set_cursor_manager
