:mod:`mongo_replica_set_client` -- Tools for connecting to a MongoDB replica set
================================================================================

.. automodule:: pymongo.mongo_replica_set_client
   :synopsis: Tools for connecting to a MongoDB replica set

   .. autoclass:: pymongo.mongo_replica_set_client.MongoReplicaSetClient([hosts_or_uri[, max_pool_size=100[, document_class=dict[, tz_aware=False[, **kwargs]]]]])

      .. automethod:: disconnect
      .. automethod:: close
      .. automethod:: alive

      .. describe:: c[db_name] || c.db_name

         Get the `db_name` :class:`~pymongo.database.Database` on :class:`MongoReplicaSetClient` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid database name is used.

      .. autoattribute:: seeds
      .. autoattribute:: hosts
      .. autoattribute:: primary
      .. autoattribute:: secondaries
      .. autoattribute:: arbiters
      .. autoattribute:: is_mongos
      .. autoattribute:: max_pool_size
      .. autoattribute:: document_class
      .. autoattribute:: tz_aware
      .. autoattribute:: max_bson_size
      .. autoattribute:: max_message_size
      .. autoattribute:: min_wire_version
      .. autoattribute:: max_wire_version
      .. autoattribute:: auto_start_request
      .. autoattribute:: read_preference
      .. autoattribute:: tag_sets
      .. autoattribute:: secondary_acceptable_latency_ms
      .. autoattribute:: write_concern
      .. autoattribute:: uuid_subtype
      .. automethod:: database_names
      .. automethod:: drop_database
      .. automethod:: copy_database(from_name, to_name[, from_host=None[, username=None[, password=None]]])
      .. automethod:: get_default_database
      .. automethod:: close_cursor
