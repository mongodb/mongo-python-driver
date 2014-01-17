:mod:`replica_set_connection` -- Tools for connecting to a MongoDB replica set
==============================================================================

.. automodule:: pymongo.replica_set_connection
   :synopsis: Tools for connecting to a MongoDB replica set

   .. autoclass:: pymongo.replica_set_connection.ReplicaSetConnection([hosts_or_uri[, max_pool_size=None[, document_class=dict[, tz_aware=False[, **kwargs]]]]])

      .. automethod:: disconnect
      .. automethod:: close
      .. automethod:: alive

      .. describe:: c[db_name] || c.db_name

         Get the `db_name` :class:`~pymongo.database.Database` on :class:`ReplicaSetConnection` `c`.

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
      .. autoattribute:: safe
      .. automethod:: database_names
      .. automethod:: drop_database
      .. automethod:: copy_database(from_name, to_name[, from_host=None[, username=None[, password=None]]])
      .. automethod:: get_default_database
      .. automethod:: close_cursor
      .. automethod:: get_lasterror_options
      .. automethod:: set_lasterror_options
      .. automethod:: unset_lasterror_options
