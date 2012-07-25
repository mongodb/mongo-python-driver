:mod:`replica_set_connection` -- Tools for connecting to a MongoDB replica set
==============================================================================

.. automodule:: pymongo.replica_set_connection
   :synopsis: Tools for connecting to a MongoDB replica set

   .. autoclass:: pymongo.replica_set_connection.ReplicaSetConnection([hosts_or_uri[, max_pool_size=10[, document_class=dict[, tz_aware=False[, **kwargs]]]]])

      .. automethod:: disconnect
      .. automethod:: close

      .. describe:: c[db_name] || c.db_name

         Get the `db_name` :class:`~pymongo.database.Database` on :class:`ReplicaSetConnection` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid database name is used.

      .. autoattribute:: seeds
      .. autoattribute:: hosts
      .. autoattribute:: arbiters
      .. autoattribute:: primary
      .. autoattribute:: secondaries
      .. autoattribute:: read_preference
      .. autoattribute:: tag_sets
      .. autoattribute:: secondary_acceptable_latency_ms
      .. autoattribute:: max_pool_size
      .. autoattribute:: document_class
      .. autoattribute:: tz_aware
      .. autoattribute:: safe
      .. automethod:: get_lasterror_options
      .. automethod:: set_lasterror_options
      .. automethod:: unset_lasterror_options
      .. automethod:: database_names
      .. automethod:: drop_database
      .. automethod:: copy_database(from_name, to_name[, from_host=None[, username=None[, password=None]]])
      .. automethod:: close_cursor
