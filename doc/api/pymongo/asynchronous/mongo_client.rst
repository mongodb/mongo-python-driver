:mod:`mongo_client` -- Tools for connecting to MongoDB
======================================================

.. warning:: This API is currently in beta, meaning the classes, methods,
   and behaviors described within may change before the full release.
   If you come across any bugs during your use of this API,
   please file a Jira ticket in the "Python Driver" project at https://jira.mongodb.org/browse/PYTHON.

.. automodule:: pymongo.asynchronous.mongo_client
   :synopsis: Tools for connecting to MongoDB

   .. autoclass:: pymongo.asynchronous.mongo_client.AsyncMongoClient(host='localhost', port=27017, document_class=dict, tz_aware=False, connect=True, **kwargs)

      .. automethod:: close

      .. describe:: c[db_name] || c.db_name

         Get the `db_name` :class:`~pymongo.asynchronous.database.AsyncDatabase` on :class:`AsyncMongoClient` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid database name is used.

      .. autoattribute:: topology_description
      .. autoattribute:: address
      .. autoattribute:: primary
      .. autoattribute:: secondaries
      .. autoattribute:: arbiters
      .. autoattribute:: is_primary
      .. autoattribute:: is_mongos
      .. autoattribute:: nodes
      .. autoattribute:: codec_options
      .. autoattribute:: read_preference
      .. autoattribute:: write_concern
      .. autoattribute:: read_concern
      .. autoattribute:: options
      .. automethod:: start_session
      .. automethod:: list_databases
      .. automethod:: list_database_names
      .. automethod:: drop_database
      .. automethod:: get_default_database
      .. automethod:: get_database
      .. automethod:: server_info
      .. automethod:: watch
      .. automethod:: bulk_write
      .. automethod:: __getitem__
      .. automethod:: __getattr__
