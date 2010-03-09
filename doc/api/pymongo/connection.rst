:mod:`connection` -- Tools for connecting to MongoDB
====================================================

.. automodule:: pymongo.connection
   :synopsis: Tools for connecting to MongoDB

   .. autoclass:: pymongo.connection.Connection([host='localhost'[, port=27017[, pool_size=None[, auto_start_request=None[, timeout=None[, slave_okay=False[, network_timeout=None]]]]]]])

      .. automethod:: from_uri([uri='mongodb://localhost'])
      .. automethod:: paired(left[, right=('localhost', 27017)])
      .. automethod:: disconnect

      .. describe:: c[db_name] || c.db_name

         Get the `db_name` :class:`~pymongo.database.Database` on :class:`Connection` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid database name is used.

      .. autoattribute:: host
      .. autoattribute:: port
      .. autoattribute:: slave_okay
      .. automethod:: database_names
      .. automethod:: drop_database
      .. automethod:: copy_database(from_name, to_name[, from_host=None[, username=None[, password=None]]])
      .. automethod:: server_info
      .. automethod:: start_request
      .. automethod:: end_request
      .. automethod:: close_cursor
      .. automethod:: kill_cursors
      .. automethod:: set_cursor_manager
