:mod:`connection` -- Tools for connecting to MongoDB
====================================================

.. automodule:: pymongo.connection
   :synopsis: Tools for connecting to MongoDB

   .. autoclass:: pymongo.connection.Connection([host=None[, port=None[, pool_size=None[, auto_start_request=None[, timeout=None[, slave_okay=False[, network_timeout=None]]]]]]])

      .. automethod:: paired(left[, right=None[, pool_size=None[, auto_start_request=None]]])
      .. automethod:: host
      .. automethod:: port
      .. autoattribute:: slave_okay
      .. automethod:: database_names
      .. automethod:: drop_database
      .. automethod:: server_info
      .. automethod:: start_request
      .. automethod:: end_request
      .. automethod:: close_cursor
      .. automethod:: kill_cursors
      .. automethod:: set_cursor_manager
