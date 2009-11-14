:mod:`connection` -- Tools for connecting to MongoDB
====================================================

.. automodule:: pymongo.connection
   :synopsis: Tools for connecting to MongoDB

   .. autoclass:: pymongo.connection.Connection([host='localhost'[, port=27017[, pool_size=1[, auto_start_request=True[, timeout=1.0[, slave_okay=False[, network_timeout=None]]]]]]])

      .. automethod:: paired(left[, right=('localhost', 27017)[, pool_size=1[, auto_start_request=True]]])
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
