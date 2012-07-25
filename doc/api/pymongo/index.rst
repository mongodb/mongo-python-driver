:mod:`pymongo` -- Python driver for MongoDB
===========================================

.. automodule:: pymongo
   :synopsis: Python driver for MongoDB

   .. autodata:: version
   .. data:: Connection

      Alias for :class:`pymongo.connection.Connection`.

   .. data:: ReplicaSetConnection

      Alias for :class:`pymongo.replica_set_connection.ReplicaSetConnection`.

   .. autoclass:: pymongo.read_preferences.ReadPreference
   .. autofunction:: has_c

Sub-modules:

.. toctree::
   :maxdepth: 2

   connection
   database
   collection
   cursor
   errors
   master_slave_connection
   message
   pool
   replica_set_connection
   son_manipulator
   cursor_manager
   uri_parser
