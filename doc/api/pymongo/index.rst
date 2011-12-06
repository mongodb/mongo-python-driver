:mod:`pymongo` -- Python driver for MongoDB
===========================================

.. automodule:: pymongo
   :synopsis: Python driver for MongoDB

   .. autodata:: version
   .. data:: Connection

      Alias for :class:`pymongo.connection.Connection`.

   .. data:: ReplicaSetConnection

      Alias for :class:`pymongo.replica_set_connection.ReplicaSetConnection`.

   .. autoclass:: pymongo.ReadPreference
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

Deprecated sub-modules (moved to the :mod:`bson` package):

.. toctree::
   :maxdepth: 2

   bson
   binary
   code
   dbref
   json_util
   max_key
   min_key
   objectid
   son
   timestamp
   tz_util
   uri_parser
