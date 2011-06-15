:mod:`database` -- Database level operations
============================================

.. automodule:: pymongo.database
   :synopsis: Database level operations

   .. autodata:: pymongo.OFF
   .. autodata:: pymongo.SLOW_ONLY
   .. autodata:: pymongo.ALL

   .. autoclass:: pymongo.database.Database
      :members:

      .. describe:: db[collection_name] || db.collection_name

         Get the `collection_name` :class:`~pymongo.collection.Collection` of
         :class:`Database` `db`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid collection name is used.

      .. autoattribute:: slave_okay
      .. autoattribute:: safe
      .. automethod:: get_lasterror_options
      .. automethod:: set_lasterror_options
      .. automethod:: unset_lasterror_options


   .. autoclass:: pymongo.database.SystemJS
      :members:
