:mod:`database` -- Database level operations
============================================


.. automodule:: pymongo.asynchronous.database
   :synopsis: Database level operations

   .. autoclass:: pymongo.asynchronous.database.AsyncDatabase
      :members:

      .. describe:: db[collection_name] || db.collection_name

         Get the `collection_name` :class:`~pymongo.asynchronous.collection.AsyncCollection` of
         :class:`AsyncDatabase` `db`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid collection
         name is used.

         .. note::  Use dictionary style access if `collection_name` is an
            attribute of the :class:`AsyncDatabase` class eg: db[`collection_name`].

      .. automethod:: __getitem__
      .. automethod:: __getattr__
      .. autoattribute:: codec_options
      .. autoattribute:: read_preference
      .. autoattribute:: write_concern
      .. autoattribute:: read_concern
