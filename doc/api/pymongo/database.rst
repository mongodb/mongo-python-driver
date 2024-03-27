:mod:`database` -- Database level operations
============================================

.. automodule:: pymongo.database
   :synopsis: Database level operations

   .. autodata:: pymongo.auth.MECHANISMS

   .. autoclass:: pymongo.database.Database
      :members:

      .. describe:: db[collection_name] || db.collection_name

         Get the `collection_name` :class:`~pymongo.collection.Collection` of
         :class:`Database` `db`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid collection
         name is used.

         .. note::  Use dictionary style access if `collection_name` is an
            attribute of the :class:`Database` class eg: db[`collection_name`].

      .. automethod:: __getitem__
      .. automethod:: __getattr__
      .. autoattribute:: codec_options
      .. autoattribute:: read_preference
      .. autoattribute:: write_concern
      .. autoattribute:: read_concern
