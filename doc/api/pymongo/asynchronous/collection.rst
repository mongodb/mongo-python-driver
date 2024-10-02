:mod:`collection` -- Collection level operations
================================================

.. warning:: This API is currently in beta, meaning the classes, methods,
   and behaviors described within may change before the full release.
   If you come across any bugs during your use of this API,
   please file a Jira ticket in the "Python Driver" project at https://jira.mongodb.org/browse/PYTHON.

.. automodule:: pymongo.asynchronous.collection
   :synopsis: Collection level operations

   .. autoclass:: pymongo.asynchronous.collection.ReturnDocument

   .. autoclass:: pymongo.asynchronous.collection.AsyncCollection(database, name, create=False, **kwargs)

      .. describe:: c[name] || c.name

         Get the `name` sub-collection of :class:`AsyncCollection` `c`.

         Raises :class:`~pymongo.asynchronous.errors.InvalidName` if an invalid
         collection name is used.

      .. autoattribute:: full_name
      .. autoattribute:: name
      .. autoattribute:: database
      .. autoattribute:: codec_options
      .. autoattribute:: read_preference
      .. autoattribute:: write_concern
      .. autoattribute:: read_concern
      .. automethod:: with_options
      .. automethod:: bulk_write
      .. automethod:: insert_one
      .. automethod:: insert_many
      .. automethod:: replace_one
      .. automethod:: update_one
      .. automethod:: update_many
      .. automethod:: delete_one
      .. automethod:: delete_many
      .. automethod:: aggregate
      .. automethod:: aggregate_raw_batches
      .. automethod:: watch
      .. automethod:: find(filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, batch_size=0, collation=None, hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False, show_record_id=False, snapshot=False, comment=None, session=None, allow_disk_use=None)
      .. automethod:: find_raw_batches(filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, batch_size=0, collation=None, hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False, show_record_id=False, snapshot=False, comment=None, session=None, allow_disk_use=None)
      .. automethod:: find_one(filter=None, *args, **kwargs)
      .. automethod:: find_one_and_delete
      .. automethod:: find_one_and_replace(filter, replacement, projection=None, sort=None, return_document=ReturnDocument.BEFORE, hint=None, session=None, **kwargs)
      .. automethod:: find_one_and_update(filter, update, projection=None, sort=None, return_document=ReturnDocument.BEFORE, array_filters=None, hint=None, session=None, **kwargs)
      .. automethod:: count_documents
      .. automethod:: estimated_document_count
      .. automethod:: distinct
      .. automethod:: create_index
      .. automethod:: create_indexes
      .. automethod:: drop_index
      .. automethod:: drop_indexes
      .. automethod:: list_indexes
      .. automethod:: index_information
      .. automethod:: create_search_index
      .. automethod:: create_search_indexes
      .. automethod:: drop_search_index
      .. automethod:: list_search_indexes
      .. automethod:: update_search_index
      .. automethod:: drop
      .. automethod:: rename
      .. automethod:: options
      .. automethod:: __getitem__
      .. automethod:: __getattr__
