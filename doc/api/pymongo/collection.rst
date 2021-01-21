:mod:`collection` -- Collection level operations
================================================

.. automodule:: pymongo.collection
   :synopsis: Collection level operations

   .. autodata:: pymongo.ASCENDING
   .. autodata:: pymongo.DESCENDING
   .. autodata:: pymongo.GEO2D
   .. autodata:: pymongo.GEOHAYSTACK
   .. autodata:: pymongo.GEOSPHERE
   .. autodata:: pymongo.HASHED
   .. autodata:: pymongo.TEXT

   .. autoclass:: pymongo.collection.ReturnDocument

      .. autoattribute:: BEFORE
         :annotation:
      .. autoattribute:: AFTER
         :annotation:

   .. autoclass:: pymongo.collection.Collection(database, name, create=False, **kwargs)

      .. describe:: c[name] || c.name

         Get the `name` sub-collection of :class:`Collection` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid
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
      .. automethod:: find(filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, modifiers=None, batch_size=0, manipulate=True, collation=None, hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False, show_record_id=False, snapshot=False, comment=None, session=None)
      .. automethod:: find_raw_batches(filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, modifiers=None, batch_size=0, manipulate=True, collation=None, hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False, show_record_id=False, snapshot=False, comment=None)
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
      .. automethod:: drop
      .. automethod:: rename
      .. automethod:: options
      .. automethod:: map_reduce
      .. automethod:: inline_map_reduce
      .. automethod:: initialize_unordered_bulk_op
      .. automethod:: initialize_ordered_bulk_op
      .. automethod:: group
      .. automethod:: count
