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
      .. automethod:: find(filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, modifiers=None, manipulate=True)
      .. automethod:: find_one(filter_or_id=None, *args, **kwargs)
      .. automethod:: find_one_and_delete
      .. automethod:: find_one_and_replace(filter, replacement, projection=None, sort=None, return_document=ReturnDocument.BEFORE, **kwargs)
      .. automethod:: find_one_and_update(filter, update, projection=None, sort=None, return_document=ReturnDocument.BEFORE, **kwargs)
      .. automethod:: count
      .. automethod:: distinct
      .. automethod:: create_index
      .. automethod:: create_indexes
      .. automethod:: drop_index
      .. automethod:: drop_indexes
      .. automethod:: reindex
      .. automethod:: list_indexes
      .. automethod:: index_information
      .. automethod:: drop
      .. automethod:: rename
      .. automethod:: options
      .. automethod:: group
      .. automethod:: map_reduce
      .. automethod:: inline_map_reduce
      .. automethod:: parallel_scan
      .. automethod:: initialize_unordered_bulk_op
      .. automethod:: initialize_ordered_bulk_op
      .. automethod:: insert(doc_or_docs, manipulate=True, check_keys=True, continue_on_error=False, **kwargs)
      .. automethod:: save(to_save, manipulate=True, check_keys=True, **kwargs)
      .. automethod:: update(spec, document, upsert=False, manipulate=False, multi=False, check_keys=True, **kwargs)
      .. automethod:: remove(spec_or_id=None, multi=True, **kwargs)
      .. automethod:: find_and_modify
      .. automethod:: ensure_index

