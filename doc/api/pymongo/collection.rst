:mod:`collection` -- Collection level operations
================================================

.. automodule:: pymongo.collection
   :synopsis: Collection level operations

   .. autodata:: pymongo.ASCENDING
   .. autodata:: pymongo.DESCENDING
   .. autodata:: pymongo.GEO2D
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
      .. describe:: reindex

         Removed. Run the `reIndex command`_ directly instead. Code like this::

           >>> result = database.my_collection.reindex()

         can be changed to this::

           >>> result = database.command('reIndex', 'my_collection')

         .. _reIndex command: https://docs.mongodb.com/manual/reference/command/reIndex/
      .. automethod:: drop_indexes
      .. automethod:: list_indexes
      .. automethod:: index_information
      .. automethod:: drop
      .. automethod:: rename
      .. automethod:: options
      .. describe:: map_reduce || inline_map_reduce

         Removed. Migrate to :meth:`~pymongo.collection.Collection.aggregate` or run the
         `mapReduce command`_ command directly with :meth:`~pymongo.database.Database.command`
         instead. For more guidance on this migration see:

         - https://docs.mongodb.com/manual/reference/map-reduce-to-aggregation-pipeline/
         - https://docs.mongodb.com/manual/reference/aggregation-commands-comparison/

         .. _mapReduce command: https://docs.mongodb.com/manual/reference/command/mapReduce/
      .. describe:: parallel_scan

         Removed. MongoDB 4.2 removed the `parallelCollectionScan command`_.
         There is no replacement.

         .. _parallelCollectionScan command: https://docs.mongodb.com/manual/reference/command/parallelCollectionScan/
      .. describe:: initialize_ordered_bulk_op || initialize_unordered_bulk_op

         Removed. Use :meth:`pymongo.collection.Collection.bulk_write`
         instead. Code like this::
           batch = coll.initialize_(un)ordered_bulk_op()
           batch.insert({'a': 1})
           batch.find({'a': 1}).update_one({'$set': {'b': 1}})
           batch.find({'a': 2}).upsert().replace_one({'b': 2})
           batch.find({'a': 3}).remove()
           result = batch.execute()

         Can be changed to this::

           coll.bulk_write([
               InsertOne({'a': 1}),
               UpdateOne({'a': 1}, {'$set': {'b': 1}}),
               ReplaceOne({'a': 2}, {'b': 2}, upsert=True),
               DeleteOne({'a': 3}),
           ])
      .. describe:: group

         Removed. This method was deprecated in PyMongo 3.5. MongoDB 4.2
         removed the `group command`_. Use :meth:`~pymongo.collection.Collection
         .aggregate` with the ``$group`` stage instead.

         .. _group command: https://docs.mongodb.com/manual/reference/command/group/
      .. describe:: count

         Removed. Use
         :meth:`~pymongo.collection.Collection.count_documents` or
         :meth:`~pymongo.collection.Collection.estimated_document_count` instead.
         Code like this::

           ntotal = collection.count({})
           nmatched = collection.count({'price': {'$gte': 10}})

         Can be changed to this::

           ntotal = collection.estimated_document_count()
           nmatched = collection.count_documents({'price': {'$gte': 10}})
      .. describe:: insert

         Removed. Use :meth:`~pymongo.collection.Collection.insert_one` or
         :meth:`~pymongo.collection.Collection.insert_many` instead.
      .. describe:: save

         Removed. Applications will get better performance using
         :meth:`~pymongo.collection.Collection.insert_one` to insert a new document
         and :meth:`~pymongo.collection.Collection.update_one` to update an existing
         document.
      .. describe:: update

         Removed. Use :meth:`~pymongo.collection.Collection.update_one` to
         update a single document or
         :meth:`~pymongo.collection.Collection.update_many` to update multiple
         documents.
      .. describe:: remove

         Removed. Use :meth:`~pymongo.collection.Collection.delete_one` to
         delete a single document or :meth:`~pymongo.collection.Collection.delete_many`
         to delete multiple documents.
      .. describe:: find_and_modify

         Removed. Use
         :meth:`~pymongo.collection.Collection.find_one_and_update`,
         :meth:`~pymongo.collection.Collection.find_one_and_replace`, or
         :meth:`~pymongo.collection.Collection.find_one_and_delete` instead.
         Code like this::

           updated_doc = collection.find_and_modify({'a': 1}, {'$set': {'b': 1}})
           replaced_doc = collection.find_and_modify({'b': 1}, {'c': 1})
           deleted_doc = collection.find_and_modify({'c': 1}, remove=True)

         Can be changed to this::

           updated_doc = collection.find_one_and_update({'a': 1}, {'$set': {'b': 1}})
           replaced_doc = collection.find_one_and_replace({'b': 1}, {'c': 1})
           deleted_doc = collection.find_one_and_delete({'c': 1})
      .. describe:: ensure_index

         Use :meth:`~pymongo.collection.Collection.create_index` or
         :meth:`~pymongo.collection.Collection.create_indexes` instead. Note that
         ``ensure_index`` maintained an in memory cache of recently created indexes
         whereas the newer methods do not. Applications should avoid frequent calls
         to :meth:`~pymongo.collection.Collection.create_index` or
         :meth:`~pymongo .collection.Collection.create_indexes`. Code like this::

           def persist(self, document):
               collection.ensure_index('a', unique=True)
               collection.insert_one(document)

         Can be changed to this::

           def persist(self, document):
               if not self.created_index:
                   collection.create_index('a', unique=True)
                   self.created_index = True
               collection.insert_one(document)
