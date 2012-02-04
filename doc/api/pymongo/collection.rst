:mod:`collection` -- Collection level operations
================================================

.. automodule:: pymongo.collection
   :synopsis: Collection level operations

   .. autodata:: pymongo.ASCENDING
   .. autodata:: pymongo.DESCENDING
   .. autodata:: pymongo.GEO2D

   .. autoclass:: pymongo.collection.Collection(database, name,[, create=False[, **kwargs]]])

      .. describe:: c[name] || c.name

         Get the `name` sub-collection of :class:`Collection` `c`.

         Raises :class:`~pymongo.errors.InvalidName` if an invalid
         collection name is used.

      .. autoattribute:: full_name
      .. autoattribute:: name
      .. autoattribute:: database
      .. autoattribute:: slave_okay
      .. autoattribute:: read_preference
      .. autoattribute:: safe
      .. autoattribute:: uuid_subtype
      .. automethod:: get_lasterror_options
      .. automethod:: set_lasterror_options
      .. automethod:: unset_lasterror_options
      .. automethod:: insert(doc_or_docs[, manipulate=True[, safe=False[, check_keys=True[, continue_on_error=False[, **kwargs]]]])
      .. automethod:: save(to_save[, manipulate=True[, safe=False[, **kwargs]]])
      .. automethod:: update(spec, document[, upsert=False[, manipulate=False[, safe=False[, multi=False[, **kwargs]]]]])
      .. automethod:: remove([spec_or_object_id=None[, safe=False[, **kwargs]]])
      .. automethod:: drop
      .. automethod:: find([spec=None[, fields=None[, skip=0[, limit=0[, timeout=True[, snapshot=False[, tailable=False[, sort=None[, max_scan=None[, as_class=None[, slave_okay=False[, await_data=False[, partial=False[, manipulate=True[, read_preference=ReadPreference.PRIMARY[, **kwargs]]]]]]]]]]]]]]]])
      .. automethod:: find_one([spec_or_id=None[, *args[, **kwargs]]])
      .. automethod:: count
      .. automethod:: create_index
      .. automethod:: ensure_index
      .. automethod:: drop_index
      .. automethod:: drop_indexes
      .. automethod:: reindex
      .. automethod:: index_information
      .. automethod:: options
      .. automethod:: group
      .. automethod:: rename
      .. automethod:: distinct
      .. automethod:: map_reduce
      .. automethod:: inline_map_reduce
      .. automethod:: find_and_modify

