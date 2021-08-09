:mod:`cursor` -- Tools for iterating over MongoDB query results
===============================================================

.. automodule:: pymongo.cursor
   :synopsis: Tools for iterating over MongoDB query results

   .. autoclass:: pymongo.cursor.CursorType

      .. autoattribute:: NON_TAILABLE
         :annotation:
      .. autoattribute:: TAILABLE
         :annotation:
      .. autoattribute:: TAILABLE_AWAIT
         :annotation:
      .. autoattribute:: EXHAUST
         :annotation:

   .. autoclass:: pymongo.cursor.Cursor(collection, filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, batch_size=0, collation=None, hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False, show_record_id=False, snapshot=False, comment=None, session=None, allow_disk_use=None)
      :members:

      .. describe:: c[index]

         See :meth:`__getitem__` and read the warning.

      .. automethod:: __getitem__

   .. autoclass:: pymongo.cursor.RawBatchCursor(collection, filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, batch_size=0, collation=None, hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False, show_record_id=False, snapshot=False, comment=None, allow_disk_use=None)
