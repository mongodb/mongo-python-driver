:mod:`cursor` -- Tools for iterating over MongoDB query results
===============================================================

.. automodule:: pymongo.cursor
   :synopsis: Tools for iterating over MongoDB query results

   .. autoclass:: pymongo.cursor.Cursor(collection, spec, fields, skip, limit, slave_okay, timeout, tailable[, snapshot=False])
      :members:

      .. describe:: c[index]

         See :meth:`__getitem__`.

      .. automethod:: __getitem__
