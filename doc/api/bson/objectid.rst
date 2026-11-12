:mod:`objectid` -- Tools for working with MongoDB ObjectIds
===========================================================

.. automodule:: bson.objectid
   :synopsis: Tools for working with MongoDB ObjectIds

   .. autoclass:: bson.objectid.ObjectId(oid=None)
      :members:

      .. describe:: str(o)

         Get a hex encoded version of :class:`ObjectId` `o`.

         The following property always holds:

         .. testsetup::

           from bson.objectid import ObjectId

         .. doctest::

           >>> o = ObjectId()
           >>> o == ObjectId(str(o))
           True

         This representation is useful for urls or other places where
         ``o.binary`` is inappropriate.
