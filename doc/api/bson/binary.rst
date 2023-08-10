:mod:`binary` -- Tools for representing binary data to be stored in MongoDB
===========================================================================

.. automodule:: bson.binary
   :synopsis: Tools for representing binary data to be stored in MongoDB

   .. autodata:: BINARY_SUBTYPE
   .. autodata:: FUNCTION_SUBTYPE
   .. autodata:: OLD_BINARY_SUBTYPE
   .. autodata:: OLD_UUID_SUBTYPE
   .. autodata:: UUID_SUBTYPE
   .. autodata:: STANDARD
   .. autodata:: PYTHON_LEGACY
   .. autodata:: JAVA_LEGACY
   .. autodata:: CSHARP_LEGACY
   .. autodata:: MD5_SUBTYPE
   .. autodata:: COLUMN_SUBTYPE
   .. autodata:: SENSITIVE_SUBTYPE
   .. autodata:: USER_DEFINED_SUBTYPE

   .. autoclass:: UuidRepresentation
      :members:

   .. autoclass:: Binary(data, subtype=BINARY_SUBTYPE)
      :members:
      :show-inheritance:
