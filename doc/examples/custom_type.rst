Custom Type Example
===================

This is an example of using a custom type with PyMongo. The example here shows
how to subclass :class:`~bson.codec_options.TypeCodecBase` to write a type
codec, which is used to populate a :class:`~bson.codec_options.TypeRegistry`.
The type registry can then be used to create a custom-type-aware
:class:`~pymongo.collection.Collection`. Read and write operations
issued against the resulting collection object transparently manipulate
documents as they are saved or retrieved from MongoDB.


Setup
-----

We'll start by getting a clean database to use for the example:

.. doctest::

  >>> from pymongo import MongoClient
  >>> client = MongoClient()
  >>> client.drop_database('custom_type_example')
  >>> db = client.custom_type_example


Since the purpose of the example is to demonstrate working with custom types,
we'll need a custom data type to use. For this example, we will be working with
the :py:class:`~decimal.Decimal` type from Python's standard library. Since the
BSON library has a :class:`~bson.decimal128.Decimal128` type (that implements
the IEEE 754 decimal128 decimal-based floating-point numbering format) which
is distinct from Python's built-in :py:class:`~decimal.Decimal` type, when we
try to save an instance of ``Decimal`` with PyMongo, we get an
:exc:`~bson.errors.InvalidDocument` exception.

.. doctest::

  >>> from decimal import Decimal
  >>> mynumber = Decimal("45.321")
  >>> db.test.insert_one({'mynumber': mynumber})
  Traceback (most recent call last):
  ...
  bson.errors.InvalidDocument: Cannot encode object: <__main__.Decimal object at ...>


.. _custom-type-type-codec:

The Type Codec
--------------

In order to encode custom types, we must first define a **type codec** for our
type. A type codec describes how an instance of a custom type can be
*transformed* to/from one of the types :mod:`~bson` already understands, and
can encode/decode. Type codecs must inherit from
:class:`~bson.codec_options.TypeCodecBase`. In order to encode a custom type,
a codec must implement the ``python_type`` property and the
``transform_python`` method. Similarly, in order to decode a custom type,
a codec must implement the ``bson_type`` property and the ``transform_bson``
method. Note that a type codec need not support both encoding and decoding.


The type codec for our custom type simply needs to define how a
:py:class:`~decimal.Decimal` instance can be converted into a
:class:`~bson.decimal128.Decimal128` instance and vice-versa:

.. doctest::

  >>> from bson.decimal128 import Decimal128
  >>> from bson.codec_options import TypeCodecBase
  >>> class DecimalCodec(TypeCodecBase):
  ...     @property
  ...     def python_type(self):
  ...         """The Python type acted upon by this type codec."""
  ...         return Decimal
  ...
  ...     def transform_python(self, value):
  ...         """Function that transforms a custom type value into a type
  ...         that BSON can encode."""
  ...         return Decimal128(value)
  ...
  ...     @property
  ...     def bson_type(self):
  ...         """The BSON type acted upon by this type codec."""
  ...         return Decimal128
  ...
  ...     def transform_bson(self, value):
  ...         """Function that transforms a vanilla BSON type value into our
  ...         custom type."""
  ...         return value.to_decimal()
  >>> decimal_codec = DecimalCodec()


.. _custom-type-type-registry:

The Type Registry
-----------------

Before we can begin encoding and decoding our custom type objects, we must
first inform PyMongo about our type codec. This is done by creating a
:class:`~bson.codec_options.TypeRegistry` instance:

.. doctest::

  >>> from bson.codec_options import TypeRegistry
  >>> type_registry = TypeRegistry(decimal_codec)


Note that type registries can be instantiated with any number of type codecs.
Once instantiated, registries are immutable and the only way to add codecs
to a registry is to create a new one.


Putting it together
-------------------

Finally, we can define a :class:`~bson.codec_options.CodecOptions` instance
with our ``type_registry`` and use it to get a
:class:`~pymongo.collection.Collection` object that understands the
:py:class:`~decimal.Decimal` data type:

.. doctest::

  >>> from bson.codec_options import CodecOptions
  >>> codec_options = CodecOptions(type_registry=type_registry)
  >>> collection = db.get_collection('test', codec_options=codec_options)


Now, we can seamlessly encode and decode instances of
:py:class:`~decimal.Decimal`:

.. doctest::

  >>> collection.insert_one({'mynumber': Decimal("45.321")})
  <pymongo.results.InsertOneResult object at ...>
  >>> mydoc = collection.find_one()
  >>> print(mydoc)
  {u'_id': ObjectId('...'), u'mynumber': Decimal('45.321')}


We can see what's actually being saved to the database by creating a fresh
collection object without the customized codec options and using that to query
MongoDB:

.. doctest::

  >>> vanilla_collection = db.get_collection('test')
  >>> vanilla_collection.find_one()
  {u'_id': ObjectId('...'), u'mynumber': Decimal128('45.321')}
