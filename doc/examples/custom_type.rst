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
  >>> num = Decimal("45.321")
  >>> db.test.insert_one({'num': num})
  Traceback (most recent call last):
  ...
  bson.errors.InvalidDocument: Cannot encode object: <__main__.Decimal object at ...>


.. _custom-type-type-codec:

The Type Codec
--------------

.. versionadded:: 3.8

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

.. versionadded:: 3.8

Before we can begin encoding and decoding our custom type objects, we must
first inform PyMongo about our type codec. This is done by creating a
:class:`~bson.codec_options.TypeRegistry` instance:

.. doctest::

  >>> from bson.codec_options import TypeRegistry
  >>> type_registry = TypeRegistry([decimal_codec])


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

  >>> collection.insert_one({'num': Decimal("45.321")})
  <pymongo.results.InsertOneResult object at ...>
  >>> mydoc = collection.find_one()
  >>> import pprint
  >>> pprint.pprint(mydoc)
  {u'_id': ObjectId('...'), u'num': Decimal('45.321')}


We can see what's actually being saved to the database by creating a fresh
collection object without the customized codec options and using that to query
MongoDB:

.. doctest::

  >>> vanilla_collection = db.get_collection('test')
  >>> pprint.pprint(vanilla_collection.find_one())
  {u'_id': ObjectId('...'), u'num': Decimal128('45.321')}


Encoding Subtypes
^^^^^^^^^^^^^^^^^

Consider the situation where, in addition to encoding
:py:class:`~decimal.Decimal`, we also need to encode a type that subclasses
``Decimal``. PyMongo does this automatically for types that inherit from
Python types that are BSON-encodable by default, but the type codec system
described above does not offer the same flexibility.

Consider this subtype of ``Decimal`` that has a method to return its value as
an integer:

.. doctest::

  >>> class DecimalInt(Decimal):
  ...     def my_method(self):
  ...         """Method implementing some custom logic."""
  ...         return int(self)

If we try to save an instance of this type without first registering a type
codec for it, we get an error:

.. doctest::

  >>> collection.insert_one({'num': DecimalInt("45.321")})
  Traceback (most recent call last):
  ...
  bson.errors.InvalidDocument: Cannot encode object: Decimal('45.321')

In order to proceed further, we must define a type codec for ``DecimalInt``.
This is trivial to do since the same transformation as the one used for
``Decimal`` is adequate for encoding ``DecimalInt`` as well:

.. doctest::

  >>> class DecimalIntCodec(DecimalCodec):
  ...     @property
  ...     def python_type(self):
  ...         """The Python type acted upon by this type codec."""
  ...         return DecimalInt
  >>> decimalint_codec = DecimalIntCodec()


.. note::

  No attempt is made to modify decoding behavior because without additional
  information, it is impossible to discern which incoming
  :class:`~bson.decimal128.Decimal128` value needs to be decoded as ``Decimal``
  and which needs to be decoded as ``DecimalInt``. This example only considers
  the situation where a user wants to *encode* documents containing one or both
  of these types.

Now, we can create a new codec options object and use it to get a collection
object:

.. doctest::

  >>> type_registry = TypeRegistry([decimal_codec, decimalint_codec])
  >>> codec_options = CodecOptions(type_registry=type_registry)
  >>> collection = db.get_collection('test', codec_options=codec_options)
  >>> collection.drop()


We can now seamlessly encode instances of ``DecimalInt``. Note that the
``transform_bson`` method of the base codec class results in these values
being decoded as ``Decimal`` (and not ``DecimalInt``):

.. doctest::

  >>> collection.insert_one({'num': DecimalInt("45.321")})
  <pymongo.results.InsertOneResult object at ...>
  >>> mydoc = collection.find_one()
  >>> pprint.pprint(mydoc)
  {u'_id': ObjectId('...'), u'num': Decimal('45.321')}


The Fallback Encoder
--------------------

.. versionadded:: 3.8


In addition to type codecs, users can also register a callable to encode types
that BSON doesn't recognize and for which no type codec has been registered.
This callable is the **fallback encoder** and like the ``transform_python``
method, it accepts an unencodable value as a parameter and returns a
BSON-encodable value. The following fallback encoder encodes python's
:py:class:`~decimal.Decimal` type to a :class:`~bson.decimal128.Decimal128`:

.. doctest::

  >>> def fallback_encoder(value):
  ...     if isinstance(value, Decimal):
  ...         return Decimal128(value)
  ...     return value

After declaring the callback, we must create a type registry and codec options
with this fallback encoder before it can be used for initializing a collection:

.. doctest::

  >>> type_registry = TypeRegistry(fallback_encoder=fallback_encoder)
  >>> codec_options = CodecOptions(type_registry=type_registry)
  >>> collection = db.get_collection('test', codec_options=codec_options)
  >>> collection.drop()

We can now seamlessly encode instances of :py:class:`~decimal.Decimal`:

.. doctest::

  >>> collection.insert_one({'num': Decimal("45.321")})
  <pymongo.results.InsertOneResult object at ...>
  >>> mydoc = collection.find_one()
  >>> pprint.pprint(mydoc)
  {u'_id': ObjectId('...'), u'num': Decimal128('45.321')}

As you can tell, fallback encoders are a compelling alternative to type codecs
when we only want to encode custom types due to their much simpler API.
Users should note however, that fallback encoders cannot be used to modify the
encoding of types that PyMongo already understands, as illustrated by the
following example:

  >>> def fallback_encoder(value):
  ...     """Encoder that converts floats to int."""
  ...     if isinstance(value, float):
  ...         return int(value)
  ...     return value
  >>> type_registry = TypeRegistry(fallback_encoder=fallback_encoder)
  >>> codec_options = CodecOptions(type_registry=type_registry)
  >>> collection = db.get_collection('test', codec_options=codec_options)
  >>> collection.drop()
  >>> collection.insert_one({'num': 45.321})
  <pymongo.results.InsertOneResult object at ...>
  >>> mydoc = collection.find_one()
  >>> pprint.pprint(mydoc)
  {u'_id': ObjectId('...'), u'num': 45.321}

This is due to the fact that fallback encoders are invoked only after
an attempt to encode the value with type codecs and standard BSON encoding
routines has been unsuccessful.