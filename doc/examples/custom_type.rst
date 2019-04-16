Custom Type Example
===================

This is an example of using a custom type with PyMongo. The example here shows
how to subclass :class:`~bson.codec_options.TypeCodec` to write a type
codec, which is used to populate a :class:`~bson.codec_options.TypeRegistry`.
The type registry can then be used to create a custom-type-aware
:class:`~pymongo.collection.Collection`. Read and write operations
issued against the resulting collection object transparently manipulate
documents as they are saved to or retrieved from MongoDB.


Setting Up
----------

We'll start by getting a clean database to use for the example:

.. doctest::

  >>> from pymongo import MongoClient
  >>> client = MongoClient()
  >>> client.drop_database('custom_type_example')
  >>> db = client.custom_type_example


Since the purpose of the example is to demonstrate working with custom types,
we'll need a custom data type to use. For this example, we will be working with
the :py:class:`~decimal.Decimal` type from Python's standard library. Since the
BSON library's :class:`~bson.decimal128.Decimal128` type (that implements
the IEEE 754 decimal128 decimal-based floating-point numbering format) is
distinct from Python's built-in :py:class:`~decimal.Decimal` type, attempting
to save an instance of ``Decimal`` with PyMongo, results in an
:exc:`~bson.errors.InvalidDocument` exception.

.. doctest::

  >>> from decimal import Decimal
  >>> num = Decimal("45.321")
  >>> db.test.insert_one({'num': num})
  Traceback (most recent call last):
  ...
  bson.errors.InvalidDocument: Cannot encode object: <__main__.Decimal object at ...>


.. _custom-type-type-codec:

The :class:`~bson.codec_options.TypeCodec` Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 3.8

In order to encode a custom type, we must first define a **type codec** for
that type. A type codec describes how an instance of a custom type can be
*transformed* to and/or from one of the types :mod:`~bson` already understands.
Depending on the desired functionality, users must choose from the following
base classes when defining type codecs:

* :class:`~bson.codec_options.TypeEncoder`: subclass this to define a codec that
  encodes a custom Python type to a known BSON type. Users must implement the
  ``python_type`` property/attribute and the ``transform_python`` method.
* :class:`~bson.codec_options.TypeDecoder`: subclass this to define a codec that
  decodes a specified BSON type into a custom Python type. Users must implement
  the ``bson_type`` property/attribute and the ``transform_bson`` method.
* :class:`~bson.codec_options.TypeCodec`: subclass this to define a codec that
  can both encode and decode a custom type. Users must implement the
  ``python_type`` and ``bson_type`` properties/attributes, as well as the
  ``transform_python`` and ``transform_bson`` methods.


The type codec for our custom type simply needs to define how a
:py:class:`~decimal.Decimal` instance can be converted into a
:class:`~bson.decimal128.Decimal128` instance and vice-versa. Since we are
interested in both encoding and decoding our custom type, we use the
``TypeCodec`` base class to define our codec:

.. doctest::

  >>> from bson.decimal128 import Decimal128
  >>> from bson.codec_options import TypeCodec
  >>> class DecimalCodec(TypeCodec):
  ...     python_type = Decimal    # the Python type acted upon by this type codec
  ...     bson_type = Decimal128   # the BSON type acted upon by this type codec
  ...     def transform_python(self, value):
  ...         """Function that transforms a custom type value into a type
  ...         that BSON can encode."""
  ...         return Decimal128(value)
  ...     def transform_bson(self, value):
  ...         """Function that transforms a vanilla BSON type value into our
  ...         custom type."""
  ...         return value.to_decimal()
  >>> decimal_codec = DecimalCodec()


.. _custom-type-type-registry:

The :class:`~bson.codec_options.TypeRegistry` Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 3.8

Before we can begin encoding and decoding our custom type objects, we must
first inform PyMongo about the corresponding codec. This is done by creating
a :class:`~bson.codec_options.TypeRegistry` instance:

.. doctest::

  >>> from bson.codec_options import TypeRegistry
  >>> type_registry = TypeRegistry([decimal_codec])


Note that type registries can be instantiated with any number of type codecs.
Once instantiated, registries are immutable and the only way to add codecs
to a registry is to create a new one.


Putting It Together
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
  the situation where a user wants to *encode* documents containing either
  of these types.

After creating a new codec options object and using it to get a collection
object, we can seamlessly encode instances of ``DecimalInt``:

.. doctest::

  >>> type_registry = TypeRegistry([decimal_codec, decimalint_codec])
  >>> codec_options = CodecOptions(type_registry=type_registry)
  >>> collection = db.get_collection('test', codec_options=codec_options)
  >>> collection.drop()
  >>> collection.insert_one({'num': DecimalInt("45.321")})
  <pymongo.results.InsertOneResult object at ...>
  >>> mydoc = collection.find_one()
  >>> pprint.pprint(mydoc)
  {u'_id': ObjectId('...'), u'num': Decimal('45.321')}

Note that the ``transform_bson`` method of the base codec class results in
these values being decoded as ``Decimal`` (and not ``DecimalInt``).


.. _decoding-binary-types:

Decoding :class:`~bson.binary.Binary` Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The decoding treatment of :class:`~bson.binary.Binary` types having
``subtype = 0`` by the :mod:`bson` module varies slightly depending on the
version of the Python runtime in use. This must be taken into account while
writing a ``TypeDecoder`` that modifies how this datatype is decoded.

On Python 3.x, :class:`~bson.binary.Binary` data (``subtype = 0``) is decoded
as a ``bytes`` instance:

.. code-block:: python

    >>> # On Python 3.x.
    >>> from bson.binary import Binary
    >>> newcoll = db.get_collection('new')
    >>> newcoll.insert_one({'_id': 1, 'data': Binary(b"123", subtype=0)})
    >>> doc = newcoll.find_one()
    >>> type(doc['data'])
    bytes


On Python 2.7.x, the same data is decoded as a :class:`~bson.binary.Binary`
instance:

.. code-block:: python

    >>> # On Python 2.7.x
    >>> newcoll = db.get_collection('new')
    >>> doc = newcoll.find_one()
    >>> type(doc['data'])
    bson.binary.Binary


As a consequence of this disparity, users must set the ``bson_type`` attribute
on their :class:`~bson.codec_options.TypeDecoder` classes differently,
depending on the python version in use.


.. note::

  For codebases requiring compatibility with both Python 2 and 3, type
  decoders will have to be registered for both possible ``bson_type`` values.


.. _fallback-encoder-callable:

The ``fallback_encoder`` Callable
---------------------------------

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


.. note::

  Fallback encoders are invoked *after* attempts to encode the given value
  with standard BSON encoders and any configured type encoders have failed.
  Therefore, in a type registry configured with a type encoder and fallback
  encoder that both target the same custom type, the behavior specified in
  the type encoder will prevail.


Because fallback encoders don't need to declare the types that they encode
beforehand, they can be used to support interesting use-cases that cannot be
serviced by ``TypeEncoder``. One such use-case is described in the next
section.


Encoding Unknown Types
^^^^^^^^^^^^^^^^^^^^^^

In this example, we demonstrate how a fallback encoder can be used to save
arbitrary objects to the database. We will use the the standard library's
:py:mod:`pickle` module to serialize the unknown types and so naturally, this
approach only works for types that are picklable.

We start by defining some arbitrary custom types:

.. code-block:: python

  class MyStringType(object):
      def __init__(self, value):
          self.__value = value
      def __repr__(self):
          return "MyStringType('%s')" % (self.__value,)

  class MyNumberType(object):
      def __init__(self, value):
          self.__value = value
      def __repr__(self):
          return "MyNumberType(%s)" % (self.__value,)

We also define a fallback encoder that pickles whatever objects it receives
and returns them as :class:`~bson.binary.Binary` instances with a custom
subtype. The custom subtype, in turn, allows us to write a TypeDecoder that
identifies pickled artifacts upon retrieval and transparently decodes them
back into Python objects:

.. code-block:: python

  import pickle
  from bson.binary import Binary, USER_DEFINED_SUBTYPE
  def fallback_pickle_encoder(value):
      return Binary(pickle.dumps(value), USER_DEFINED_SUBTYPE)

  class PickledBinaryDecoder(TypeDecoder):
      bson_type = Binary
      def transform_bson(self, value):
          if value.subtype == USER_DEFINED_SUBTYPE:
              return pickle.loads(value)
          return value


.. note::

  The above example is written assuming the use of Python 3. If you are using
  Python 2, ``bson_type`` must be set to ``Binary``. See the
  :ref:`decoding-binary-types` section for a detailed explanation.


Finally, we create a ``CodecOptions`` instance:

.. code-block:: python

  codec_options = CodecOptions(type_registry=TypeRegistry(
      [PickledBinaryDecoder()], fallback_encoder=fallback_pickle_encoder))

We can now round trip our custom objects to MongoDB:

.. code-block:: python

  collection = db.get_collection('test_fe', codec_options=codec_options)
  collection.insert_one({'_id': 1, 'str': MyStringType("hello world"),
                         'num': MyNumberType(2)})
  mydoc = collection.find_one()
  assert isinstance(mydoc['str'], MyStringType)
  assert isinstance(mydoc['num'], MyNumberType)


Limitations
-----------

PyMongo's type codec and fallback encoder features have the following
limitations:

#. Users cannot customize the encoding behavior of Python types that PyMongo
   already understands like ``int`` and ``str`` (the 'built-in types').
   Attempting to instantiate a type registry with one or more codecs that act
   upon a built-in type results in a ``TypeError``. This limitation extends
   to all subtypes of the standard types.
#. Chaining type encoders is not supported. A custom type value, once
   transformed by a codec's ``transform_python`` method, *must* result in a
   type that is either BSON-encodable by default, or can be
   transformed by the fallback encoder into something BSON-encodable--it
   *cannot* be transformed a second time by a different type codec.
#. The :meth:`~pymongo.database.Database.command` method does not apply the
   user's TypeDecoders while decoding the command response document.
#. :mod:`gridfs` does not apply custom type encoding or decoding to any
   documents received from or to returned to the user.
