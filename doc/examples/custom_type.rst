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

We’ll start by getting a clean database to use for the example:

.. doctest::

  >>> from pymongo import MongoClient
  >>> client = MongoClient()
  >>> client.drop_database('custom_type_example')
  >>> db = client.custom_type_example


Since the purpose of the example is to demonstrate working with custom types,
we’ll need a custom data type to use. Here, we define the aptly named
``Custom`` class, which has a single method, ``x()``:

.. doctest::

  >>> class Custom(object):
  ...     def __init__(self, x):
  ...         self.__x = x
  ...
  ...     def x(self):
  ...         return self.__x
  >>> foo = Custom(10)
  >>> foo.x()
  10


When we try to save an instance of ``Custom`` with PyMongo, we'll get an
:exc:`~bson.errors.InvalidDocument` exception since the :mod:`~bson` module
does not have a way to serialize an instance of our custom type:

.. doctest::

  >>> db.test.insert_one({'custom': Custom(5)})
  Traceback (most recent call last):
  ...
  bson.errors.InvalidDocument: Cannot encode object: <__main__.Custom object at ...>


.. _custom-type-type-codec:

The Type Codec
--------------

In order to encode custom types, we must first define a **type codec** for our
type. A type codec describes how an instance of a custom type can be
*transformed* into one of the types :mod:`~bson` already understands, and can
encode. Type codecs must inherit from
:class:`~bson.codec_options.TypeCodecBase`. In order to facilitate encoding of
a custom type, they also must implement the ``python_type`` property, and the
``transform_python`` method. Similarly, for decoding, codecs must implement the
``bson_type`` property and the ``transform_bson`` method. Note that a type
codec does not need to implement both encoding and decoding logic.

While our ``Custom`` type does not place any restrictions on the kind of
data that it can encapsulate, for the sake of this particular exercise, let's
assume that we expect it to only store integers. Without any such restriction
it would be impossible for us to define the decoding logic for our codec since
a BSON value of any type could potentially be an instance of ``Custom``. With
that, we can define our codec:

.. doctest::

  >>> from bson.codec_options import TypeCodecBase
  >>> class CustomTypeCodec(TypeCodecBase):
  ...     @property
  ...     def python_type(self):
  ...         """The Python type acted upon by this type codec."""
  ...         return Custom
  ...
  ...     def transform_python(self, value):
  ...         """Function that transforms a custom type value into a type
  ...         that BSON can encode."""
  ...         return value.x()
  ...
  ...     @property
  ...     def bson_type(self):
  ...         """The BSON type acted upon by this type codec."""
  ...         return int
  ...
  ...     def transform_bson(self, value):
  ...         """Function that transforms a vanilla BSON type value into our
  ...         custom type."""
  ...         return Custom(value)
  >>> custom_type_codec = CustomTypeCodec()


.. _custom-type-type-registry:

The Type Registry
-----------------

Before we can begin encoding and decoding our custom type objects, we must
first inform PyMongo about our type codec. This is done by creating a
:class:`bson.codec_options.TypeRegistry` instance:

.. doctest::

  >>> from bson.codec_options import TypeRegistry
  >>> type_registry = TypeRegistry(custom_type_codec)


Note that type registries can be instantiated with any number of type codecs.
Once instantiated, registries are immutable and the only way to add codecs
to a registry is to create a new one.


Putting it together
-------------------

Finally, we can define a :class:`~bson.codec_options.CodecOptions` instance
with our ``type_registry`` and use it to get a
:class:`~pymongo.collection.Collection` object that understands the ``Custom``
data type:

.. doctest::

  >>> from bson.codec_options import CodecOptions
  >>> codec_options = CodecOptions(type_registry=type_registry)
  >>> collection = db.get_collection('test', codec_options=codec_options)


Now, we can seamlessly encode and decode ``Custom`` type instances:

.. doctest::

  >>> collection.insert_one({'custom': Custom(5)})
  <pymongo.results.InsertOneResult object at ...>
  >>> mydoc = collection.find_one()
  >>> print(mydoc)
  {'_id': ObjectId('...'), 'custom': <Custom object at ...>}
  >>> print(mydoc['custom'].x())
  5


We can see what's actually being saved to the database by creating a new
collection object without the custom codec options and using that to query
MongoDB:

.. doctest::

  >>> vanilla_collection = db.get_collection('test')
  >>> vanilla_collection.find_one()
  {'_id': ObjectId('...'), 'custom': 5}
