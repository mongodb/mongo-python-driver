Custom Type Example
===================

This is an example of using a custom type with PyMongo. The example here is a
bit contrived, but shows how to use a :class:`~bson.codec_options.TypeRegistry`
to manipulate documents as they are saved or retrieved from MongoDB.


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
  bson.errors.InvalidDocument: Cannot encode object: <__main__.Custom object at 0x106f46c18>


.. _custom-type-type-codec:

The Type Codec
--------------

In order to encode custom types, we must first define a **type codec** for our
type. A type codec describes how an instance of a custom type can be
*transformed* into one of the types :mod:`~bson` already understands, and can
encode.

Type codecs must inherit from :class:`bson.codec_options.TypeCodecBase`. In
order to facilitate encoding of a custom type, they also must implement
the ``python_type`` property, along with the ``transform_python`` method.
Similarly, for decoding, they must implement the ``bson_type`` property and
the ``transform_bson`` method. Note that a type codec does not need to
implement both encoding and decoding logic for any give type.

While our ``Custom`` type does not place any restrictions on the kind of
data that it can hold, for the sake of this particular exercise, let us assume
that we expect it to hold integers. Without any such restriction it would be
impossible for us to define the decoding logic for our codec since a BSON
value of any type could potentially be an instance of ``Custom``. With that,
we can define our codec:

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
  ...         return value.x()
  >>> custom_type_codec = CustomTypeCodec()


.. _custom-type-type-registry:

The Type Registry
-----------------

Before we can begin encoding and decoding our custom type objects, we must
first inform PyMongo about our type codec. This is achieved with the help of a
:class:`bson.codec_options.TypeRegistry` instance. Creating a registry is
trivial:

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


Now, we can transparently encode and decode ``Custom`` type instances. As long
as we use the :class:`~pymongo.collection.Collection` that has been properly
setup, the BSON library will do the heavy-lifting for us:

.. doctest::

  >>> collection.insert_one({'custom': Custom(5)})
  <pymongo.results.InsertOneResult at 0x1076bb348>
  >>> mydoc = collection.find_one()
  >>> print(mydoc)
  {'_id': ObjectId('5c8161350c944094f971aeff'), 'custom': <__main__.Custom object at 0x107716a20>}
  >>> print(mydoc['custom'].x())
  5