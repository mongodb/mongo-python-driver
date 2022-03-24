
.. _type_hints-example:

Type Hints
===========

As of version 4.1, PyMongo ships with `type hints`_.

With type hints, Python type checkers can easily find bugs before they reveal themselves in your code.  If your IDE is configured to use type hints,
it can suggest more appropriate completions and highlight errors in your code.

All of the public APIs in PyMongo are fully type hinted, and
several of them support generic parameters for the
type of document object returned by methods.

Due to `limitations in mypy`_, the default
values for generics are not yet provided (they will eventually be ``Dict[str, any]``).

If you would like to opt out of using the provided types, add the following to
your `mypy config`_: ::

    [mypy-pymongo]
    follow_imports = False


Basic Usage
-----------

Note that a type for :class:`~pymongo.mongo_client.MongoClient` must be specified.  Here we use the
default, specified type:

.. doctest::

  >>> from pymongo import MongoClient
  >>> client: MongoClient = MongoClient()
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"x": 1, "tags": ["dog", "cat"]})
  >>> retrieved = collection.find_one({"x": 1})
  >>> assert isinstance(retrieved, dict)

You can also use ``MongoClient[Dict[str, Any]]`` for a more accurate typing.

Client Types
------------

:class:`~pymongo.mongo_client.MongoClient`is generic on the document type returned by methods.
You can specify a :class:`~bson.raw_bson.RawBSONDocument` document type:

.. doctest::

  >>> from pymongo import MongoClient
  >>> from bson.raw_bson import RawBSONDocument
  >>> client = MongoClient(document_class=RawBSONDocument)
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"x": 1, "tags": ["dog", "cat"]})
  >>> result = collection.find_one({"x": 1})
  >>> assert isinstance(result, RawBSONDocument)

Another option is to use a custom :py:class:`~typing.TypedDict` when using a well-defined schema:

.. doctest::

  >>> from typing import TypedDict
  >>> from pymongo import MongoClient
  >>> class Movie(TypedDict):
  ...       name: str
  ...       year: int
  ...
  >>> client: MongoClient[Movie] = MongoClient()
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"name": "Jurassic Park", "year": 1993 })
  >>> result = collection.find_one({"name": "Jurassic Park"})
  >>> assert result is not None
  >>> assert result["year"] == 1993

Custom classes that subclass :py:class:`collections.abc.Mapping` can also be used, such as :class:`~bson.son.SON`:

.. doctest::

  >>> from bson import SON
  >>> from pymongo import MongoClient
  >>> client = MongoClient(document_class=SON[str, int])
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"x": 1, "y": 2 })
  >>> result = collection.find_one({"x": 1})
  >>> assert result is not None
  >>> assert result["x"] == 1


Database Command Types
----------------------
The :meth:`~pymongo.database.Database.command` method can also be used directly with generic types by providing a custom :class:`~bson.codec_options.CodecOptions`:

.. doctest::

  >>> from pymongo import MongoClient
  >>> from bson.raw_bson import RawBSONDocument
  >>> from bson import CodecOptions
  >>> client: MongoClient = MongoClient()
  >>> options = CodecOptions(RawBSONDocument)
  >>> result = client.admin.command("ping", codec_options=options)
  >>> assert isinstance(result, RawBSONDocument)

Custom :py:class:`collections.abc.Mapping` subclasses and :py:class:`~typing.TypedDict` are also supported.
For :py:class:`~typing.TypedDict`, use the form ``options: CodecOptions[MyTypedDict] = CodecOptions(...)``.


BSON Decoding Types
-------------------
Finally, the :mod:`bson` decoding functions can be used with generic types by providing a custom :class:`~bson.codec_options.CodecOptions`:

.. doctest::

  >>> from typing import Any, Dict
  >>> from bson import CodecOptions, encode, decode
  >>> class MyDict(Dict[str, Any]):
  ...       def foo(self):
  ...           return "bar"
  ...
  >>> options = CodecOptions(document_class=MyDict)
  >>> doc = {"x": 1, "y": 2 }
  >>> bsonbytes = encode(doc, codec_options=options)
  >>> rt_document = decode(bsonbytes, codec_options=options)
  >>> assert rt_document.foo() == "bar"

:class:`~bson.raw_bson.RawBSONDocument` and :py:class:`~typing.TypedDict` are also supported.
For :py:class:`~typing.TypedDict`, use  the form ``options: CodecOptions[MyTypedDict] = CodecOptions(...)``.


.. _type hints: https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html
.. _limitations in mypy: https://github.com/python/mypy/issues/3737
.. _TypedDict: https://docs.python.org/3/library/typing.html#typing.TypedDict
.. _mypy config: https://mypy.readthedocs.io/en/stable/config_file.html
