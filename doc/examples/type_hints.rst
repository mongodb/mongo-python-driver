
.. _type_hints-example:

Type Hints
===========

As of version 4.1, PyMongo ships with `type hints`_. With type hints, Python
type checkers can easily find bugs before they reveal themselves in your code.

If your IDE is configured to use type hints,
it can suggest more appropriate completions and highlight errors in your code.
Some examples include `PyCharm`_,  `Sublime Text`_, and `Visual Studio Code`_.

You can also use the `mypy`_ tool from your command line or in Continuous Integration tests.

All of the public APIs in PyMongo are fully type hinted, and
several of them support generic parameters for the
type of document object returned when decoding BSON documents.

Due to `limitations in mypy`_, the default
values for generic document types are not yet provided (they will eventually be ``Dict[str, any]``).

For a larger set of examples that use types, see the PyMongo `test_mypy module`_.

If you would like to opt out of using the provided types, add the following to
your `mypy config`_: ::

    [mypy-pymongo]
    follow_imports = False


Basic Usage
-----------

Note that a type for :class:`~pymongo.mongo_client.MongoClient` must be specified.  Here we use the
default, unspecified document type:

.. doctest::

  >>> from pymongo import MongoClient
  >>> client: MongoClient = MongoClient()
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"x": 1, "tags": ["dog", "cat"]})
  >>> retrieved = collection.find_one({"x": 1})
  >>> assert isinstance(retrieved, dict)

For a more accurate typing for document type you can use:

.. doctest::

  >>> from typing import Any, Dict
  >>> from pymongo import MongoClient
  >>> client: MongoClient[Dict[str, Any]] = MongoClient()
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"x": 1, "tags": ["dog", "cat"]})
  >>> retrieved = collection.find_one({"x": 1})
  >>> assert isinstance(retrieved, dict)

Typed Client
------------

:class:`~pymongo.mongo_client.MongoClient` is generic on the document type used to decode BSON documents.

You can specify a :class:`~bson.raw_bson.RawBSONDocument` document type:

.. doctest::

  >>> from pymongo import MongoClient
  >>> from bson.raw_bson import RawBSONDocument
  >>> client = MongoClient(document_class=RawBSONDocument)
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"x": 1, "tags": ["dog", "cat"]})
  >>> result = collection.find_one({"x": 1})
  >>> assert isinstance(result, RawBSONDocument)

Subclasses of :py:class:`collections.abc.Mapping` can also be used, such as :class:`~bson.son.SON`:

.. doctest::

  >>> from bson import SON
  >>> from pymongo import MongoClient
  >>> client = MongoClient(document_class=SON[str, int])
  >>> collection = client.test.test
  >>> inserted = collection.insert_one({"x": 1, "y": 2 })
  >>> result = collection.find_one({"x": 1})
  >>> assert result is not None
  >>> assert result["x"] == 1

Note that when using :class:`~bson.son.SON`, the key and value types must be given, e.g. ``SON[str, Any]``.


Typed Collection
----------------

You can use :py:class:`~typing.TypedDict` (Python 3.8+) when using a well-defined schema for the data in a :class:`~pymongo.collection.Collection`:

.. doctest::

  >>> from typing import TypedDict
  >>> from pymongo import MongoClient
  >>> from pymongo.collection import Collection
  >>> class Movie(TypedDict):
  ...       name: str
  ...       year: int
  ...
  >>> client: MongoClient = MongoClient()
  >>> collection: Collection[Movie] = client.test.test
  >>> inserted = collection.insert_one({"name": "Jurassic Park", "year": 1993 })
  >>> result = collection.find_one({"name": "Jurassic Park"})
  >>> assert result is not None
  >>> assert result["year"] == 1993

Typed Database
--------------

While less common, you could specify that the documents in an entire database
match a well-defined shema using :py:class:`~typing.TypedDict` (Python 3.8+).


.. doctest::

  >>> from typing import TypedDict
  >>> from pymongo import MongoClient
  >>> from pymongo.database import Database
  >>> class Movie(TypedDict):
  ...       name: str
  ...       year: int
  ...
  >>> client: MongoClient = MongoClient()
  >>> db: Database[Movie] = client.test
  >>> collection = db.test
  >>> inserted = collection.insert_one({"name": "Jurassic Park", "year": 1993 })
  >>> result = collection.find_one({"name": "Jurassic Park"})
  >>> assert result is not None
  >>> assert result["year"] == 1993

Typed Command
-------------
When using the :meth:`~pymongo.database.Database.command`, you can specify the document type by providing a custom :class:`~bson.codec_options.CodecOptions`:

.. doctest::

  >>> from pymongo import MongoClient
  >>> from bson.raw_bson import RawBSONDocument
  >>> from bson import CodecOptions
  >>> client: MongoClient = MongoClient()
  >>> options = CodecOptions(RawBSONDocument)
  >>> result = client.admin.command("ping", codec_options=options)
  >>> assert isinstance(result, RawBSONDocument)

Custom :py:class:`collections.abc.Mapping` subclasses and :py:class:`~typing.TypedDict` (Python 3.8+) are also supported.
For :py:class:`~typing.TypedDict`, use the form: ``options: CodecOptions[MyTypedDict] = CodecOptions(...)``.

Typed BSON Decoding
-------------------
You can specify the document type returned by :mod:`bson` decoding functions by providing :class:`~bson.codec_options.CodecOptions`:

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

:class:`~bson.raw_bson.RawBSONDocument` and :py:class:`~typing.TypedDict` (Python 3.8+) are also supported.
For :py:class:`~typing.TypedDict`, use  the form: ``options: CodecOptions[MyTypedDict] = CodecOptions(...)``.


Troubleshooting
---------------

Client Type Annotation
~~~~~~~~~~~~~~~~~~~~~~
If you forget to add a type annotation for a :class:`~pymongo.mongo_client.MongoClient` object you may get the followig ``mypy`` error::

  from pymongo import MongoClient
  client = MongoClient()  # error: Need type annotation for "client"

The solution is to annotate the type as ``client: MongoClient`` or ``client: MongoClient[Dict[str, Any]]``.  See `Basic Usage`_.

Incompatible Types
~~~~~~~~~~~~~~~~~~
If you use the generic form of :class:`~pymongo.mongo_client.MongoClient` you
may encounter a ``mypy`` error like::

  from pymongo import MongoClient

  client: MongoClient = MongoClient()
  client.test.test.insert_many(
      {"a": 1}
  )  # error: Dict entry 0 has incompatible type "str": "int";
     # expected "Mapping[str, Any]": "int"


The solution is to use ``client: MongoClient[Dict[str, Any]]`` as used in
`Basic Usage`_ .

Actual Type Errors
~~~~~~~~~~~~~~~~~~

Other times ``mypy`` will catch an actual error, like the following code::

    from pymongo import MongoClient
    from typing import Mapping
    client: MongoClient = MongoClient()
    client.test.test.insert_one(
        [{}]
    )  # error: Argument 1 to "insert_one" of "Collection" has
       # incompatible type "List[Dict[<nothing>, <nothing>]]";
       # expected "Mapping[str, Any]"

In this case the solution is to use ``insert_one({})``, passing a document instead of a list.

Another example is trying to set a value on a :class:`~bson.raw_bson.RawBSONDocument`, which is read-only.::

    from bson.raw_bson import RawBSONDocument
    from pymongo import MongoClient

    client = MongoClient(document_class=RawBSONDocument)
    coll = client.test.test
    doc = {"my": "doc"}
    coll.insert_one(doc)
    retreived = coll.find_one({"_id": doc["_id"]})
    assert retreived is not None
    assert len(retreived.raw) > 0
    retreived[
        "foo"
    ] = "bar"  # error: Unsupported target for indexed assignment
               # ("RawBSONDocument")  [index]

.. _PyCharm: https://www.jetbrains.com/help/pycharm/type-hinting-in-product.html
.. _Visual Studio Code: https://code.visualstudio.com/docs/languages/python
.. _Sublime Text: https://github.com/sublimelsp/LSP-pyright
.. _type hints: https://docs.python.org/3/library/typing.html
.. _mypy: https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html
.. _limitations in mypy: https://github.com/python/mypy/issues/3737
.. _mypy config: https://mypy.readthedocs.io/en/stable/config_file.html
.. _test_mypy module: https://github.com/mongodb/mongo-python-driver/blob/master/test/test_mypy.py
