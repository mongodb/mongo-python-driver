Python 3 FAQ
============

.. contents::

What Python 3 versions are supported?
-------------------------------------

PyMongo supports CPython 3.7+ and PyPy3.7+.

Are there any PyMongo behavior changes with Python 3?
-----------------------------------------------------

Only one intentional change. Instances of :class:`bytes`
are encoded as BSON type 5 (Binary data) with subtype 0.
In Python 3 they are decoded back to :class:`bytes`. In
Python 2 they are decoded to :class:`~bson.binary.Binary`
with subtype 0.

For example, let's insert a :class:`bytes` instance using Python 3 then
read it back. Notice the byte string is decoded back to :class:`bytes`::

  Python 3.7.9 (v3.7.9:13c94747c7, Aug 15 2020, 01:31:08)
  [Clang 6.0 (clang-600.0.57)] on darwin
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  >>> c = pymongo.MongoClient()
  >>> c.test.bintest.insert_one({'binary': b'this is a byte string'}).inserted_id
  ObjectId('4f9086b1fba5222021000000')
  >>> c.test.bintest.find_one()
  {'binary': b'this is a byte string', '_id': ObjectId('4f9086b1fba5222021000000')}

Now retrieve the same document in Python 2. Notice the byte string is decoded
to :class:`~bson.binary.Binary`::

  Python 2.7.6 (default, Feb 26 2014, 10:36:22)
  [GCC 4.7.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  >>> c = pymongo.MongoClient()
  >>> c.test.bintest.find_one()
  {u'binary': Binary('this is a byte string', 0), u'_id': ObjectId('4f9086b1fba5222021000000')}


There is a similar change in behavior in parsing JSON binary with subtype 0.
In Python 3 they are decoded into :class:`bytes`. In Python 2 they are
decoded to :class:`~bson.binary.Binary` with subtype 0.

For example, let's decode a JSON binary subtype 0 using Python 3. Notice the
byte string is decoded to :class:`bytes`::

  Python 3.7.9 (v3.7.9:13c94747c7, Aug 15 2020, 01:31:08)
  [Clang 6.0 (clang-600.0.57)] on darwin
  Type "help", "copyright", "credits" or "license" for more information.
  >>> from bson.json_util import loads
  >>> loads('{"b": {"$binary": "dGhpcyBpcyBhIGJ5dGUgc3RyaW5n", "$type": "00"}}')
  {'b': b'this is a byte string'}

Now decode the same JSON in Python 2 . Notice the byte string is decoded
to :class:`~bson.binary.Binary`::

  Python 2.7.10 (default, Feb  7 2017, 00:08:15)
  [GCC 4.2.1 Compatible Apple LLVM 8.0.0 (clang-800.0.34)] on darwin
  Type "help", "copyright", "credits" or "license" for more information.
  >>> from bson.json_util import loads
  >>> loads('{"b": {"$binary": "dGhpcyBpcyBhIGJ5dGUgc3RyaW5n", "$type": "00"}}')
  {u'b': Binary('this is a byte string', 0)}

Why can't I share pickled ObjectIds between some versions of Python 2 and 3?
----------------------------------------------------------------------------

Instances of :class:`~bson.objectid.ObjectId` pickled using Python 2
can always be unpickled using Python 3.

If you pickled an ObjectId using Python 2 and want to unpickle it using
Python 3 you must pass ``encoding='latin-1'`` to pickle.loads::

  Python 2.7.6 (default, Feb 26 2014, 10:36:22)
  [GCC 4.7.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pickle
  >>> from bson.objectid import ObjectId
  >>> oid = ObjectId()
  >>> oid
  ObjectId('4f919ba2fba5225b84000000')
  >>> pickle.dumps(oid)
  'ccopy_reg\n_reconstructor\np0\n(cbson.objectid\...'

  Python 3.7.9 (v3.7.9:13c94747c7, Aug 15 2020, 01:31:08)
  [Clang 6.0 (clang-600.0.57)] on darwin
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pickle
  >>> pickle.loads(b'ccopy_reg\n_reconstructor\np0\n(cbson.objectid\...', encoding='latin-1')
  ObjectId('4f919ba2fba5225b84000000')


If you need to pickle ObjectIds using Python 3 and unpickle them using Python 2
you must use ``protocol <= 2``::

  Python 3.7.9 (v3.7.9:13c94747c7, Aug 15 2020, 01:31:08)
  [Clang 6.0 (clang-600.0.57)] on darwin
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pickle
  >>> from bson.objectid import ObjectId
  >>> oid = ObjectId()
  >>> oid
  ObjectId('4f96f20c430ee6bd06000000')
  >>> pickle.dumps(oid, protocol=2)
  b'\x80\x02cbson.objectid\nObjectId\nq\x00)\x81q\x01c_codecs\nencode\...'

  Python 2.7.15 (default, Jun 21 2018, 15:00:48)
  [GCC 7.3.0] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pickle
  >>> pickle.loads('\x80\x02cbson.objectid\nObjectId\nq\x00)\x81q\x01c_codecs\nencode\...')
  ObjectId('4f96f20c430ee6bd06000000')
