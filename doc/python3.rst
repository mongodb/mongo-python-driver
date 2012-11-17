Python 3 FAQ
============

.. contents::

What Python 3 versions are supported?
-------------------------------------

PyMongo supports Python 3.x where x >= 1.

We **do not** support Python 3.0.x. It has many problems
(some that directly impact PyMongo) and was `end-of-lifed`_
with the release of Python 3.1.

.. _end-of-lifed: http://www.python.org/download/releases/3.0.1/

Are there any PyMongo behavior changes with Python 3?
-----------------------------------------------------

Only one intentional change. Instances of :class:`bytes`
are encoded as BSON type 5 (Binary data) with subtype 0.
In Python 3 they are decoded back to :class:`bytes`. In
Python 2 they will be decoded to :class:`~bson.binary.Binary`
with subtype 0.

For example, let's insert a :class:`bytes` instance using Python 3 then
read it back. Notice the byte string is decoded back to :class:`bytes`::

  Python 3.1.4 (default, Mar 21 2012, 14:34:01)
  [GCC 4.5.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  >>> c = pymongo.MongoClient()
  >>> c.test.bintest.insert({'binary': b'this is a byte string'})
  ObjectId('4f9086b1fba5222021000000')
  >>> c.test.bintest.find_one()
  {'binary': b'this is a byte string', '_id': ObjectId('4f9086b1fba5222021000000')}

Now retrieve the same document in Python 2. Notice the byte string is decoded
to :class:`~bson.binary.Binary`::

  Python 2.7.3 (default, Apr 12 2012, 10:35:17)
  [GCC 4.5.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  >>> c = pymongo.MongoClient()
  >>> c.test.bintest.find_one()
  {u'binary': Binary('this is a byte string', 0), u'_id': ObjectId('4f9086b1fba5222021000000')}


Why can't I share pickled ObjectIds between some versions of Python 2 and 3?
----------------------------------------------------------------------------

Instances of :class:`~bson.objectid.ObjectId` pickled using Python 2
can always be unpickled using Python 3. Due to
`http://bugs.python.org/issue13505 <http://bugs.python.org/issue13505>`_
you must use Python 3.2.3 or newer to pickle instances of ObjectId if you
need to unpickle them in Python 2.

If you pickled an ObjectId using Python 2 and want to unpickle it using
Python 3 you must pass ``encoding='latin-1'`` to pickle.loads::

    Python 2.7.3 (default, Apr 12 2012, 10:35:17)
    [GCC 4.5.3] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> from bson.objectid import ObjectId
    >>> oid = ObjectId()
    >>> oid
    ObjectId('4f919ba2fba5225b84000000')
    >>> pickle.dumps(oid)
    'ccopy_reg\n_reconstructor\np0\n(cbson.objectid\...'

    Python 3.1.4 (default, Mar 21 2012, 14:34:01)
    [GCC 4.5.3] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> pickle.loads(b'ccopy_reg\n_reconstructor\np0\n(cbson.objectid\...', encoding='latin-1')
    ObjectId('4f919ba2fba5225b84000000')


If you need to pickle ObjectIds using Python 3 and unpickle them using Python 2
you must use Python 3.2.3 or newer and ``protocol <= 2``::

    Python 3.2.3 (v3.2.3:3d0686d90f55, Apr 10 2012, 11:25:50) 
    [GCC 4.2.1 (Apple Inc. build 5666) (dot 3)] on darwin
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> from bson.objectid import ObjectId
    >>> oid = ObjectId()
    >>> oid
    ObjectId('4f96f20c430ee6bd06000000')
    >>> pickle.dumps(oid, protocol=2)
    b'\x80\x02cbson.objectid\nObjectId\nq\x00)\x81q\x01c_codecs\nencode\...'

    Python 2.4.4 (#1, Oct 18 2006, 10:34:39) 
    [GCC 4.0.1 (Apple Computer, Inc. build 5341)] on darwin
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> pickle.loads('\x80\x02cbson.objectid\nObjectId\nq\x00)\x81q\x01c_codecs\nencode\...')
    ObjectId('4f96f20c430ee6bd06000000')


Unfortunately this won't work if you pickled the ObjectId using a Python 3
version older than 3.2.3::

    Python 3.2.2 (default, Mar 21 2012, 14:32:23) 
    [GCC 4.5.3] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> from bson.objectid import ObjectId
    >>> oid = ObjectId()
    >>> pickle.dumps(oid, protocol=2)
    b'\x80\x02cbson.objectid\nObjectId\nq\x00)\x81q\x01c__builtin__\nbytes\...'

    Python 2.4.6 (#1, Apr 12 2012, 14:48:24) 
    [GCC 4.5.3] on linux3
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> pickle.loads('\x80\x02cbson.objectid\nObjectId\nq\x00)\x81q\x01c__builtin__\nbytes\...')
    Traceback (most recent call last):
      File "<stdin>", line 1, in ?
      File "/usr/lib/python2.4/pickle.py", line 1394, in loads
        return Unpickler(file).load()
      File "/usr/lib/python2.4/pickle.py", line 872, in load
        dispatch[key](self)
      File "/usr/lib/python2.4/pickle.py", line 1104, in load_global
        klass = self.find_class(module, name)
      File "/usr/lib/python2.4/pickle.py", line 1140, in find_class
        klass = getattr(mod, name)
      AttributeError: 'module' object has no attribute 'bytes'

.. warning::

  Unpickling in Python 2.6 or 2.7 an ObjectId pickled in a Python 3 version
  older than 3.2.3 will seem to succeed but the resulting ObjectId instance
  will contain garbage data.

  >>> pickle.loads('\x80\x02cbson.objectid\nObjectId\nq\x00)\x81q\x01c__builtin__\nbytes\...)
  ObjectId('5b37392c203135302c203234362c2034352c203235312c203136352c2033342c203532...')


Why do I get a syntax error importing pymongo after installing from source?
---------------------------------------------------------------------------

PyMongo makes use of the 2to3 tool to translate much of its code to valid
Python 3 syntax at install time. The translated modules are written to the
build subdirectory before being installed, leaving the original source files
intact. If you start the python interactive shell from the top level source
directory after running ``python setup.py install`` the untranslated modules
will be the first thing in your path. Importing pymongo will result in an
exception similar to::

  Python 3.1.5 (default, Jun  2 2012, 12:24:49)
  [GCC 4.6.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "pymongo/__init__.py", line 58, in <module>
      version = get_version_string()
    File "pymongo/__init__.py", line 54, in get_version_string
      if isinstance(version_tuple[-1], basestring):
  NameError: global name 'basestring' is not defined

Note the path in the traceback (``pymongo/__init__.py``). Changing out of the
source directory takes the untranslated modules out of your path::

  $ cd ..
  $ python
  Python 3.1.5 (default, Jun  2 2012, 12:24:49)
  [GCC 4.6.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  >>> pymongo.__file__
  '/home/behackett/py3k/lib/python3.1/site-packages/pymongo-2.2-py3.1-linux-x86_64.egg/pymongo/__init__.py'


