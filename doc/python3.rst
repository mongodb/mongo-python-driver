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
  >>> c = pymongo.Connection()
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
  >>> c = pymongo.Connection()
  >>> c.test.bintest.find_one()
  {u'binary': Binary('this is a byte string', 0), u'_id': ObjectId('4f9086b1fba5222021000000')}


Are there any issues migrating from Python 2 to Python 3?
---------------------------------------------------------

There are some issues sharing pickled instances of
:class:`~bson.objectid.ObjectId` between Python versions.
:class:`~bson.objectid.ObjectId` instances are implemented
internally as packed binary data (:class:`str` in Python 2,
:class:`bytes` in Python 3).

Changes have been made to allow unpickling in Python 3 of instances
pickled in Python 2. You just have to use the encoding parameter
to pickle.loads::

    Python 2.7.3 (default, Apr 12 2012, 10:35:17)
    [GCC 4.5.3] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> from bson.objectid import ObjectId
    >>> oid = ObjectId()
    >>> oid
    ObjectId('4f919ba2fba5225b84000000')
    >>> pickle.dumps(oid)
    'ccopy_reg\n_reconstructor\np0\n(cbson.objectid\nObjectId\np1\nc__builtin__\nobject\np2\nNtp3\nRp4\nS\'O\\x91\\x9b\\xa2\\xfb\\xa5"[\\x84\\x00\\x00\\x00\'\np5\nb.'

    Python 3.1.4 (default, Mar 21 2012, 14:34:01)
    [GCC 4.5.3] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pickle
    >>> pickle.loads(b'ccopy_reg\n_reconstructor\np0\n(cbson.objectid\nObjectId\np1\nc__builtin__\nobject\np2\nNtp3\nRp4\nS\'O\\x91\\x9b\\xa2\\xfb\\xa5"[\\x84\\x00\\x00\\x00\'\np5\nb.', encoding='latin-1')
    ObjectId('4f919ba2fba5225b84000000')

Unfortunately there isn't currently a way to unpickle in Python 2
instances of ObjectId pickled in Python 3. Python 2.4 and 2.5 will
raise exceptions. Python 2.6 and 2.7 will decode the ObjectId to
a garbage value. See the following links for an explanation and
possible future work-arounds:

http://bugs.python.org/issue6784

http://mail.python.org/pipermail/python-dev/2012-March/117536.html


Why do I get a syntax error importing pymongo after installing from source?
---------------------------------------------------------------------------

PyMongo makes use of the 2to3 tool to translate much of its code to valid
Python 3 syntax at install time. The translated modules are written to the
build subdirectory before being installed, leaving the original source files
intact. If you start the python interactive shell from the top level source
directory after running ``python setup.py install`` the untranslated modules
will be the first thing in your path. Importing pymongo will result in an
exception similar to::

  Python 3.1.4 (default, Mar 21 2012, 14:34:01)
  [GCC 4.5.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "pymongo/__init__.py", line 104, in <module>
      from pymongo.connection import Connection
    File "pymongo/connection.py", line 573
      except Exception, why:
                      ^
  SyntaxError: invalid syntax

Note the path in the traceback (``pymongo/__init__.py``). Changing out of the
source directory takes the untranslated modules out of your path::

  $ cd ..
  $ python
  Python 3.1.4 (default, Mar 21 2012, 14:34:01) 
  [GCC 4.5.3] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import pymongo
  >>> pymongo.__file__
  '/home/behackett/py3k/lib/python3.1/site-packages/pymongo-2.2-py3.1-linux-x86_64.egg/pymongo/__init__.py'


