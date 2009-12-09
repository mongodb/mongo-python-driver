PyMongo |release| Documentation
===============================

Overview
--------
**PyMongo** is a Python distribution containing tools for working with
`MongoDB <http://www.mongodb.org>`_, and is the recommended way to work with MongoDB from Python. This documentation attempts to explain everything you need to know to use **PyMongo**.

.. todo:: a list of PyMongo's features

:doc:`installation`
  Instructions on how to get the distribution.

:doc:`tutorial`
  Start here for a quick overview.

:doc:`examples/index`
  Examples of how to perform specific tasks.

:doc:`faq`
  Some questions that come up often.

:doc:`api/index`
  The complete API documentation, organized by module.

:doc:`tools`
  A listing of Python tools and libraries that have been written for MongoDB.

About This Documentation
------------------------
This documentation is generated using the `Sphinx
<http://sphinx.pocoo.org/>`_ documentation generator. The source files
for the documentation are located in the *doc/* directory of the
**PyMongo** distribution. To generate the docs locally run the
following command from the root directory of the **PyMongo** source:

.. code-block:: bash

  $ python setup.py doc

Contributing
------------
**PyMongo** has a large :doc:`community <contributors>` and
contributions are always encouraged. Contributions can be as simple as
minor tweaks to this documentation. To contribute, fork the project on
`github <http://github.com/mongodb/mongo-python-driver/>`_ and send a
pull request.

Changes in Version 1.2
----------------------
- `spec` parameter for :meth:`~pymongo.collection.Collection.remove` is
  now optional to allow for deleting all documents in a
  :class:`~pymongo.collection.Collection`
- always wrap queries with ``{query: ...}`` even when no special options -
  get around some issues with queries on fields named ``query``
- enforce 4MB document limit on the client side
- added :meth:`~pymongo.collection.Collection.map_reduce` helper - see
  :doc:`example <examples/map_reduce>`
- added :meth:`pymongo.cursor.Cursor.distinct` to allow distinct with
  queries
- fix for :meth:`pymongo.cursor.Cursor.__getitem__` after
  :meth:`~pymongo.cursor.Cursor.skip`
- allow any UTF-8 string in :class:`~pymongo.bson.BSON` encoder, not
  just ASCII subset
- added :attr:`pymongo.objectid.ObjectId.generation_time`
- removed support for legacy :class:`~pymongo.objectid.ObjectId`
  format - pretty sure this was never used, and is just confusing
- DEPRECATED :meth:`pymongo.objectid.ObjectId.url_encode` and
  :meth:`pymongo.objectid.ObjectId.url_decode` in favor of :meth:`str`
  and :meth:`pymongo.objectid.ObjectId`, respectively
- allow *oplog.$main* as a valid collection name
- some minor fixes for installation process
- added support for datetime and regex in :mod:`~pymongo.json_util`

Full Contents Tree
------------------

.. toctree::
   :maxdepth: 3

   installation
   tutorial
   examples/index
   faq
   api/index
   tools
   contributors

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

