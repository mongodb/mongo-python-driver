PyMongo |release| Documentation
===============================

Overview
--------
**PyMongo** is a Python distribution containing tools for working with
`MongoDB <http://www.mongodb.org>`_, and is the recommended way to
work with MongoDB from Python. This documentation attempts to explain
everything you need to know to use **PyMongo**.

.. todo:: a list of PyMongo's features

:doc:`installation`
  Instructions on how to get the distribution.

:doc:`tutorial`
  Start here for a quick overview.

:doc:`examples/index`
  Examples of how to perform specific tasks.

:doc:`faq`
  Some questions that come up often.

:doc:`python3`
  Frequently asked questions about python 3 support.

:doc:`api/index`
  The complete API documentation, organized by module.

:doc:`tools`
  A listing of Python tools and libraries that have been written for
  MongoDB.

:doc:`developer/index`
  Developer guide for contributors to PyMongo.

Getting Help
------------
If you're having trouble or have questions about PyMongo, the best place to ask is the `MongoDB user group <http://groups.google.com/group/mongodb-user>`_. Once you get an answer, it'd be great if you could work it back into this documentation and contribute!

Issues
------
All issues should be reported (and can be tracked / voted for /
commented on) at the main `MongoDB JIRA bug tracker
<http://jira.mongodb.org/browse/PYTHON>`_, in the "Python Driver"
project.

Contributing
------------
**PyMongo** has a large :doc:`community <contributors>` and
contributions are always encouraged. Contributions can be as simple as
minor tweaks to this documentation. To contribute, fork the project on
`github <http://github.com/mongodb/mongo-python-driver/>`_ and send a
pull request.

Changes
-------
See the :doc:`changelog` for a full list of changes to PyMongo.
For older versions of the documentation please see the
`archive list <http://api.mongodb.org/python/>`_.

About This Documentation
------------------------
This documentation is generated using the `Sphinx
<http://sphinx.pocoo.org/>`_ documentation generator. The source files
for the documentation are located in the *doc/* directory of the
**PyMongo** distribution. To generate the docs locally run the
following command from the root directory of the **PyMongo** source:

.. code-block:: bash

  $ python setup.py doc

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :hidden:

   installation
   tutorial
   examples/index
   faq
   api/index
   tools
   contributors
   changelog
   python3
   developer/index
