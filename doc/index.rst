PyMongo |release| Documentation
===============================

.. note:: The PyMongo documentation has been migrated to the
   `MongoDB Documentation site <https://www.mongodb.com/docs/languages/python/pymongo-driver/current>`_.
   As of PyMongo 4.10, the ReadTheDocs site will contain the detailed changelog and API docs, while the
   rest of the documentation will only appear on the MongoDB Documentation site.

Overview
--------
**PyMongo** is a Python distribution containing tools for working with
`MongoDB <https://www.mongodb.org>`_, and is the recommended way to
work with MongoDB from Python. This documentation attempts to explain
everything you need to know to use **PyMongo**.

.. todo:: a list of PyMongo's features

:doc:`installation`
  Instructions on how to get the distribution.

:doc:`tutorial`
  Start here for a quick overview.

:doc:`async-tutorial`
  Start here for a quick overview of the asynchronous API.

:doc:`examples/index`
  Examples of how to perform specific tasks.

:doc:`atlas`
  Using PyMongo with MongoDB Atlas.

:doc:`examples/tls`
  Using PyMongo with TLS / SSL.

:doc:`examples/encryption`
  Using PyMongo with In-Use Encryption.

:doc:`examples/type_hints`
  Using PyMongo with type hints.

:doc:`examples/logging`
  Using PyMongo's logging capabilities.

:doc:`faq`
  Some questions that come up often.

:doc:`migrate-to-pymongo4`
  A PyMongo 3.x to 4.x migration guide.

:doc:`python3`
  Frequently asked questions about python 3 support.

:doc:`compatibility-policy`
  Explanation of deprecations, and how to keep pace with changes in PyMongo's
  API.

:doc:`api/index`
  The complete API documentation, organized by module.

:doc:`tools`
  A listing of Python tools and libraries that have been written for
  MongoDB.

:doc:`developer/index`
  Developer guide for contributors to PyMongo.

:doc:`common-issues`
  Common issues encountered when using PyMongo.

Getting Help
------------
If you're having trouble or have questions about PyMongo, ask your question on
our `MongoDB Community Forum <https://www.mongodb.com/community/forums/tag/python>`_.
You may also want to consider a
`commercial support subscription <https://support.mongodb.com/welcome>`_.
Once you get an answer, it'd be great if you could work it back into this
documentation and contribute!

Issues
------
All issues should be reported (and can be tracked / voted for /
commented on) at the main `MongoDB JIRA bug tracker
<https://jira.mongodb.org/browse/PYTHON>`_, in the "Python Driver"
project.

Feature Requests / Feedback
---------------------------
Use our `feedback engine <https://feedback.mongodb.com/forums/924286-drivers>`_
to send us feature requests and general feedback about PyMongo.

Contributing
------------
**PyMongo** has a large :doc:`community <contributors>` and
contributions are always encouraged. Contributions can be as simple as
minor tweaks to this documentation. To contribute, fork the project on
`GitHub <https://github.com/mongodb/mongo-python-driver/>`_ and send a
pull request.

Changes
-------
See the :doc:`changelog` for a full list of changes to PyMongo.

About This Documentation
------------------------
This documentation is generated using the `Sphinx
<https://www.sphinx-doc.org/en/master/>`_ documentation generator. The source files
for the documentation are located in the *doc/* directory of the
**PyMongo** distribution. See the PyMongo `contributing guide  <https://github.com/mongodb/mongo-python-driver/blob/master/CONTRIBUTING.md>`_
for instructions on the building the docs from source.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :hidden:

   atlas
   installation
   tutorial
   async-tutorial
   examples/index
   faq
   compatibility-policy
   api/index
   tools
   contributors
   changelog
   python3
   migrate-to-pymongo4
   developer/index
   common-issues
