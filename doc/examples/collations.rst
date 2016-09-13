Collations
==========

.. seealso:: The API docs for :mod:`~pymongo.collation`.

Collations are a new feature in MongoDB version 3.4. They provide a set of rules
to use when comparing strings that comply with the conventions of a particular
language, such as Spanish or German. If no collation is specified, the server
sorts strings based on a binary comparison. Many languages have specific
ordering rules, and collations allow users to build applications that adhere to
language-specific comparison rules.

In French, for example, the last accent in a given word determines the sorting
order. The correct sorting order for the following four words in French is::

  cote < côte < coté < côté

Specifying a French collation allows users to sort string fields using the
French sort order.

Usage
-----

Users can specify a collation for a
:ref:`collection<collation-on-collection>`, an
:ref:`index<collation-on-index>`, or a
:ref:`CRUD command <collation-on-operation>`.

Collation Parameters:
~~~~~~~~~~~~~~~~~~~~~

Collations can be specified with the :class:`~pymongo.collation.Collation` model
or with plain Python dictionaries. The structure is the same::

   Collation(locale=<string>,
             caseLevel=<bool>,
             caseFirst=<string>,
             strength=<int>,
             numericOrdering=<bool>,
             alternate=<string>,
             maxVariable=<string>,
             backwards=<bool>)

The only required parameter is ``locale``, which the server parses as
an `ICU format locale ID <http://userguide.icu-project.org/locale>`_.
For example, set ``locale`` to ``en_US`` to represent US English
or ``fr_CA`` to represent Canadian French.

For a complete description of the available parameters, see the MongoDB `manual
</>`_.

.. COMMENT add link for manual entry.

.. _collation-on-collection:

Assign a Default Collation to a Collection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following example demonstrates how to create a new collection called
``contacts`` and assign a default collation with the ``fr_CA`` locale. This
operation ensures that all queries that are run against the ``contacts``
collection use the ``fr_CA`` collation unless another collation is explicitly
specified::

  from pymongo import MongoClient
  from pymongo.collation import Collation

  db = MongoClient().test
  collection = db.create_collection('contacts',
                                    collation=Collation(locale='fr_CA'))

.. _collation-on-index:

Assign a Default Collation to an Index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When creating a new index, you can specify a default collation.

The following example shows how to create an index on the ``name``
field of the ``contacts`` collection, with the ``unique`` parameter
enabled and a default collation with ``locale`` set to ``fr_CA``::

  from pymongo import MongoClient
  from pymongo.collation import Collation

  contacts = MongoClient().test.contacts
  contacts.create_index('name',
                        unique=True,
                        collation=Collation(locale='fr_CA'))

.. _collation-on-operation:

Specify a Collation for a Query
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Individual queries can specify a collation to use when sorting
results. The following example demonstrates a query that runs on the
``contacts`` collection in database ``test``. It matches on
documents that contain ``New York`` in the ``city`` field,
and sorts on the ``name`` field with the ``fr_CA`` collation::

  from pymongo import MongoClient
  from pymongo.collation import Collation

  collection = MongoClient().test.contacts
  docs = collection.find({'city': 'New York'}).sort('name').collation(
      Collation(locale='fr_CA'))

Other Query Types
~~~~~~~~~~~~~~~~~

You can use collations to control document matching rules for several different
types of queries. All the various update and delete methods
(:meth:`~pymongo.collection.Collection.update_one`,
:meth:`~pymongo.collection.Collection.update_many`,
:meth:`~pymongo.collection.Collection.delete_one`, etc.) support collation, and
you can create query filters which employ collations to comply with any of the
languages and variants available to the ``locale`` parameter.

The following example uses a collation with ``strength`` set to
:const:`~pymongo.collation.CollationStrength.SECONDARY`, which considers only
the base character and character accents in string comparisons, but not case
sensitivity, for example. All documents in the ``contacts`` collection with
``jürgen`` (case-insensitive) in the ``first_name`` field are updated::

  from pymongo import MongoClient
  from pymongo.collation import Collation, CollationStrength

  contacts = MongoClient().test.contacts
  result = contacts.update_many(
      {'first_name': 'jürgen'},
      {'$set': {'verified': 1}},
      collation=Collation(locale='de',
                          strength=CollationStrength.SECONDARY))
