Tutorial
========
Fuente: https://github.com/mongodb/mongo-python-driver/blob/master/doc/tutorial.rst

.. Pasos de instalación::

  from pymongo import MongoClient
  client = MongoClient()
  client.drop_database('test-database')

Este tutorial tiene la intención de ser una introducción para el trabajo con **MongoDB** y **PyMongo**.

Requerimientos
--------------
Antes de iniciar, Asegúrese de que cuenta con la distribución de **PyMongo** :doc:`installed <installation>`. En el Python shell, el siguiente comando debería ejecutarse y no tener ninguna excepción:

.. doctest::

  >>> import pymongo

Este tutorial también asume que una instancia de MongoDB esta corriendo en localhost y en el puerto por defecto. Se asume que tiene `downloaded and installed
<http://www.mongodb.org/display/DOCS/Getting+Started>`_ MongoDB, se puede inciar asi:

.. code-block:: bash

  $ mongod

Haciendo una conexión con MongoClient
-------------------------------------
El primer paso cuando trabaja con **PyMongo** es crear una :class:`~pymongo.mongo_client.MongoClient` para correr la instacia **mongod**. Se hace muy facil:

.. doctest::

  >>> from pymongo import MongoClient
  >>> client = MongoClient()

El código anterior se conectara al host y al puerto por defecto. Se puede especificar el host y un puerto específico, como se muestra en la siguiente línea de código:

.. doctest::

  >>> client = MongoClient('localhost', 27017)

O usando el formato MongoDB URI:

.. doctest::

  >>> client = MongoClient('mongodb://localhost:27017/')

Trabajando con una base de datos
--------------------------------
Una simple instancia de MongoDB puede soportar múltiples `bases de datos <http://www.mongodb.org/display/DOCS/Databases>` independientes. cuando trabaja con PyMongo el acceso a bases de datos usa el acceso de estilo de atributo en :class:`~pymongo.mongo_client.MongoClient` instancias:

.. doctest::

  >>> db = client.test_database

Si el nombre de su base de datos es igual que el acceso al estilo de atributo no funciona (como ``test-database``), puede usar el acceso al estilo del diccionario en su lugar:

.. doctest::

  >>> db = client['test-database']

trabajando con una Collection
-----------------------------
una `collection <http://www.mongodb.org/display/DOCS/Collections>`_ es un grupo de documentos almacenados en MongoDB, y se puede pensar que es equivalente a una tabla en una base de datos relacional. Tener una Colecciones en PyMongo funciona igual que tener una base de datos:

.. doctest::

  >>> collection = db.test_collection

O (usando el acceso al estilo del diccionario):

.. doctest::

  >>> collection = db['test-collection']

Algo importante acerca de las colecciones (y bases de datos) en MongoDB es que son creadas de manera perezosa: los anteriores comando no han realizado ninguna operación en el servidor de MongoDB. Colecciones y bases de datos se crean cuando se guarda el primer documento.

Documentos
----------
Los datos en MongoDB estan representados (y almacenados) mediante documentos en formato JSON. En PyMongo se usan diccionarios para representar los documentos. por ejemplo, el siguiente diccionario podría usarse para representar una publicación de un blog:

.. doctest::

  >>> import datetime
  >>> post = {"author": "Mike",
  ...         "text": "My first blog post!",
  ...         "tags": ["mongodb", "python", "pymongo"],
  ...         "date": datetime.datetime.utcnow()}

Tenga en cuenta que los documentos pueden contener tipos nativos de Python (como :class: instancias `datetime.datetime`) las cuales se convertirán automáticamente hacia y desde los tipos `BSON <http://www.mongodb.org/display/DOCS/BSON>`_apropiados.
.

.. todo:: enlace a la table de Python <-> tipo BSON 

Incersion de un documento
-------------------------
Para insertar un documento en una collection puede usar el método :meth:`~pymongo.collection.Collection.insert_one`:

.. doctest::

  >>> posts = db.posts
  >>> post_id = posts.insert_one(post).inserted_id
  >>> post_id
  ObjectId('...')

Cuando se inserta un documento, se genera automáticamente una llave, ``"_id"``,si el documento no tiene una llave ``"_id"``. el valor de la llave ``"_id"`` debe ser único en toda la colección. :meth:`~pymongo.collection.Collection.insert_one` retorna una instancia de:class:`~pymongo.results.InsertOneResult`. para tener más información sobre ``"_id"``, consultar `documentación sobre el _id
<http://www.mongodb.org/display/DOCS/Object+IDs>`_.

Después de insertar el primer documento, la colección se creará automáticamente en el servidor. Se puede verificar si se lista toda la colección:

.. doctest::

  >>> db.list_collection_names()
  [u'posts']

Obtener un solo documento con :meth:`~pymongo.collection.Collection.find_one`
------------------------------------------------------------------------------
El tipo más básico de consulta que se puede realizar en MongoDB es:meth:`~pymongo.collection.Collection.find_one`. Este método retorna un solo documento que coincide con una consulta (o nada si no hay coincidencias).. Es útil cuando sabe que solo hay un documento coincidente o solo está nos interesa la primera coincidencia. usar :meth:`~pymongo.collection.Collection.find_one` para obtener el primer documento de la colección posts:

.. doctest::

  >>> import pprint
  >>> pprint.pprint(posts.find_one())
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'mongodb', u'python', u'pymongo'],
   u'text': u'My first blog post!'}

El resultado es un diccionario que coincide con el insertado anteriormente..

.. Nota:: El documento devuelto contiene un ``"_id"``, que se agregó automáticamente al insertar.

:meth:`~pymongo.collection.Collection.find_one` también soporta consultas sobre elementos específicos y el documento resultante debe coincidir. Para limitar nuestros resultados a un documento con el autor "Mike" hacer:

.. doctest::

  >>> pprint.pprint(posts.find_one({"author": "Mike"}))
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'mongodb', u'python', u'pymongo'],
   u'text': u'My first blog post!'}

Si se prueba con un autor diferente, como "Eliot", no se obtendrá ningún resultado:

.. doctest::

  >>> posts.find_one({"author": "Eliot"})
  >>>

.. _querying-by-objectid:

Consultando por ObjectId
------------------------
También se puede encontrar un registro en la colección posts por su ``_id``,  que en este ejemplo es el ObjectId:

.. doctest::

  >>> post_id
  ObjectId(...)
  >>> pprint.pprint(posts.find_one({"_id": post_id}))
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'mongodb', u'python', u'pymongo'],
   u'text': u'My first blog post!'}

Tenga en cuenta que un ObjectId no es lo mismo que su representación de cadena:

.. doctest::

  >>> post_id_as_str = str(post_id)
  >>> posts.find_one({"_id": post_id_as_str}) # Ningún resultado
  >>>

Una tarea común en las aplicaciones web es obtener un ObjectId desde una solicitud URL y encontrar el documento coincidente. En este caso, es necesario **convertir el ObjectId de una cadena** antes de pasarlo a
``find_one``::

  from bson.objectid import ObjectId

  # El framework web obtiene post_id de la URL y lo pasa como una cadena
  def get(post_id):
      # Convert from string to ObjectId:
      document = client.db.collection.find_one({'_id': ObjectId(post_id)})

.. ver también:: :ref:`web-application-querying-by-objectid`

Una nota sobre cadenas Unicode
------------------------------
Probablemente haya notado que las cadenas de Python normales que almacenamos anteriormente se ven diferentes cuando se recuperan desde el servidor(por ejemplo. u'Mike' en lugar de 'Mike').
Una breve explicación está bien.

MongoDB almacena datos en formato `BSON <http://bsonspec.org>`_. Las cadenas BSON están codificadas en UTF-8 por lo que PyMongo debe asegurarse de que cualquier cadena que almacena contenga solo datos UTF-8 válidos. Las cadenas regulares (<type 'str'>) se validan y almacenan inalteradas. Las cadenas Unicode (<type 'unicode'>) se codifican primero en UTF-8. La razón por la que la cadena de ejemplo se representa en el Python shell como u'Mike' en lugar de 'Mike' es porque PyMongo decodifica cada cadena BSON en una cadena Unicode de Python, no en una cadena normal.

`Leer más sobre las cadenas Unicode de Python aquí.
<http://docs.python.org/howto/unicode.html>`_.

Inserciones masivas
-------------------
Para hacer las consultas un poco más interesantes, insertar algunos documentos más. Además de insertar un solo documento, también se puede realizar operaciones de inserción masiva, pasando una lista como primer argumento a :meth:`~pymongo.collection.Collection.insert_many`. Esto insertará cada documento en la lista, enviando solo un comando al servidor:

.. doctest::

  >>> new_posts = [{"author": "Mike",
  ...               "text": "Another post!",
  ...               "tags": ["bulk", "insert"],
  ...               "date": datetime.datetime(2009, 11, 12, 11, 14)},
  ...              {"author": "Eliot",
  ...               "title": "MongoDB is fun",
  ...               "text": "and pretty easy too!",
  ...               "date": datetime.datetime(2009, 11, 10, 10, 45)}]
  >>> result = posts.insert_many(new_posts)
  >>> result.inserted_ids
  [ObjectId('...'), ObjectId('...')]

Hay un par de cosas interesantes a tener en cuenta sobre este ejemplo:

  - El resultado de :meth:`~pymongo.collection.Collection.insert_many` ahora retorna dos :class:`~bson.objectid.ObjectId` instancias, una para cada documento insertado.
  - ``new_posts[1]`` tiene una "forma" diferente a las otros registros de la colección posts; no hay ningún campo ``"tags"`` y se agrego uno nuevo,``"title"``. Esto es lo que se quiere decir cuando se dice que MongoDB es *schema-free*.

Consultar más de un documento
-----------------------------
Para obtener más de un documento como resultado de una consulta usar el metodo :meth:`~pymongo.collection.Collection.find` method. :meth:`~pymongo.collection.Collection.find` retornara una instancia :class:`~pymongo.cursor.Cursor`, que permite iterar sobre todos los documentos coincidentes. por ejemplo, es posible iterar sobre cada documento de la colección ``posts``:

.. doctest::

  >>> for post in posts.find():
  ...   pprint.pprint(post)
  ...
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'mongodb', u'python', u'pymongo'],
   u'text': u'My first blog post!'}
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'bulk', u'insert'],
   u'text': u'Another post!'}
  {u'_id': ObjectId('...'),
   u'author': u'Eliot',
   u'date': datetime.datetime(...),
   u'text': u'and pretty easy too!',
   u'title': u'MongoDB is fun'}

Al igual como se hizo con :meth:`~pymongo.collection.Collection.find_one`, es posible pasar un documento a :meth:`~pymongo.collection.Collection.find` para limitar los resultados devueltos. Aquí, obtenemos solo aquellos documentos cuyo autor es "Mike":

.. doctest::

  >>> for post in posts.find({"author": "Mike"}):
  ...   pprint.pprint(post)
  ...
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'mongodb', u'python', u'pymongo'],
   u'text': u'My first blog post!'}
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'bulk', u'insert'],
   u'text': u'Another post!'}

Contando
--------
Si solo queremos saber cuántos documentos coinciden con una consulta,realizar una operación :meth:`~pymongo.collection.Collection.count_documents` en lugar de una consulta completa. Es posible obtener un recuento de todos los documentos de una colección:

.. doctest::

  >>> posts.count_documents({})
  3

O solo de aquellos documentos que coinciden con una consulta específica:

.. doctest::

  >>> posts.count_documents({"author": "Mike"})
  2

Consultas de rango
------------------
MongoDB admite muchos tipos diferentes de `consultas avanzadas <http://www.mongodb.org/display/DOCS/Advanced+Queries>`_. Por ejemplo, realicemos una consulta en la que limitamos los resultados de la colección posts anteriores a una fecha determinada, pero también clasificamos los resultados por autor:

.. doctest::

  >>> d = datetime.datetime(2009, 11, 12, 12)
  >>> for post in posts.find({"date": {"$lt": d}}).sort("author"):
  ...   pprint.pprint(post)
  ...
  {u'_id': ObjectId('...'),
   u'author': u'Eliot',
   u'date': datetime.datetime(...),
   u'text': u'and pretty easy too!',
   u'title': u'MongoDB is fun'}
  {u'_id': ObjectId('...'),
   u'author': u'Mike',
   u'date': datetime.datetime(...),
   u'tags': [u'bulk', u'insert'],
   u'text': u'Another post!'}

Aquí el ``"$lt"`` operador especial para hacer una consulta de rango, y se llama a :meth:`~pymongo.cursor.Cursor.sort` para ordenar los resultados por autor.

Indexación
----------
Agregar índices puede ayudar a acelerar ciertas consultas y también puede agregar funcionalidad adicional para consultar y almacenar documentos. En este ejemplo, se puede ver cómo crear un `índice único <http://docs.mongodb.org/manual/core/index-unique/>`_en una llave que rechaza documentos cuyo valor para esa llave ya existe en el índice

Primero, crear el índice:

.. doctest::

   >>> result = db.profiles.create_index([('user_id', pymongo.ASCENDING)],
   ...                                   unique=True)
   >>> sorted(list(db.profiles.index_information()))
   [u'_id_', u'user_id_1']

Observe que ahora tenemos dos índices: uno es el índice ``_id`` que MongoDB crea automáticamente, y el otro es el índice ``user_id`` que se acabo de crear.

Ahora configurar algunos perfiles de usuario:

.. doctest::

   >>> user_profiles = [
   ...     {'user_id': 211, 'name': 'Luke'},
   ...     {'user_id': 212, 'name': 'Ziltoid'}]
   >>> result = db.profiles.insert_many(user_profiles)

El índice impide insertar un documento que ``user_id`` ya está en la colección:

.. doctest::
   :options: +IGNORE_EXCEPTION_DETAIL

   >>> new_profile = {'user_id': 213, 'name': 'Drew'}
   >>> duplicate_profile = {'user_id': 212, 'name': 'Tommy'}
   >>> result = db.profiles.insert_one(new_profile)  # This is fine.
   >>> result = db.profiles.insert_one(duplicate_profile)
   Traceback (most recent call last):
   DuplicateKeyError: E11000 duplicate key error index: test_database.profiles.$user_id_1 dup key: { : 212 }

.. Ver también:: La documentación de MongoDB sobre `indexes <http://www.mongodb.org/display/DOCS/Indexes>`_
