.. _Client-Side Field Level Encryption:

Client-Side Field Level Encryption
==================================

New in MongoDB 4.2, client-side field level encryption allows an application
to encrypt specific data fields in addition to pre-existing MongoDB
encryption features such as `Encryption at Rest
<https://dochub.mongodb.org/core/security-encryption-at-rest>`_ and
`TLS/SSL (Transport Encryption)
<https://dochub.mongodb.org/core/security-tls-transport-encryption>`_.

With field level encryption, applications can encrypt fields in documents
*prior* to transmitting data over the wire to the server. Client-side field
level encryption supports workloads where applications must guarantee that
unauthorized parties, including server administrators, cannot read the
encrypted data.

.. seealso:: The MongoDB documentation on `Client Side Field Level Encryption <https://dochub.mongodb.org/core/client-side-field-level-encryption>`_.

Dependencies
------------

To get started using client-side field level encryption in your project,
you will need to install the
`pymongocrypt <https://pypi.org/project/pymongocrypt/>`_ library
as well as the driver itself. Install both the driver and a compatible
version of pymongocrypt like this::

  $ python -m pip install 'pymongo[encryption]'

Note that installing on Linux requires pip 19 or later for manylinux2010 wheel
support. For more information about installing pymongocrypt see
`the installation instructions on the project's PyPI page
<https://pypi.org/project/pymongocrypt/>`_.

mongocryptd
-----------

The ``mongocryptd`` binary is required for automatic client-side encryption
and is included as a component in the `MongoDB Enterprise Server package
<https://dochub.mongodb.org/core/install-mongodb-enterprise>`_.
For detailed installation instructions see
`the MongoDB documentation on mongocryptd
<https://dochub.mongodb.org/core/client-side-field-level-encryption-mongocryptd>`_.

``mongocryptd`` performs the following:

- Parses the automatic encryption rules specified to the database connection.
  If the JSON schema contains invalid automatic encryption syntax or any
  document validation syntax, ``mongocryptd`` returns an error.
- Uses the specified automatic encryption rules to mark fields in read and
  write operations for encryption.
- Rejects read/write operations that may return unexpected or incorrect results
  when applied to an encrypted field. For supported and unsupported operations,
  see `Read/Write Support with Automatic Field Level Encryption
  <https://dochub.mongodb.org/core/client-side-field-level-encryption-read-write-support>`_.

A MongoClient configured with auto encryption will automatically spawn the
``mongocryptd`` process from the application's ``PATH``. Applications can
control the spawning behavior as part of the automatic encryption options.
For example to set the path to the ``mongocryptd`` process::

  auto_encryption_opts = AutoEncryptionOpts(
          ...,
          mongocryptd_spawn_path='/path/to/mongocryptd')

To control the logging output of ``mongocryptd`` pass options using
``mongocryptd_spawn_args``::

  auto_encryption_opts = AutoEncryptionOpts(
          ...,
          mongocryptd_spawn_args=['--logpath=/path/to/mongocryptd.log', '--logappend'])

If your application wishes to manage the ``mongocryptd`` process manually,
it is possible to disable spawning ``mongocryptd``::

  auto_encryption_opts = AutoEncryptionOpts(
          ...,
          mongocryptd_bypass_spawn=True,
          # URI of the local ``mongocryptd`` process.
          mongocryptd_uri='mongodb://localhost:27020')

``mongocryptd`` is only responsible for supporting automatic client-side field
level encryption and does not itself perform any encryption or decryption.

.. _automatic-client-side-encryption:

Automatic Client-Side Field Level Encryption
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Automatic client-side field level encryption is enabled by creating a
:class:`~pymongo.mongo_client.MongoClient` with the ``auto_encryption_opts``
option set to an instance of
:class:`~pymongo.encryption_options.AutoEncryptionOpts`. The following
examples show how to setup automatic client-side field level encryption
using :class:`~pymongo.encryption.ClientEncryption` to create a new
encryption data key.

.. note:: Automatic client-side field level encryption requires MongoDB 4.2
   enterprise or a MongoDB 4.2 Atlas cluster. The community version of the
   server supports automatic decryption as well as
   :ref:`explicit-client-side-encryption`.

Providing Local Automatic Encryption Rules
``````````````````````````````````````````

The following example shows how to specify automatic encryption rules via the
``schema_map`` option. The automatic encryption rules are expressed using a
`strict subset of the JSON Schema syntax
<https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules>`_.

Supplying a ``schema_map`` provides more security than relying on
JSON Schemas obtained from the server. It protects against a
malicious server advertising a false JSON Schema, which could trick
the client into sending unencrypted data that should be encrypted.

JSON Schemas supplied in the ``schema_map`` only apply to configuring
automatic client-side field level encryption. Other validation
rules in the JSON schema will not be enforced by the driver and
will result in an error.::

  import os

  from bson.codec_options import CodecOptions
  from bson import json_util

  from pymongo import MongoClient
  from pymongo.encryption import (Algorithm,
                                  ClientEncryption)
  from pymongo.encryption_options import AutoEncryptionOpts


  def create_json_schema_file(kms_providers, key_vault_namespace,
                              key_vault_client):
      client_encryption = ClientEncryption(
          kms_providers,
          key_vault_namespace,
          key_vault_client,
          # The CodecOptions class used for encrypting and decrypting.
          # This should be the same CodecOptions instance you have configured
          # on MongoClient, Database, or Collection. We will not be calling
          # encrypt() or decrypt() in this example so we can use any
          # CodecOptions.
          CodecOptions())

      # Create a new data key and json schema for the encryptedField.
      # https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules
      data_key_id = client_encryption.create_data_key(
          'local', key_alt_names=['pymongo_encryption_example_1'])
      schema = {
          "properties": {
              "encryptedField": {
                  "encrypt": {
                      "keyId": [data_key_id],
                      "bsonType": "string",
                      "algorithm":
                          Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
                  }
              }
          },
          "bsonType": "object"
      }
      # Use CANONICAL_JSON_OPTIONS so that other drivers and tools will be
      # able to parse the MongoDB extended JSON file.
      json_schema_string = json_util.dumps(
          schema, json_options=json_util.CANONICAL_JSON_OPTIONS)

      with open('jsonSchema.json', 'w') as file:
          file.write(json_schema_string)


  def main():
      # The MongoDB namespace (db.collection) used to store the
      # encrypted documents in this example.
      encrypted_namespace = "test.coll"

      # This must be the same master key that was used to create
      # the encryption key.
      local_master_key = os.urandom(96)
      kms_providers = {"local": {"key": local_master_key}}

      # The MongoDB namespace (db.collection) used to store
      # the encryption data keys.
      key_vault_namespace = "encryption.__pymongoTestKeyVault"
      key_vault_db_name, key_vault_coll_name = key_vault_namespace.split(".", 1)

      # The MongoClient used to access the key vault (key_vault_namespace).
      key_vault_client = MongoClient()
      key_vault = key_vault_client[key_vault_db_name][key_vault_coll_name]
      # Ensure that two data keys cannot share the same keyAltName.
      key_vault.drop()
      key_vault.create_index(
          "keyAltNames",
          unique=True,
          partialFilterExpression={"keyAltNames": {"$exists": True}})

      create_json_schema_file(
          kms_providers, key_vault_namespace, key_vault_client)

      # Load the JSON Schema and construct the local schema_map option.
      with open('jsonSchema.json', 'r') as file:
          json_schema_string = file.read()
      json_schema = json_util.loads(json_schema_string)
      schema_map = {encrypted_namespace: json_schema}

      auto_encryption_opts = AutoEncryptionOpts(
          kms_providers, key_vault_namespace, schema_map=schema_map)

      client = MongoClient(auto_encryption_opts=auto_encryption_opts)
      db_name, coll_name = encrypted_namespace.split(".", 1)
      coll = client[db_name][coll_name]
      # Clear old data
      coll.drop()

      coll.insert_one({"encryptedField": "123456789"})
      print('Decrypted document: %s' % (coll.find_one(),))
      unencrypted_coll = MongoClient()[db_name][coll_name]
      print('Encrypted document: %s' % (unencrypted_coll.find_one(),))


  if __name__ == "__main__":
      main()

Server-Side Field Level Encryption Enforcement
``````````````````````````````````````````````

The MongoDB 4.2 server supports using schema validation to enforce encryption
of specific fields in a collection. This schema validation will prevent an
application from inserting unencrypted values for any fields marked with the
``"encrypt"`` JSON schema keyword.

The following example shows how to setup automatic client-side field level
encryption using
:class:`~pymongo.encryption.ClientEncryption` to create a new encryption
data key and create a collection with the
`Automatic Encryption JSON Schema Syntax
<https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules>`_::

  import os

  from bson.codec_options import CodecOptions
  from bson.binary import STANDARD

  from pymongo import MongoClient
  from pymongo.encryption import (Algorithm,
                                  ClientEncryption)
  from pymongo.encryption_options import AutoEncryptionOpts
  from pymongo.errors import OperationFailure
  from pymongo.write_concern import WriteConcern


  def main():
      # The MongoDB namespace (db.collection) used to store the
      # encrypted documents in this example.
      encrypted_namespace = "test.coll"

      # This must be the same master key that was used to create
      # the encryption key.
      local_master_key = os.urandom(96)
      kms_providers = {"local": {"key": local_master_key}}

      # The MongoDB namespace (db.collection) used to store
      # the encryption data keys.
      key_vault_namespace = "encryption.__pymongoTestKeyVault"
      key_vault_db_name, key_vault_coll_name = key_vault_namespace.split(".", 1)

      # The MongoClient used to access the key vault (key_vault_namespace).
      key_vault_client = MongoClient()
      key_vault = key_vault_client[key_vault_db_name][key_vault_coll_name]
      # Ensure that two data keys cannot share the same keyAltName.
      key_vault.drop()
      key_vault.create_index(
          "keyAltNames",
          unique=True,
          partialFilterExpression={"keyAltNames": {"$exists": True}})

      client_encryption = ClientEncryption(
          kms_providers,
          key_vault_namespace,
          key_vault_client,
          # The CodecOptions class used for encrypting and decrypting.
          # This should be the same CodecOptions instance you have configured
          # on MongoClient, Database, or Collection. We will not be calling
          # encrypt() or decrypt() in this example so we can use any
          # CodecOptions.
          CodecOptions())

      # Create a new data key and json schema for the encryptedField.
      data_key_id = client_encryption.create_data_key(
          'local', key_alt_names=['pymongo_encryption_example_2'])
      json_schema = {
          "properties": {
              "encryptedField": {
                  "encrypt": {
                      "keyId": [data_key_id],
                      "bsonType": "string",
                      "algorithm":
                          Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
                  }
              }
          },
          "bsonType": "object"
      }

      auto_encryption_opts = AutoEncryptionOpts(
          kms_providers, key_vault_namespace)
      client = MongoClient(auto_encryption_opts=auto_encryption_opts)
      db_name, coll_name = encrypted_namespace.split(".", 1)
      db = client[db_name]
      # Clear old data
      db.drop_collection(coll_name)
      # Create the collection with the encryption JSON Schema.
      db.create_collection(
          coll_name,
          # uuid_representation=STANDARD is required to ensure that any
          # UUIDs in the $jsonSchema document are encoded to BSON Binary
          # with the standard UUID subtype 4. This is only needed when
          # running the "create" collection command with an encryption
          # JSON Schema.
          codec_options=CodecOptions(uuid_representation=STANDARD),
          write_concern=WriteConcern(w="majority"),
          validator={"$jsonSchema": json_schema})
      coll = client[db_name][coll_name]

      coll.insert_one({"encryptedField": "123456789"})
      print('Decrypted document: %s' % (coll.find_one(),))
      unencrypted_coll = MongoClient()[db_name][coll_name]
      print('Encrypted document: %s' % (unencrypted_coll.find_one(),))
      try:
          unencrypted_coll.insert_one({"encryptedField": "123456789"})
      except OperationFailure as exc:
          print('Unencrypted insert failed: %s' % (exc.details,))


  if __name__ == "__main__":
      main()

.. _explicit-client-side-encryption:

Explicit Encryption
~~~~~~~~~~~~~~~~~~~

Explicit encryption is a MongoDB community feature and does not use the
``mongocryptd`` process. Explicit encryption is provided by the
:class:`~pymongo.encryption.ClientEncryption` class, for example::

  import os

  from pymongo import MongoClient
  from pymongo.encryption import (Algorithm,
                                  ClientEncryption)


  def main():
      # This must be the same master key that was used to create
      # the encryption key.
      local_master_key = os.urandom(96)
      kms_providers = {"local": {"key": local_master_key}}

      # The MongoDB namespace (db.collection) used to store
      # the encryption data keys.
      key_vault_namespace = "encryption.__pymongoTestKeyVault"
      key_vault_db_name, key_vault_coll_name = key_vault_namespace.split(".", 1)

      # The MongoClient used to read/write application data.
      client = MongoClient()
      coll = client.test.coll
      # Clear old data
      coll.drop()

      # Set up the key vault (key_vault_namespace) for this example.
      key_vault = client[key_vault_db_name][key_vault_coll_name]
      # Ensure that two data keys cannot share the same keyAltName.
      key_vault.drop()
      key_vault.create_index(
          "keyAltNames",
          unique=True,
          partialFilterExpression={"keyAltNames": {"$exists": True}})

      client_encryption = ClientEncryption(
          kms_providers,
          key_vault_namespace,
          # The MongoClient to use for reading/writing to the key vault.
          # This can be the same MongoClient used by the main application.
          client,
          # The CodecOptions class used for encrypting and decrypting.
          # This should be the same CodecOptions instance you have configured
          # on MongoClient, Database, or Collection.
          coll.codec_options)

      # Create a new data key for the encryptedField.
      data_key_id = client_encryption.create_data_key(
          'local', key_alt_names=['pymongo_encryption_example_3'])

      # Explicitly encrypt a field:
      encrypted_field = client_encryption.encrypt(
          "123456789",
          Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
          key_id=data_key_id)
      coll.insert_one({"encryptedField": encrypted_field})
      doc = coll.find_one()
      print('Encrypted document: %s' % (doc,))

      # Explicitly decrypt the field:
      doc["encryptedField"] = client_encryption.decrypt(doc["encryptedField"])
      print('Decrypted document: %s' % (doc,))

      # Cleanup resources.
      client_encryption.close()
      client.close()


  if __name__ == "__main__":
      main()


Explicit Encryption with Automatic Decryption
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Although automatic encryption requires MongoDB 4.2 enterprise or a
MongoDB 4.2 Atlas cluster, automatic *decryption* is supported for all users.
To configure automatic *decryption* without automatic *encryption* set
``bypass_auto_encryption=True`` in
:class:`~pymongo.encryption_options.AutoEncryptionOpts`::

  import os

  from pymongo import MongoClient
  from pymongo.encryption import (Algorithm,
                                  ClientEncryption)
  from pymongo.encryption_options import AutoEncryptionOpts


  def main():
      # This must be the same master key that was used to create
      # the encryption key.
      local_master_key = os.urandom(96)
      kms_providers = {"local": {"key": local_master_key}}

      # The MongoDB namespace (db.collection) used to store
      # the encryption data keys.
      key_vault_namespace = "encryption.__pymongoTestKeyVault"
      key_vault_db_name, key_vault_coll_name = key_vault_namespace.split(".", 1)

      # bypass_auto_encryption=True disable automatic encryption but keeps
      # the automatic _decryption_ behavior. bypass_auto_encryption will
      # also disable spawning mongocryptd.
      auto_encryption_opts = AutoEncryptionOpts(
          kms_providers, key_vault_namespace, bypass_auto_encryption=True)

      client = MongoClient(auto_encryption_opts=auto_encryption_opts)
      coll = client.test.coll
      # Clear old data
      coll.drop()

      # Set up the key vault (key_vault_namespace) for this example.
      key_vault = client[key_vault_db_name][key_vault_coll_name]
      # Ensure that two data keys cannot share the same keyAltName.
      key_vault.drop()
      key_vault.create_index(
          "keyAltNames",
          unique=True,
          partialFilterExpression={"keyAltNames": {"$exists": True}})

      client_encryption = ClientEncryption(
          kms_providers,
          key_vault_namespace,
          # The MongoClient to use for reading/writing to the key vault.
          # This can be the same MongoClient used by the main application.
          client,
          # The CodecOptions class used for encrypting and decrypting.
          # This should be the same CodecOptions instance you have configured
          # on MongoClient, Database, or Collection.
          coll.codec_options)

      # Create a new data key for the encryptedField.
      data_key_id = client_encryption.create_data_key(
          'local', key_alt_names=['pymongo_encryption_example_4'])

      # Explicitly encrypt a field:
      encrypted_field = client_encryption.encrypt(
          "123456789",
          Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
          key_alt_name='pymongo_encryption_example_4')
      coll.insert_one({"encryptedField": encrypted_field})
      # Automatically decrypts any encrypted fields.
      doc = coll.find_one()
      print('Decrypted document: %s' % (doc,))
      unencrypted_coll = MongoClient().test.coll
      print('Encrypted document: %s' % (unencrypted_coll.find_one(),))

      # Cleanup resources.
      client_encryption.close()
      client.close()


  if __name__ == "__main__":
      main()
