Using PyMongo with MongoDB Atlas
================================

`Atlas <https://www.mongodb.com/cloud>`_ is MongoDB, Inc.'s hosted MongoDB as a
service offering. To connect to Atlas, pass the connection string provided by
Atlas to :class:`~pymongo.mongo_client.MongoClient`::

  client = pymongo.MongoClient(<Atlas connection string>)

Connections to Atlas require TLS/SSL.

.. warning:: Industry best practices recommend, and some regulations require,
  the use of TLS 1.1 or newer. Though no application changes are required for
  PyMongo to make use of the newest protocols, some operating systems or
  versions may not provide an OpenSSL version new enough to support them.

  Users of macOS older than 10.13 (High Sierra) will need to install Python
  from `python.org`_, `homebrew`_, `macports`_, or another similar source.

  Users of Linux or other non-macOS Unix can check their OpenSSL version like
  this::

    $ openssl version

  If the version number is less than 1.0.1 support for TLS 1.1 or newer is not
  available. Contact your operating system vendor for a solution or upgrade to
  a newer distribution.

  You can check your Python interpreter by installing the `requests`_ module
  and executing the following command::

    python -c "import requests; print(requests.get('https://www.howsmyssl.com/a/check', verify=False).json()['tls_version'])"

  You should see "TLS 1.X" where X is >= 1.

  You can read more about TLS versions and their security implications here:

  `<https://cheatsheetseries.owasp.org/cheatsheets/Transport_Layer_Protection_Cheat_Sheet.html#only-support-strong-protocols>`_

.. _python.org: https://www.python.org/downloads/
.. _homebrew: https://brew.sh/
.. _macports: https://www.macports.org/
.. _requests: https://pypi.python.org/pypi/requests
