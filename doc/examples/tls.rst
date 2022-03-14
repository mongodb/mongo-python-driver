TLS/SSL and PyMongo
===================

PyMongo supports connecting to MongoDB over TLS/SSL. This guide covers the
configuration options supported by PyMongo. See `the server documentation
<http://docs.mongodb.org/manual/tutorial/configure-ssl/>`_ to configure
MongoDB.

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

Basic configuration
...................

In many cases connecting to MongoDB over TLS/SSL requires nothing more than
passing ``tls=True`` as a keyword argument to
:class:`~pymongo.mongo_client.MongoClient`::

  >>> client = pymongo.MongoClient('example.com', tls=True)

Or passing ``tls=true`` in the URI::

  >>> client = pymongo.MongoClient('mongodb://example.com/?tls=true')

This configures PyMongo to connect to the server using TLS, verify the server's
certificate and verify that the host you are attempting to connect to is listed
by that certificate.

Certificate verification policy
...............................

By default, PyMongo is configured to require a certificate from the server when
TLS is enabled. This is configurable using the ``tlsAllowInvalidCertificates``
option. To disable this requirement pass ``tlsAllowInvalidCertificates=True``
as a keyword parameter::

  >>> client = pymongo.MongoClient('example.com',
  ...                              tls=True,
  ...                              tlsAllowInvalidCertificates=True)

Or, in the URI::

  >>> uri = 'mongodb://example.com/?tls=true&tlsAllowInvalidCertificates=true'
  >>> client = pymongo.MongoClient(uri)

Specifying a CA file
....................

In some cases you may want to configure PyMongo to use a specific set of CA
certificates. This is most often the case when you are acting as your own
certificate authority rather than using server certificates signed by a well
known authority. The ``tlsCAFile`` option takes a path to a CA file. It can be
passed as a keyword argument::

  >>> client = pymongo.MongoClient('example.com',
  ...                              tls=True,
  ...                              tlsCAFile='/path/to/ca.pem')

Or, in the URI::

  >>> uri = 'mongodb://example.com/?tls=true&tlsCAFile=/path/to/ca.pem'
  >>> client = pymongo.MongoClient(uri)

Specifying a certificate revocation list
........................................

The ``tlsCRLFile`` option takes a path to a CRL file. It can be passed
as a keyword argument::

  >>> client = pymongo.MongoClient('example.com',
  ...                              tls=True,
  ...                              tlsCRLFile='/path/to/crl.pem')

Or, in the URI::

  >>> uri = 'mongodb://example.com/?tls=true&tlsCRLFile=/path/to/crl.pem'
  >>> client = pymongo.MongoClient(uri)

.. note:: Certificate revocation lists and :ref:`OCSP` cannot be used together.

Client certificates
...................

PyMongo can be configured to present a client certificate using the
``tlsCertificateKeyFile`` option::

  >>> client = pymongo.MongoClient('example.com',
  ...                              tls=True,
  ...                              tlsCertificateKeyFile='/path/to/client.pem')

If the private key for the client certificate is stored in a separate file,
it should be concatenated with the certificate file. For example, to
concatenate a PEM-formatted certificate file ``cert.pem`` and a PEM-formatted
keyfile ``key.pem`` into a single file ``combined.pem``, on Unix systems,
users can run::

  $ cat key.pem cert.pem > combined.pem

PyMongo can be configured with the concatenated certificate keyfile using the
``tlsCertificateKeyFile`` option::

  >>> client = pymongo.MongoClient('example.com',
  ...                              tls=True,
  ...                              tlsCertificateKeyFile='/path/to/combined.pem')

If the private key contained in the certificate keyfile is encrypted, users
can provide a password or passphrase to decrypt the encrypted private keys
using the ``tlsCertificateKeyFilePassword`` option::

  >>> client = pymongo.MongoClient('example.com',
  ...                              tls=True,
  ...                              tlsCertificateKeyFile='/path/to/combined.pem',
  ...                              tlsCertificateKeyFilePassword=<passphrase>)

These options can also be passed as part of the MongoDB URI.

.. _OCSP:

OCSP
....

Starting with PyMongo 3.11, if PyMongo was installed with the "ocsp" extra::

  python -m pip install pymongo[ocsp]

certificate revocation checking is enabled by way of `OCSP (Online Certification
Status Protocol) <https://en.wikipedia.org/wiki/Online_Certificate_Status_Protocol>`_.
MongoDB 4.4+ `staples OCSP responses <https://en.wikipedia.org/wiki/OCSP_stapling>`_
to the TLS handshake which PyMongo will verify, failing the TLS handshake if
the stapled OCSP response is invalid or indicates that the peer certificate is
revoked.

When connecting to a server version older than 4.4, or when a 4.4+ version of
MongoDB does not staple an OCSP response, PyMongo will attempt to connect
directly to an OCSP endpoint if the peer certificate specified one. The TLS
handshake will only fail in this case if the response indicates that the
certificate is revoked. Invalid or malformed responses will be ignored,
favoring availability over maximum security.


Troubleshooting TLS Errors
..........................

TLS errors often fall into three categories - certificate verification failure,
protocol version mismatch or certificate revocation checking failure. An error
message similar to the following means that OpenSSL was not able to verify the
server's certificate::

  [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed

This often occurs because OpenSSL does not have access to the system's
root certificates or the certificates are out of date. Linux users should
ensure that they have the latest root certificate updates installed from
their Linux vendor. macOS users using Python 3.6.0 or newer downloaded
from python.org `may have to run a script included with python
<https://bugs.python.org/issue29065#msg283984>`_ to install
root certificates::

  open "/Applications/Python <YOUR PYTHON VERSION>/Install Certificates.command"

Users of older PyPy portable versions may have to `set an environment
variable <https://github.com/squeaky-pl/portable-pypy/issues/15>`_ to tell
OpenSSL where to find root certificates. This is easily done using the `certifi
module <https://pypi.org/project/certifi/>`_ from pypi::

  $ pypy -m pip install certifi
  $ export SSL_CERT_FILE=$(pypy -c "import certifi; print(certifi.where())")

An error message similar to the following message means that the OpenSSL
version used by Python does not support a new enough TLS protocol to connect
to the server::

  [SSL: TLSV1_ALERT_PROTOCOL_VERSION] tlsv1 alert protocol version

Industry best practices recommend, and some regulations require, that older
TLS protocols be disabled in some MongoDB deployments. Some deployments may
disable TLS 1.0, others may disable TLS 1.0 and TLS 1.1. See the warning
earlier in this document for troubleshooting steps and solutions.

An error message similar to the following message means that certificate
revocation checking failed::

  [('SSL routines', 'tls_process_initial_server_flight', 'invalid status response')]

See :ref:`OCSP` for more details.

Python 3.10+ incompatibilities with TLS/SSL on MongoDB <= 4.0
.............................................................

Note that `changes made to the ssl module in Python 3.10+
<https://docs.python.org/3/whatsnew/3.10.html#ssl>`_ may cause incompatibilities
with MongoDB <= 4.0. The following are some example errors that may occur with this
combination::

  SSL handshake failed: localhost:27017: [SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:997)
  SSL handshake failed: localhost:27017: EOF occurred in violation of protocol (_ssl.c:997)

The MongoDB server logs may show the following error::

  2021-06-30T21:22:44.917+0100 E NETWORK  [conn16] SSL: error:1408A0C1:SSL routines:ssl3_get_client_hello:no shared cipher

To resolve this issue, use Python <=3.10, upgrade to MongoDB 4.2+, or install
pymongo with the :ref:`OCSP` extra which relies on PyOpenSSL.
