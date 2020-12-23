# Copyright 2020-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Support for MongoDB Versioned API.

.. _versioned-api-ref:

MongoDB Versioned API
=====================

To configure MongoDB Versioned API, pass the ``server_api`` keyword option to
:class:`~pymongo.mongo_client.MongoClient`::

    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi

    client = MongoClient(server_api=ServerApi('1'))

Note that Versioned API requires MongoDB >=5.0.

Strict Mode
```````````

When ``strict`` mode is configured, commands that are not supported in the
given :attr:`ServerApi.version` will fail. For example::

    >>> client = MongoClient(server_api=ServerApi('1', strict=True))
    >>> client.test.command('count', 'test')
    Traceback (most recent call last):
    ...
    pymongo.errors.OperationFailure: Provided apiStrict:true, but the command count is not in API Version 1, full error: {'ok': 0.0, 'errmsg': 'Provided apiStrict:true, but the command count is not in API Version 1', 'code': 323, 'codeName': 'APIStrictError'

Classes
=======
"""


class ServerApiVersion:
    """An enum that defines values for :attr:`ServerApi.version`.

    .. versionadded:: 3.12
    """

    V1 = "1"
    """Server API version "1"."""


class ServerApi(object):
    """MongoDB Versioned API."""
    def __init__(self, version, strict=None, deprecation_errors=None):
        """Options to configure MongoDB Versioned API.

        :Parameters:
          - `version`: The API version string. Must be one of the values in
            :class:`ServerApiVersion`.
          - `strict` (optional): Set to ``True`` to enable API strict mode.
            Defaults to ``None`` which means "use the server's default".
          - `deprecation_errors` (optional): Set to ``True`` to enable
            deprecation errors. Defaults to ``None`` which means "use the
            server's default".

        .. versionadded:: 3.12
        """
        if version != ServerApiVersion.V1:
            raise ValueError("Unknown ServerApi version: %s" % (version,))
        if strict is not None and not isinstance(strict, bool):
            raise TypeError(
                "Wrong type for ServerApi strict, value must be an instance "
                "of bool, not %s" % (type(strict),))
        if (deprecation_errors is not None and
                not isinstance(deprecation_errors, bool)):
            raise TypeError(
                "Wrong type for ServerApi deprecation_errors, value must be "
                "an instance of bool, not %s" % (type(deprecation_errors),))
        self._version = version
        self._strict = strict
        self._deprecation_errors = deprecation_errors

    @property
    def version(self):
        """The API version setting.

        This value is sent to the server in the "apiVersion" field.
        """
        return self._version

    @property
    def strict(self):
        """The API strict mode setting.

        When set, this value is sent to the server in the "apiStrict" field.
        """
        return self._strict

    @property
    def deprecation_errors(self):
        """The API deprecation errors setting.

        When set, this value is sent to the server in the
        "apiDeprecationErrors" field.
        """
        return self._deprecation_errors


def _add_to_command(cmd, server_api):
    if not server_api:
        return
    cmd['apiVersion'] = server_api.version
    if server_api.strict is not None:
        cmd['apiStrict'] = server_api.strict
    if server_api.deprecation_errors is not None:
        cmd['apiDeprecationErrors'] = server_api.deprecation_errors
