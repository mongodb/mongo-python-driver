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

"""Support for MongoDB Versioned API."""


class ServerApiVersion:
    """An enum that defines values for ServerApi `version`.

    .. versionadded:: 3.12
    """

    V1 = "1"
    """Server API version "1"."""


class ServerApi(object):
    """MongoDB Versioned API.

    .. versionadded:: 3.12
    """
    def __init__(self, version, strict=None, deprecation_errors=None):
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
        self.version = version
        self.strict = strict
        self.deprecation_errors = deprecation_errors


def _add_to_command(cmd, server_api):
    if not server_api:
        return
    cmd['apiVersion'] = server_api.version
    if server_api.strict is not None:
        cmd['apiStrict'] = server_api.strict
    if server_api.deprecation_errors is not None:
        cmd['apiDeprecationErrors'] = server_api.deprecation_errors
