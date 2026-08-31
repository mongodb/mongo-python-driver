# Copyright 2019-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Encryption utilities and shared helper methods."""

from __future__ import annotations

import contextlib
import enum
from collections.abc import Iterator
from typing import Any, Optional

try:
    from pymongocrypt.mongocrypt import MongoCryptOptions  # type:ignore[import-untyped]

    _HAVE_PYMONGOCRYPT = True
except ImportError:
    _HAVE_PYMONGOCRYPT = False

from bson.errors import BSONError
from pymongo.errors import EncryptionError
from pymongo.results import BulkWriteResult


@contextlib.contextmanager
def _wrap_encryption_errors() -> Iterator[None]:
    """Context manager to wrap encryption related errors."""
    try:
        yield
    except BSONError:
        # BSON encoding/decoding errors are unrelated to encryption so
        # we should propagate them unchanged.
        raise
    except Exception as exc:
        raise EncryptionError(exc) from exc


class RewrapManyDataKeyResult:
    """Result object returned by a
    :meth:`~pymongo.encryption.ClientEncryption.rewrap_many_data_key` operation.

    .. versionadded:: 4.2
    """

    def __init__(self, bulk_write_result: Optional[BulkWriteResult] = None) -> None:
        self._bulk_write_result = bulk_write_result

    @property
    def bulk_write_result(self) -> Optional[BulkWriteResult]:
        """The result of the bulk write operation used to update the key vault
        collection with one or more rewrapped data keys. If
        :meth:`~pymongo.encryption.ClientEncryption.rewrap_many_data_key` does not
        find any matching keys to rewrap, no bulk write operation will be executed
        and this field will be ``None``.
        """
        return self._bulk_write_result

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._bulk_write_result!r})"


class Algorithm(str, enum.Enum):
    """An enum that defines the supported encryption algorithms."""

    AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
    """AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic."""
    AEAD_AES_256_CBC_HMAC_SHA_512_Random = "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
    """AEAD_AES_256_CBC_HMAC_SHA_512_Random."""
    INDEXED = "Indexed"
    """Indexed.

    .. versionadded:: 4.2
    """
    UNINDEXED = "Unindexed"
    """Unindexed.

    .. versionadded:: 4.2
    """
    RANGE = "Range"
    """Range.

    .. versionadded:: 4.9
    """
    RANGEPREVIEW = "RangePreview"
    """**DEPRECATED** - RangePreview.

    .. note:: Support for RangePreview is deprecated. Use :attr:`Algorithm.RANGE` instead.

    .. versionadded:: 4.4
    """
    STRING = "String"
    """String.

    .. versionadded:: 4.18
    """
    TEXTPREVIEW = "TextPreview"
    """**DEPRECATED** - TextPreview.

    .. note:: Support for TextPreview is deprecated. Use :attr:`Algorithm.STRING` instead.

    .. versionadded:: 4.15
    """


class QueryType(str, enum.Enum):
    """An enum that defines the supported values for explicit encryption query_type.

    .. versionadded:: 4.2
    """

    EQUALITY = "equality"
    """Used to encrypt a value for an equality query."""

    RANGE = "range"
    """Used to encrypt a value for a range query.

    .. versionadded:: 4.9
    """

    RANGEPREVIEW = "RangePreview"
    """**DEPRECATED** - Used to encrypt a value for a rangePreview query.

    .. note:: Support for RangePreview is deprecated. Use :attr:`QueryType.RANGE` instead.

    .. versionadded:: 4.4
    """

    PREFIX = "prefix"
    """Used to encrypt a value for a prefix query.

    Used for the ``$encStrStartsWith`` operator. Requires MongoDB 9.0+.

    .. versionadded:: 4.18
    """

    SUFFIX = "suffix"
    """Used to encrypt a value for a suffix query.

    Used for the ``$encStrEndsWith`` operator. Requires MongoDB 9.0+.

    .. versionadded:: 4.18
    """

    SUBSTRING = "substring"
    """Used to encrypt a value for a substring query.

    Used for the ``$encStrContains`` operator. Requires MongoDB 9.0+.

    .. versionadded:: 4.18
    """

    PREFIXPREVIEW = "prefixPreview"
    """**BETA** - Used to encrypt a value for a prefixPreview query.

    .. note:: The preview query types are for experimental workloads only and
       are only supported by MongoDB versions before 9.0. Use
       :attr:`QueryType.PREFIX` instead.

    .. versionadded:: 4.15
    """

    SUFFIXPREVIEW = "suffixPreview"
    """**BETA** - Used to encrypt a value for a suffixPreview query.

    .. note:: The preview query types are for experimental workloads only and
       are only supported by MongoDB versions before 9.0. Use
       :attr:`QueryType.SUFFIX` instead.

    .. versionadded:: 4.15
    """

    SUBSTRINGPREVIEW = "substringPreview"
    """**BETA** - Used to encrypt a value for a substringPreview query.

    .. note:: The preview query types are for experimental workloads only and
       are only supported by MongoDB versions before 9.0. Use
       :attr:`QueryType.SUBSTRING` instead.

    .. versionadded:: 4.15
    """


def _create_mongocrypt_options(**kwargs: Any) -> MongoCryptOptions:
    # For compat with pymongocrypt <1.13, avoid setting the default key_expiration_ms.
    if kwargs.get("key_expiration_ms") is None:
        kwargs.pop("key_expiration_ms", None)
    return MongoCryptOptions(**kwargs, enable_multiple_collinfo=True)
