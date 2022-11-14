# Copyright 2022-Present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Type aliases used by PyMongo"""
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from bson.raw_bson import RawBSONDocument
    from pymongo.collation import Collation


# Common Shared Types.
_Address = Tuple[str, Optional[int]]
_CollationIn = Union[Mapping[str, Any], "Collation"]
_DocumentIn = Union[MutableMapping[str, Any], "RawBSONDocument"]
_Pipeline = Sequence[Mapping[str, Any]]
_DocumentOut = _DocumentIn
_DocumentType = TypeVar("_DocumentType", bound=Mapping[str, Any])


def strip_optional(elem):
    """This function is to allow us to cast all of the elements of an iterator from Optional[_T] to _T
    while inside a list comprehension."""
    assert elem is not None
    return elem
