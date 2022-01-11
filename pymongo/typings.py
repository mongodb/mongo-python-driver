# Copyright 2014-2022 MongoDB, Inc.
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
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Tuple, Type, TypeVar, Union, TYPE_CHECKING


if TYPE_CHECKING:
    from pymongo.collation import Collation


# Common Shared Types.
Address = Tuple[str, Optional[int]]
CollationIn = Union[Mapping[str, Any], "Collation"]
DocumentIn = MutableMapping[str, Any]
Pipeline = Sequence[Mapping[str, Any]]
DocumentType = TypeVar('DocumentType', Mapping[str, Any], MutableMapping[str, Any])
