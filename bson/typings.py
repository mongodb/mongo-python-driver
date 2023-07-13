# Copyright 2023-Present MongoDB, Inc.
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

"""Type aliases used by bson"""
from typing import TYPE_CHECKING, Any, Mapping, TypeVar, Union

if TYPE_CHECKING:
    from array import array
    from mmap import mmap


# Common Shared Types.
_DocumentType = TypeVar("_DocumentType", bound=Mapping[str, Any])
_DocumentTypeArg = TypeVar("_DocumentTypeArg", bound=Mapping[str, Any])
_ReadableBuffer = Union[bytes, memoryview, "mmap", "array"]
