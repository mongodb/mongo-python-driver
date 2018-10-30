# Copyright 2014-present MongoDB, Inc.
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


class CustomBSONTypeABC(object):
    def to_bson(self, key, writer):
        raise NotImplementedError


class CustomDocumentClassABC(object):
    def to_bson(self, writer):
        raise NotImplementedError

    @classmethod
    def from_bson(cls, reader):
        result = {}
        reader.start_document()
        for entry in reader:
            result[entry.name] = entry.value
        reader.end_document()
        return result

    @property
    def _id(self):
        raise NotImplementedError