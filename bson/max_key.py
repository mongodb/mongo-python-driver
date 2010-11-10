# Copyright 2010 10gen, Inc.
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

"""Representation for the MongoDB internal MaxKey type.
"""


class MaxKey(object):
    """MongoDB internal MaxKey type.
    """

    def __eq__(self, other):
        if isinstance(other, MaxKey):
            return True
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "MaxKey()"
