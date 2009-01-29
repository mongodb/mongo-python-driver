# Copyright 2009 10gen, Inc.
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

"""Representation of binary data to be stored in or retrieved from Mongo.

This is necessary because we want to store normal strings as the Mongo string
type. We need to wrap binary so we can tell the difference between what should
be considered binary and what should be considered a string.
"""

class Binary(str):
    """Binary data stored in or retrieved from Mongo.
    """
    def __repr__(self):
        return "Binary(%s)" % str.__repr__(self)
