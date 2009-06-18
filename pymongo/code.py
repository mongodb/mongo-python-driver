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

"""Representation of JavaScript code to be evaluated by MongoDB.
"""

import types


class Code(str):
    """JavaScript code to be evaluated by MongoDB.
    """

    def __new__(cls, code, scope=None):
        """Initialize a new code object.

        `code` is a string containing JavaScript code.

        `scope` is a dictionary representing the scope in which `code` should
        be evaluated. It should be a mapping from identifiers (as strings) to
        values.

        Raises TypeError if `code` is not an instance of (str, unicode) or
        `scope` is not an instance of dict.

        :Parameters:
          - `code`: JavaScript code to be evaluated
          - `scope` (optional): dictionary representing the scope for
            evaluation
        """
        if not isinstance(code, types.StringTypes):
            raise TypeError("code must be an instance of (str, unicode)")

        if scope is None:
            scope = {}
        if not isinstance(scope, types.DictType):
            raise TypeError("scope must be an instance of dict")

        self = str.__new__(cls, code)
        self.__scope = scope
        return self

    def scope(self):
        """Get the scope dictionary.
        """
        return self.__scope
    scope = property(scope)

    def __repr__(self):
        return "Code(%s, %r)" % (str.__repr__(self), self.__scope)
