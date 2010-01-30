# Copyright 2009-2010 10gen, Inc.
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

"""Tools for representing JavaScript code to be evaluated by MongoDB.
"""

class Code(str):
    """JavaScript code to be evaluated by MongoDB.

    Raises :class:`TypeError` if `code` is not an instance of
    :class:`basestring` or `scope` is not an instance of
    :class:`dict`.

    :Parameters:
      - `code`: string containing JavaScript code to be evaluated
      - `scope` (optional): dictionary representing the scope in which
        `code` should be evaluated - a mapping from identifiers (as
        strings) to values
    """

    def __new__(cls, code, scope=None):
        if not isinstance(code, basestring):
            raise TypeError("code must be an instance of basestring")

        if scope is None:
            try:
                scope = code.scope
            except AttributeError:
                scope = {}
        if not isinstance(scope, dict):
            raise TypeError("scope must be an instance of dict")

        self = str.__new__(cls, code)
        self.__scope = scope
        return self

    @property
    def scope(self):
        """Scope dictionary for this instance.
        """
        return self.__scope

    def __repr__(self):
        return "Code(%s, %r)" % (str.__repr__(self), self.__scope)

    def __eq__(self, other):
        if isinstance(other, Code):
            return (self.__scope, str(self)) == (other.__scope, str(other))
        return False
