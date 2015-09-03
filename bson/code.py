# Copyright 2009-2015 MongoDB, Inc.
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

"""Tools for representing JavaScript code in BSON.
"""

class Code(str):
    """BSON's JavaScript code type.

    Raises :class:`TypeError` if `code` is not an instance of
    :class:`basestring` (:class:`str` in python 3) or `scope`
    is not ``None`` or an instance of :class:`dict`.

    Scope variables can be set by passing a dictionary as the `scope`
    argument or by using keyword arguments. If a variable is set as a
    keyword argument it will override any setting for that variable in
    the `scope` dictionary.

    :Parameters:
      - `code`: string containing JavaScript code to be evaluated
      - `scope` (optional): dictionary representing the scope in which
        `code` should be evaluated - a mapping from identifiers (as
        strings) to values
      - `**kwargs` (optional): scope variables can also be passed as
        keyword arguments

    .. versionadded:: 1.9
       Ability to pass scope values using keyword arguments.
    """

    _type_marker = 13

    def __new__(cls, code, scope=None, **kwargs):
        if not isinstance(code, basestring):
            raise TypeError("code must be an "
                            "instance of %s" % (basestring.__name__,))

        self = str.__new__(cls, code)

        try:
            self.__scope = code.scope
        except AttributeError:
            self.__scope = {}

        if scope is not None:
            if not isinstance(scope, dict):
                raise TypeError("scope must be an instance of dict")
            self.__scope.update(scope)

        self.__scope.update(kwargs)

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

    __hash__ = None

    def __ne__(self, other):
        return not self == other
