# Copyright 2013 MongoDB, Inc.
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

"""Tools for representing MongoDB regular expressions.
"""

import re

from bson.py3compat import string_types


def str_flags_to_int(str_flags):
    flags = 0
    if "i" in str_flags:
        flags |= re.IGNORECASE
    if "l" in str_flags:
        flags |= re.LOCALE
    if "m" in str_flags:
        flags |= re.MULTILINE
    if "s" in str_flags:
        flags |= re.DOTALL
    if "u" in str_flags:
        flags |= re.UNICODE
    if "x" in str_flags:
        flags |= re.VERBOSE

    return flags


class Regex(object):
    """BSON regular expression data."""
    _type_marker = 11

    def __init__(self, pattern, flags=0):
        """BSON regular expression data.

        This class is useful to store and retrieve regular expressions that are
        incompatible with Python's regular expression dialect.

        :Parameters:
          - `pattern`: string
          - `flags`: (optional) an integer bitmask, or a string of flag
            characters like "im" for IGNORECASE and MULTILINE
        """
        if not isinstance(pattern, string_types):
            raise TypeError("pattern must be a string, not %s" % type(pattern))
        self.pattern = pattern

        if isinstance(flags, string_types):
            self.flags = str_flags_to_int(flags)
        elif isinstance(flags, int):
            self.flags = flags
        else:
            raise TypeError(
                "flags must be a string or int, not %s" % type(flags))

    def __eq__(self, other):
        if isinstance(other, Regex):
            return self.pattern == self.pattern and self.flags == other.flags
        else:
            return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Regex(%r, %r)" % (self.pattern, self.flags)

    def compile(self):
        """Compile this ``Regex`` as a Python regular expression.
        """
        return re.compile(self.pattern, self.flags)
