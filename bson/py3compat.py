# Copyright 2009-2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Utility functions and definitions for python3 compatibility."""

import sys

PY3 = sys.version_info[0] == 3

if PY3:
    import codecs
    def b(s):
        return codecs.latin_1_encode(s)[0]

    def bytes_from_hex(h):
        return bytes.fromhex(h)

    binary_type = bytes
    text_type   = str

else:
    def b(s):
        return s

    def bytes_from_hex(h):
        return h.decode('hex')

    binary_type = str
    # 2to3 will convert this to "str". That's okay
    # since we won't ever get here under python3.
    text_type   = unicode

string_types = (binary_type, text_type)
