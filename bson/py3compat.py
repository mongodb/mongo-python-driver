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
import codecs

PY3 = sys.version_info[0] == 3


def b(s):
    # BSON and socket operations deal in binary data. In
    # python 3 that means instances of `bytes`. In python
    # 2.6 and 2.7 you can create an alias for `bytes` using
    # the b prefix (e.g. b'foo'). Python 2.4 and 2.5 don't
    # provide this marker so we provide this compat function.
    # In python 3.x b('foo') results in b'foo'.
    # See http://python3porting.com/problems.html#nicer-solutions
    return codecs.latin_1_encode(s)[0] if PY3 else s


def bytes_from_hex(h):
    return bytes.fromhex(h) if PY3 else h.decode('hex')

binary_type = bytes if PY3 else str
# 2to3 will convert this to "str". That's okay
# since we won't ever get here under python3.
text_type = str if PY3 else unicode


string_types = (binary_type, text_type)
