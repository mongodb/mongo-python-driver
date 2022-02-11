# Copyright 2017 MongoDB, Inc.
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

"""Test PyMongo with cdecimal monkey-patched over stdlib decimal."""

import getopt
import sys

try:
    import cdecimal

    _HAVE_CDECIMAL = True
except ImportError:
    _HAVE_CDECIMAL = False


def run(args):
    """Run tests with cdecimal monkey-patched over stdlib decimal."""
    # Monkey-patch.
    sys.modules["decimal"] = cdecimal

    # Run the tests.
    sys.argv[:] = ["setup.py", "test"] + list(args)
    import setup


def main():
    """Parse options and run tests."""
    usage = """python %s

Test PyMongo with cdecimal monkey-patched over decimal.""" % (
        sys.argv[0],
    )

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.GetoptError as err:
        print(str(err))
        print(usage)
        sys.exit(2)

    for option_name, _ in opts:
        if option_name in ("-h", "--help"):
            print(usage)
            sys.exit()
        else:
            assert False, "unhandled option"

    if not _HAVE_CDECIMAL:
        print("The cdecimal package is not installed.")
        sys.exit(1)

    run(args)  # Command line args to setup.py, like what test to run.


if __name__ == "__main__":
    main()
