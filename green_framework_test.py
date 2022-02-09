# Copyright 2015-present MongoDB, Inc.
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

"""Test PyMongo with a variety of greenlet-based monkey-patching frameworks."""

import getopt
import sys


def run_gevent():
    """Prepare to run tests with Gevent. Can raise ImportError."""
    from gevent import monkey

    monkey.patch_all()


def run_eventlet():
    """Prepare to run tests with Eventlet. Can raise ImportError."""
    import eventlet

    # https://github.com/eventlet/eventlet/issues/401
    eventlet.sleep()
    eventlet.monkey_patch()


FRAMEWORKS = {
    "gevent": run_gevent,
    "eventlet": run_eventlet,
}


def list_frameworks():
    """Tell the user what framework names are valid."""
    sys.stdout.write(
        """Testable frameworks: %s

Note that membership in this list means the framework can be tested with
PyMongo, not necessarily that it is officially supported.
"""
        % ", ".join(sorted(FRAMEWORKS))
    )


def run(framework_name, *args):
    """Run tests with monkey-patching enabled. Can raise ImportError."""
    # Monkey-patch.
    FRAMEWORKS[framework_name]()

    # Run the tests.
    sys.argv[:] = ["setup.py", "test"] + list(args)
    import setup


def main():
    """Parse options and run tests."""
    usage = """python %s FRAMEWORK_NAME

Test PyMongo with a variety of greenlet-based monkey-patching frameworks. See
python %s --help-frameworks.""" % (
        sys.argv[0],
        sys.argv[0],
    )

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "help-frameworks"])
    except getopt.GetoptError as err:
        print(str(err))
        print(usage)
        sys.exit(2)

    for option_name, _ in opts:
        if option_name in ("-h", "--help"):
            print(usage)
            sys.exit()
        elif option_name == "--help-frameworks":
            list_frameworks()
            sys.exit()
        else:
            assert False, "unhandled option"

    if not args:
        print(usage)
        sys.exit(1)

    if args[0] not in FRAMEWORKS:
        print("%r is not a testable framework.\n" % args[0])
        list_frameworks()
        sys.exit(1)

    run(
        args[0], *args[1:]  # Framework name.
    )  # Command line args to setup.py, like what test to run.


if __name__ == "__main__":
    main()
