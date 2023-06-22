import os
import sys
import warnings

# Hack to silence atexit traceback in some Python versions
try:
    import multiprocessing  # noqa: F401
except ImportError:
    pass

from setuptools import Command, setup
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension


class test(Command):
    description = "run the tests"

    user_options = [
        ("test-module=", "m", "Discover tests in specified module"),
        ("test-suite=", "s", "Test suite to run (e.g. 'some_module.test_suite')"),
        ("failfast", "f", "Stop running tests on first failure or error"),
        ("xunit-output=", "x", "Generate a results directory with XUnit XML format"),
    ]

    def initialize_options(self):
        self.test_module = None
        self.test_suite = None
        self.failfast = False
        self.xunit_output = None

    def finalize_options(self):
        if self.test_suite is None and self.test_module is None:
            self.test_module = "test"
        elif self.test_module is not None and self.test_suite is not None:
            raise Exception("You may specify a module or suite, but not both")

    def run(self):
        # Installing required packages, running egg_info and build_ext are
        # part of normal operation for setuptools.command.test.test
        if self.distribution.install_requires:
            self.distribution.fetch_build_eggs(self.distribution.install_requires)
        if self.distribution.tests_require:
            self.distribution.fetch_build_eggs(self.distribution.tests_require)
        if self.xunit_output:
            self.distribution.fetch_build_eggs(["unittest-xml-reporting"])
        self.run_command("egg_info")
        build_ext_cmd = self.reinitialize_command("build_ext")
        build_ext_cmd.inplace = 1
        self.run_command("build_ext")

        # Construct a TextTestRunner directly from the unittest imported from
        # test, which creates a TestResult that supports the 'addSkip' method.
        # setuptools will by default create a TextTestRunner that uses the old
        # TestResult class.
        from test import PymongoTestRunner, test_cases, unittest

        if self.test_suite is None:
            all_tests = unittest.defaultTestLoader.discover(self.test_module)
            suite = unittest.TestSuite()
            suite.addTests(sorted(test_cases(all_tests), key=lambda x: x.__module__))
        else:
            suite = unittest.defaultTestLoader.loadTestsFromName(self.test_suite)
        if self.xunit_output:
            from test import PymongoXMLTestRunner

            runner = PymongoXMLTestRunner(
                verbosity=2, failfast=self.failfast, output=self.xunit_output
            )
        else:
            runner = PymongoTestRunner(verbosity=2, failfast=self.failfast)
        result = runner.run(suite)
        sys.exit(not result.wasSuccessful())


class custom_build_ext(build_ext):
    """Allow C extension building to fail.

    The C extension speeds up BSON encoding, but is not essential.
    """

    warning_message = """
********************************************************************
WARNING: %s could not
be compiled. No C extensions are essential for PyMongo to run,
although they do result in significant speed improvements.
%s

Please see the installation docs for solutions to build issues:

https://pymongo.readthedocs.io/en/stable/installation.html

Here are some hints for popular operating systems:

If you are seeing this message on Linux you probably need to
install GCC and/or the Python development package for your
version of Python.

Debian and Ubuntu users should issue the following command:

    $ sudo apt-get install build-essential python-dev

Users of Red Hat based distributions (RHEL, CentOS, Amazon Linux,
Oracle Linux, Fedora, etc.) should issue the following command:

    $ sudo yum install gcc python-devel

If you are seeing this message on Microsoft Windows please install
PyMongo using pip. Modern versions of pip will install PyMongo
from binary wheels available on pypi. If you must install from
source read the documentation here:

https://pymongo.readthedocs.io/en/stable/installation.html#installing-from-source-on-windows

If you are seeing this message on macOS / OSX please install PyMongo
using pip. Modern versions of pip will install PyMongo from binary
wheels available on pypi. If wheels are not available for your version
of macOS / OSX, or you must install from source read the documentation
here:

https://pymongo.readthedocs.io/en/stable/installation.html#osx
********************************************************************
"""

    def run(self):
        try:
            build_ext.run(self)
        except Exception:
            if "TOX_ENV_NAME" in os.environ:
                raise
            e = sys.exc_info()[1]
            sys.stdout.write("%s\n" % str(e))
            warnings.warn(
                self.warning_message
                % (
                    "Extension modules",
                    "There was an issue with your platform configuration - see above.",
                )
            )

    def build_extension(self, ext):
        name = ext.name
        try:
            build_ext.build_extension(self, ext)
        except Exception:
            if "TOX_ENV_NAME" in os.environ:
                raise
            e = sys.exc_info()[1]
            sys.stdout.write("%s\n" % str(e))
            warnings.warn(
                self.warning_message
                % (
                    "The %s extension module" % (name,),
                    "The output above this warning shows how the compilation failed.",
                )
            )


ext_modules = [
    Extension(
        "bson._cbson",
        include_dirs=["bson"],
        sources=["bson/_cbsonmodule.c", "bson/time64.c", "bson/buffer.c"],
    ),
    Extension(
        "pymongo._cmessage",
        include_dirs=["bson"],
        sources=[
            "pymongo/_cmessagemodule.c",
            "bson/_cbsonmodule.c",
            "bson/time64.c",
            "bson/buffer.c",
        ],
    ),
]


if "--no_ext" in sys.argv or "NO_EXT" in os.environ:
    sys.argv.remove("--no_ext")
    ext_modules = []
elif sys.platform.startswith("java") or sys.platform == "cli" or "PyPy" in sys.version:
    sys.stdout.write(
        """
*****************************************************\n
The optional C extensions are currently not supported\n
by this python implementation.\n
*****************************************************\n
"""
    )
    ext_modules = []

setup(
    cmdclass={"build_ext": custom_build_ext, "test": test}, ext_modules=ext_modules
)  # type:ignore
