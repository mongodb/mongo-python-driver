from __future__ import annotations

import os
import sys
import warnings

# Hack to silence atexit traceback in some Python versions
try:
    import multiprocessing  # noqa: F401
except ImportError:
    pass

from setuptools import setup
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension


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
            if os.environ.get("PYMONGO_C_EXT_MUST_BUILD"):
                raise
            e = sys.exc_info()[1]
            sys.stdout.write("%s\n" % str(e))
            warnings.warn(
                self.warning_message
                % (
                    "Extension modules",
                    "There was an issue with your platform configuration - see above.",
                ),
                stacklevel=2,
            )

    def build_extension(self, ext):
        name = ext.name
        try:
            build_ext.build_extension(self, ext)
        except Exception:
            if os.environ.get("PYMONGO_C_EXT_MUST_BUILD"):
                raise
            e = sys.exc_info()[1]
            sys.stdout.write("%s\n" % str(e))
            warnings.warn(
                self.warning_message
                % (
                    "The %s extension module" % (name,),  # noqa: UP031
                    "The output above this warning shows how the compilation failed.",
                ),
                stacklevel=2,
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


if "--no_ext" in sys.argv or os.environ.get("NO_EXT"):
    try:
        sys.argv.remove("--no_ext")
    except ValueError:
        pass
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
    cmdclass={"build_ext": custom_build_ext},
    ext_modules=ext_modules,
    packages=["bson", "pymongo", "gridfs"],
)  # type:ignore
