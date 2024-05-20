"""A custom hatch build hook for pymongo."""
from __future__ import annotations

import os
import subprocess
import sys
import warnings
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

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


class CustomHook(BuildHookInterface):
    """The pymongo build hook."""

    def initialize(self, version, build_data):
        """Initialize the hook."""
        if self.target_name == "sdist":
            return
        here = Path(__file__).parent.resolve()
        sys.path.insert(0, str(here))

        if "--no_ext" in sys.argv or os.environ.get("NO_EXT"):
            try:
                sys.argv.remove("--no_ext")
            except ValueError:
                pass
            return
        elif sys.platform == "cli" or "PyPy" in sys.version:
            sys.stdout.write(
                """
        *****************************************************\n
        The optional C extensions are currently not supported\n
        by this python implementation.\n
        *****************************************************\n
        """
            )
            return

        # Handle CMake invocation.
        try:
            cmake_build = here / "cmake_build"
            cmake_build.mkdir(parents=True)
            subprocess.check_call(["cmake", "-DCMAKE_BUILD_TYPE=Release", ".."], cwd=cmake_build)
            subprocess.check_call(["cmake", "build", "."], cwd=cmake_build)
            subprocess.check_call(["cmake", "--install", cmake_build, "--prefix", str(here)])
        except Exception:
            if os.environ.get("PYMONGO_C_EXT_MUST_BUILD"):
                raise
            e = sys.exc_info()[1]
            sys.stdout.write("%s\n" % str(e))
            warnings.warn(
                warning_message
                % (
                    "Extension modules",
                    "There was an issue with your platform configuration - see above.",
                ),
                stacklevel=2,
            )

        # Ensure wheel is marked as binary and contains the binary files.
        build_data["infer_tag"] = True
        build_data["pure_python"] = False
        if os.name == "nt":
            patt = ".pyd"
        else:
            patt = ".so"
        for pkg in ["bson", "pymongo"]:
            dpath = here / pkg
            for fpath in dpath.glob(f"*{patt}"):
                relpath = os.path.relpath(fpath, here)
                build_data["artifacts"].append(relpath)
                build_data["force_include"][relpath] = relpath
