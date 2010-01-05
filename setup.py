#!/usr/bin/env python

import sys
import os
try:
    import subprocess
    has_subprocess = True
except:
    has_subprocess = False
import shutil

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup
from setuptools import Feature
from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
from distutils.core import Extension

from pymongo import version

f = open("README.rst")
try:
    try:
        readme_content = f.read()
    except:
        readme_content = ""
finally:
    f.close()


class doc(Command):

    description = "generate or test documentation"

    user_options = [("test", "t",
                     "run doctests instead of generating documentation")]

    boolean_options = ["test"]

    def initialize_options(self):
        self.test = False

    def finalize_options(self):
        pass

    def run(self):
        if self.test:
            path = "doc/_build/doctest"
            mode = "doctest"
        else:
            path = "doc/_build/%s" % version
            mode = "html"

            # shutil.rmtree("doc/_build", ignore_errors=True)
            try:
                os.makedirs(path)
            except:
                pass

        if has_subprocess:
            status = subprocess.call(["sphinx-build", "-b", mode, "doc", path])

            if status:
                raise RuntimeError("documentation step '%s' failed" % mode)

            print ""
            print "Documentation step '%s' performed, results here:" % mode
            print "   %s/" % path
        else:
            print """
`setup.py doc` is not supported for this version of Python.

Please ask in the user forums for help.
"""


if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError,
                 IOError)
else:
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)


class custom_build_ext(build_ext):
    """Allow C extension building to fail.

    The C extension speeds up BSON encoding, but is not essential.
    """

    warning_message = """
**************************************************************
WARNING: %s could not
be compiled. No C extensions are essential for PyMongo to run,
although they do result in significant speed improvements.

%s
**************************************************************
"""

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError, e:
            print e
            print self.warning_message % ("Extension modules",
                                          "There was an issue with your "
                                          "platform configuration - see above.")

    def build_extension(self, ext):
        if sys.version_info[:3] >= (2, 4, 0):
            try:
                build_ext.build_extension(self, ext)
            except build_errors:
                print self.warning_message % ("The %s extension module" % ext.name,
                                              "Above is the ouput showing how "
                                              "the compilation failed.")
        else:
            print self.warning_message % ("The %s extension module" % ext.name,
                                          "Please use Python >= 2.4 to take "
                                          "advantage of the extension.")

c_ext = Feature(
    "optional C extension",
    standard=True,
    ext_modules=[Extension('pymongo._cbson',
                           include_dirs=['pymongo'],
                           sources=['pymongo/_cbsonmodule.c',
                                    'pymongo/time_helpers.c',
                                    'pymongo/encoding_helpers.c'])])

if "--no_ext" in sys.argv:
    sys.argv = [x for x in sys.argv if x != "--no_ext"]
    features = {}
else:
    features = {"c-ext": c_ext}

setup(
    name="pymongo",
    version=version,
    description="Python driver for MongoDB <http://www.mongodb.org>",
    long_description=readme_content,
    author="Mike Dirolf",
    author_email="mongodb-user@googlegroups.com",
    url="http://github.com/mongodb/mongo-python-driver",
    keywords=["mongo", "mongodb", "pymongo", "gridfs"],
    packages=["pymongo", "gridfs"],
    install_requires=[],
    features=features,
    license="Apache License, Version 2.0",
    test_suite="nose.collector",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database"],
    cmdclass={"build_ext": custom_build_ext,
              "doc": doc})
