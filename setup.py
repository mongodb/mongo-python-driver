import glob
import os
import subprocess
import sys
import warnings

# Hack to silence atexit traceback in newer python versions.
try:
    import multiprocessing
except ImportError:
    pass

try:
    from ConfigParser import SafeConfigParser
except ImportError:
    # PY3
    from configparser import SafeConfigParser

# Don't force people to install distribute unless
# we have to.
try:
    from setuptools import setup, Feature
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, Feature

from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
from distutils.core import Extension

version = "2.5.1+"

f = open("README.rst")
try:
    try:
        readme_content = f.read()
    except:
        readme_content = ""
finally:
    f.close()

PY3 = sys.version_info[0] == 3

nose_config_options = {
    'with-xunit': '1',    # Write out nosetests.xml for CI.
    'py3where': 'build',  # Tell nose where to find tests under PY3.
}

def write_nose_config():
    """Write out setup.cfg. Since py3where has to be set
    for tests to run correctly in Python 3 we create this
    on the fly.
    """
    config = SafeConfigParser()
    config.add_section('nosetests')
    for opt, val in nose_config_options.items():
        config.set('nosetests', opt, val)
    try:
        cf = open('setup.cfg', 'w')
        config.write(cf)
    finally:
        cf.close()


def should_run_tests():
    if "test" in sys.argv or "nosetests" in sys.argv:
        return True
    return False


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

            try:
                os.makedirs(path)
            except:
                pass

        status = subprocess.call(["sphinx-build", "-E",
                                  "-b", mode, "doc", path])

        if status:
            raise RuntimeError("documentation step '%s' failed" % (mode,))

        sys.stdout.write("\nDocumentation step '%s' performed, results here:\n"
                         "   %s/\n" % (mode, path))


if sys.platform == 'win32' and sys.version_info > (2, 6):
    # 2.6's distutils.msvc9compiler can raise an IOError when failing to
    # find the compiler
    build_errors = (CCompilerError, DistutilsExecError,
                    DistutilsPlatformError, IOError)
else:
    build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)


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

http://api.mongodb.org/python/current/installation.html

Here are some hints for popular operating systems:

If you are seeing this message on Linux you probably need to
install GCC and/or the Python development package for your
version of Python.

Debian and Ubuntu users should issue the following command:

    $ sudo apt-get install build-essential python-dev

RedHat, CentOS, and Fedora users should issue the following command:

    $ sudo yum install gcc python-devel

If you are seeing this message on Microsoft Windows please install
PyMongo using the MS Windows installer for your version of Python,
available on pypi here:

http://pypi.python.org/pypi/pymongo/#downloads

If you are seeing this message on OSX please read the documentation
here:

http://api.mongodb.org/python/current/installation.html#osx
********************************************************************
"""

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            e = sys.exc_info()[1]
            sys.stdout.write('%s\n' % str(e))
            warnings.warn(self.warning_message % ("Extension modules",
                                                  "There was an issue with "
                                                  "your platform configuration"
                                                  " - see above."))

    def set_nose_options(self):
        # Under python 3 we need to tell nose where to find the
        # proper tests. if we built the C extensions this will be
        # someplace like build/lib.<os>-<arch>-<python version>
        if PY3:
            ver = '.'.join(map(str, sys.version_info[:2]))
            lib_dirs = glob.glob(os.path.join('build', 'lib*' + ver))
            if lib_dirs:
                nose_config_options['py3where'] = lib_dirs[0]
        write_nose_config()

    def build_extension(self, ext):
        name = ext.name
        if sys.version_info[:3] >= (2, 4, 0):
            try:
                build_ext.build_extension(self, ext)
                if should_run_tests():
                    self.set_nose_options()
            except build_errors:
                e = sys.exc_info()[1]
                sys.stdout.write('%s\n' % str(e))
                warnings.warn(self.warning_message % ("The %s extension "
                                                      "module" % (name,),
                                                      "The output above "
                                                      "this warning shows how "
                                                      "the compilation "
                                                      "failed."))
        else:
            warnings.warn(self.warning_message % ("The %s extension "
                                                  "module" % (name,),
                                                  "Please use Python >= 2.4 "
                                                  "to take advantage of the "
                                                  "extension."))

c_ext = Feature(
    "optional C extensions",
    standard=True,
    ext_modules=[Extension('bson._cbson',
                           include_dirs=['bson'],
                           sources=['bson/_cbsonmodule.c',
                                    'bson/time64.c',
                                    'bson/buffer.c',
                                    'bson/encoding_helpers.c']),
                 Extension('pymongo._cmessage',
                           include_dirs=['bson'],
                           sources=['pymongo/_cmessagemodule.c',
                                    'bson/buffer.c'])])

if "--no_ext" in sys.argv:
    sys.argv = [x for x in sys.argv if x != "--no_ext"]
    features = {}
elif (sys.platform.startswith("java") or
      sys.platform == "cli" or
      "PyPy" in sys.version):
    sys.stdout.write("""
*****************************************************\n
The optional C extensions are currently not supported\n
by this python implementation.\n
*****************************************************\n
""")
    features = {}
elif sys.byteorder == "big":
    sys.stdout.write("""
*****************************************************\n
The optional C extensions are currently not supported\n
on big endian platforms and will not be built.\n
Performance may be degraded.\n
*****************************************************\n
""")
    features = {}
else:
    features = {"c-ext": c_ext}

extra_opts = {
    "packages": ["bson", "pymongo", "gridfs"],
    "test_suite": "nose.collector"
}
if PY3:
    extra_opts["use_2to3"] = True
    if should_run_tests():
        # Distribute isn't smart enough to copy the
        # tests and run 2to3 on them. We don't want to
        # install the test suite so only do this if we
        # are testing.
        # https://bitbucket.org/tarek/distribute/issue/233
        extra_opts["packages"].append("test")
        extra_opts['package_data'] = {"test": ["certificates/ca.pem",
                                               "certificates/client.pem"]}
        # Hack to make "python3.x setup.py nosetests" work in python 3
        # otherwise it won't run 2to3 before running the tests.
        if "nosetests" in sys.argv:
            sys.argv.remove("nosetests")
            sys.argv.append("test")
            # All "nosetests" does is import and run nose.main.
            extra_opts["test_suite"] = "nose.main"

# This may be called a second time if
# we are testing with C extensions.
if should_run_tests():
    write_nose_config()

setup(
    name="pymongo",
    version=version,
    description="Python driver for MongoDB <http://www.mongodb.org>",
    long_description=readme_content,
    author="Mike Dirolf",
    author_email="mongodb-user@googlegroups.com",
    maintainer="Bernie Hackett",
    maintainer_email="bernie@10gen.com",
    url="http://github.com/mongodb/mongo-python-driver",
    keywords=["mongo", "mongodb", "pymongo", "gridfs", "bson"],
    install_requires=[],
    features=features,
    license="Apache License, Version 2.0",
    tests_require=["nose"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.4",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.1",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: Jython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database"],
    cmdclass={"build_ext": custom_build_ext,
              "doc": doc},
    **extra_opts
)
