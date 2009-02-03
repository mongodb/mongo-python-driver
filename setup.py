#!/usr/bin/env python

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup
from distutils.core import Extension

f = open("README.rst")
try:
    readme_content = f.read()
except:
    readme_content = ""
finally:
    f.close()

setup(
    name="pymongo",
    version="0.3.1pre",
    description="Driver for the Mongo database <http://www.mongodb.org>",
    long_description=readme_content,
    author="10gen",
    author_email="mike@10gen.com",
    url="http://github.com/mongodb/mongo-python-driver",
    packages=["pymongo", "gridfs"],
    ext_modules=[Extension('pymongo._cbson', ['pymongo/_cbsonmodule.c'])],
    install_requires=["elementtree"],
    license="Apache License, Version 2.0",
    test_suite="nose.collector",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database"])
