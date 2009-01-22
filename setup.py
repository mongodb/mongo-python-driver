#!/usr/bin/env python

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup

setup(
    name="pymongo",
    version="0.1",
    description="Driver for the Mongo database <http://www.mongodb.org>",
    long_description="""\
The pymongo package is a driver for Mongo, a high performance, document-based
database. See http://www.mongodb.org and http://www.10gen.com for more
information on Mongo.
""",
    author="10gen",
    author_email="mike@10gen.com",
    url="http://github.com/mongodb/mongo-python-driver",
    packages=["pymongo"],
    install_requires=['elementtree'],
    test_suite = 'nose.collector')
