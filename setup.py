
import os
import sys

try:
    from setuptools import setup, find_packages
    from setuptools.extension import Extension
except ImportError:
    from distutils.core import setup, find_packages
    from distutils.extension import Extension

packages = find_packages(exclude=("test", "test.*"))
package_metadata = {
    "name": __package__,
    "version": "1.0.0",
    "description": "Python driver for Neo4j",
    "long_description": "", 
    "author": "Neo Technology",
    "classifiers": [
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Software Development",
    ],
    "zip_safe": False,
}

setup(**package_metadata)

