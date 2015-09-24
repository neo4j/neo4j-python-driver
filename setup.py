# -*- coding: utf-8 -*-
"""
   Copyright 2015 Neo Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
try:
    from setuptools import setup, find_packages
    from setuptools.extension import Extension
except ImportError:
    from distutils.core import setup, find_packages
    from distutils.extension import Extension


# Used for reading the README into long_description below.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


packages = find_packages(exclude=("test", "test.*"))

setup(name=__package__,
      version="1.0.0",
      description="Python driver for Neo4j",
      license="Apache",
      long_description=read("README.rst"),
      author="Neo Technology",
      keywords="neo4j graph database",
      url="https://github.com/neo4j/neo4j-python-driver",
      classifiers=[
          "Intended Audience :: Developers",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: OS Independent",
          "Topic :: Database",
          "Topic :: Software Development",
          "Programming Language :: Python :: 2.6",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.3",
          "Programming Language :: Python :: 3.4",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: Implementation :: Jython",
      ],
      packages=packages)
