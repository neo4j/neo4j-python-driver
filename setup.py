#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from os.path import dirname, join as path_join
from setuptools import find_packages, setup

from neo4j.meta import package, version

install_requires = [
    "pytz",
]
classifiers = [
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Topic :: Database",
    "Topic :: Software Development",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
]
entry_points = {
    "console_scripts": [
        "pybolt = neo4j.bolt.__main__:main",
    ],
}
packages = find_packages(exclude=["tests"])
readme = open(path_join(dirname(__file__), "README.rst")).read()
setup_args = {
    "name": package,
    "version": version,
    "description": "Neo4j Bolt driver for Python",
    "license": "Apache License, Version 2.0",
    "long_description": readme,
    "author": "Neo Technology",
    "author_email": "drivers@neo4j.com",
    "keywords": "neo4j graph database",
    "url": "https://github.com/neo4j/neo4j-python-driver",
    "install_requires": install_requires,
    "classifiers": classifiers,
    "packages": packages,
    "entry_points": entry_points,
}

setup(**setup_args)
