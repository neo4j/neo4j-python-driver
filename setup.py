#!/usr/bin/env python

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TODO: 6.0 - the whole deprecation and double naming shebang can be removed


import os
import pathlib
import sys
import warnings
from contextlib import contextmanager

import tomlkit
from setuptools import setup


sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))

from neo4j._meta import (
    deprecated_package as deprecated,
    package,
)


if deprecated:
    warnings.warn(
        f"`{package}` is deprecated, please install `neo4j` instead.",
        DeprecationWarning
    )

readme_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                           "README.rst"))
with open(readme_path, mode="r", encoding="utf-8") as fr:
    readme = fr.read()

if deprecated:
    readme = """\
.. warning::

    This package is deprecated and will stop receiving updates starting with
    version 6.0.0. Please install ``neo4j`` instead (which is an alias, i.e.,
    a drop-in replacement). See https://pypi.org/project/neo4j/ .

""" + readme


def change_project_name(new_name):
    with open("pyproject.toml", "a+") as fd:
        fd.seek(0)
        pyproject = tomlkit.parse(fd.read())
        old_name = pyproject["project"]["name"]
        pyproject["project"]["name"] = new_name
        fd.seek(0)
        fd.truncate()
        tomlkit.dump(pyproject, fd)
        return old_name


@contextmanager
def changed_package_name(new_name):
    old_name = change_project_name(new_name)
    try:
        yield
    finally:
        change_project_name(old_name)


with changed_package_name(package):
    setup(long_description=readme)
