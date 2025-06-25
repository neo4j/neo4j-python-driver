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


import importlib

import pytest


MODULE_PATH = "neo4j.vector"
VECTOR_ATTRIBUTES = (
    # (name, warning)
    ("Vector", None),
    ("VectorDType", None),
    ("VectorEndian", None),
)


def _get_module():
    module = importlib.__import__(MODULE_PATH)
    for submodule in MODULE_PATH.split(".")[1:]:
        module = getattr(module, submodule)
    return module


@pytest.mark.parametrize(("name", "warning"), VECTOR_ATTRIBUTES)
def test_attribute_import(name, warning):
    module = _get_module()
    if warning:
        with pytest.warns(warning):
            getattr(module, name)
    else:
        getattr(module, name)


@pytest.mark.parametrize(("name", "warning"), VECTOR_ATTRIBUTES)
def test_attribute_from_import(name, warning):
    if warning:
        with pytest.warns(warning):
            importlib.__import__(MODULE_PATH, fromlist=(name,))
    else:
        importlib.__import__(MODULE_PATH, fromlist=(name,))


def test_all():
    module = _get_module()

    assert sorted(module.__all__) == sorted([i[0] for i in VECTOR_ATTRIBUTES])


def test_dir():
    module = _get_module()

    dir_attrs = (attr for attr in dir(module) if not attr.startswith("_"))
    assert sorted(dir_attrs) == sorted([i[0] for i in VECTOR_ATTRIBUTES])


def test_import_star():
    # ignore PT029: purposefully capturing all warnings to then apply further
    # checks on them
    importlib.__import__(MODULE_PATH, fromlist=("*",))
