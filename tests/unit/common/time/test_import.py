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


def test_import():
    import neo4j.time  # noqa: F401 - unused import to test import works


def test_import_from():
    from neo4j import time  # noqa: F401 - unused import to test import works


MODULE_ATTRIBUTES = (
    # (name, warning)
    ("ClockTime", DeprecationWarning),
    ("MAX_INT64", None),
    ("MAX_YEAR", None),
    ("MIN_INT64", None),
    ("MIN_YEAR", None),
    ("Date", None),
    ("DateTime", None),
    ("Duration", None),
    ("Midday", None),
    ("Midnight", None),
    ("Never", None),
    ("Time", None),
    ("UnixEpoch", None),
    ("ZeroDate", None),
)

MODULE_ATTRIBUTES_NOT_IN_ALL = ("ClockTime",)


@pytest.mark.parametrize(("name", "warning"), MODULE_ATTRIBUTES)
def test_attribute_import(name, warning):
    module = importlib.__import__("neo4j.time").time
    if warning:
        with pytest.warns(warning):
            getattr(module, name)
    else:
        getattr(module, name)


@pytest.mark.parametrize(("name", "warning"), MODULE_ATTRIBUTES)
def test_attribute_from_import(name, warning):
    if warning:
        with pytest.warns(warning):
            importlib.__import__("neo4j.time", fromlist=(name,))
    else:
        importlib.__import__("neo4j.time", fromlist=(name,))


def test_all():
    import neo4j.time as module

    assert sorted(module.__all__) == sorted(
        [
            i[0]
            for i in MODULE_ATTRIBUTES
            if i[0] not in MODULE_ATTRIBUTES_NOT_IN_ALL
        ]
    )


def test_import_star():
    importlib.__import__("neo4j.time", fromlist=("*",))
