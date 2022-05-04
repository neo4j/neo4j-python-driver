# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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


import pytest

from neo4j.data import DataHydrator
from neo4j.packstream import Structure


# python -m pytest -s -v tests/unit/test_data.py


def test_can_hydrate_v1_node_structure():
    hydrant = DataHydrator()

    struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
    alice, = hydrant.hydrate([struct])

    with pytest.warns(DeprecationWarning, match="element_id"):
        assert alice.id == 123
    # for backwards compatibility, the driver should compy the element_id
    assert alice.element_id == "123"
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"


@pytest.mark.parametrize("with_id", (True, False))
def test_can_hydrate_v2_node_structure(with_id):
    hydrant = DataHydrator()

    id_ = 123 if with_id else None

    struct = Structure(b'N', id_, ["Person"], {"name": "Alice"}, "abc")
    alice, = hydrant.hydrate([struct])

    with pytest.warns(DeprecationWarning, match="element_id"):
        assert alice.id == id_
    assert alice.element_id == "abc"
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"


def test_can_hydrate_v1_relationship_structure():
    hydrant = DataHydrator()

    struct = Structure(b'R', 123, 456, 789, "KNOWS", {"since": 1999})
    rel, = hydrant.hydrate([struct])

    with pytest.warns(DeprecationWarning, match="element_id"):
        assert rel.id == 123
    with pytest.warns(DeprecationWarning, match="element_id"):
        assert rel.start_node.id == 456
    with pytest.warns(DeprecationWarning, match="element_id"):
        assert rel.end_node.id == 789
    # for backwards compatibility, the driver should compy the element_id
    assert rel.element_id == "123"
    assert rel.start_node.element_id == "456"
    assert rel.end_node.element_id == "789"
    assert rel.type == "KNOWS"
    assert set(rel.keys()) == {"since"}
    assert rel.get("since") == 1999


@pytest.mark.parametrize("with_ids", (True, False))
def test_can_hydrate_v2_relationship_structure(with_ids):
    hydrant = DataHydrator()

    id_ = 123 if with_ids else None
    start_id = 456 if with_ids else None
    end_id = 789 if with_ids else None

    struct = Structure(b'R', id_, start_id, end_id, "KNOWS", {"since": 1999},
                       "abc", "def", "ghi")

    rel, = hydrant.hydrate([struct])

    with pytest.warns(DeprecationWarning, match="element_id"):
        assert rel.id == id_
    with pytest.warns(DeprecationWarning, match="element_id"):
        assert rel.start_node.id == start_id
    with pytest.warns(DeprecationWarning, match="element_id"):
        assert rel.end_node.id == end_id
    # for backwards compatibility, the driver should compy the element_id
    assert rel.element_id == "abc"
    assert rel.start_node.element_id == "def"
    assert rel.end_node.element_id == "ghi"
    assert rel.type == "KNOWS"
    assert set(rel.keys()) == {"since"}
    assert rel.get("since") == 1999


def test_hydrating_unknown_structure_returns_same():
    hydrant = DataHydrator()

    struct = Structure(b'?', "foo")
    mystery, = hydrant.hydrate([struct])

    assert mystery == struct


def test_can_hydrate_in_list():
    hydrant = DataHydrator()

    struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
    alice_in_list, = hydrant.hydrate([[struct]])

    assert isinstance(alice_in_list, list)

    alice, = alice_in_list

    assert alice.id == 123
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"


def test_can_hydrate_in_dict():
    hydrant = DataHydrator()

    struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
    alice_in_dict, = hydrant.hydrate([{"foo": struct}])

    assert isinstance(alice_in_dict, dict)

    alice = alice_in_dict["foo"]

    assert alice.id == 123
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"
