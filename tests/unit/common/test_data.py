# Copyright (c) "Neo4j"
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


from neo4j.data import DataHydrator
from neo4j.packstream import Structure


# python -m pytest -s -v tests/unit/test_data.py


def test_can_hydrate_node_structure():
    hydrant = DataHydrator()

    struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
    alice, = hydrant.hydrate([struct])

    assert alice.id == 123
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"


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
