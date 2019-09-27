#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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

from neo4j.graph import (
    Node,
    Path,
    Graph,
)

from neo4j.time import (
    Date,
    Time,
    DateTime,
    Duration,
)

from neo4j.spatial import (
    CartesianPoint,
    WGS84Point,
)

import datetime

def test_create_node():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(123, {"Test", "Node"}, {"name": "Alice", "age": 33})

    assert alice.labels == {"Test", "Node"}
    assert alice.id == 123

    assert set(alice.keys()) == {"name", "age"}
    assert set(alice.values()) == {"Alice", 33}
    assert set(alice.items()) == {("name", "Alice"), ("age", 33)}
    assert repr(alice)
    assert len(alice) == 2
    assert set(iter(alice)) == {"name", "age"}

    assert alice.get("name") == "Alice"
    assert alice.get("age") == 33

    assert alice["name"], "Alice"
    assert alice["age"], 33

    assert "name" in alice.keys()
    assert "age" in alice.keys()

    assert "name" in alice
    assert "age" in alice


def test_null_properties_node():
    g = Graph()
    gh = Graph.Hydrator(g)
    node = gh.hydrate_node(123, (), {"good": ["puppies", "kittens"], "bad": None})

    assert set(node.keys()) == {"good"}
    assert len(node) == 1

    assert node.get("good") == ["puppies", "kittens"]
    assert node.get("bad") == None

    assert node["good"] == ["puppies", "kittens"]
    assert node["bad"] == None

    assert "good" in node
    assert "bad" not in node


def test_node_equality():
    g = Graph()
    node_1 = Node(g, 1234)
    node_2 = Node(g, 1234)
    node_3 = Node(g, 5678)

    assert node_1 == node_2
    assert node_1 != node_3
    assert node_1 != "this is not a node"


def test_node_hashing():
    g = Graph()
    node_1 = Node(g, 1234)
    node_2 = Node(g, 1234)
    node_3 = Node(g, 5678)

    assert hash(node_1) == hash(node_2)
    assert hash(node_1) != hash(node_3)


def test_create_relationship():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})

    assert alice_knows_bob.start_node == alice
    assert alice_knows_bob.type == "KNOWS"
    assert alice_knows_bob.end_node == bob
    assert set(alice_knows_bob.keys()) == {"since"}
    assert set(alice_knows_bob.values()) == {1999}
    assert set(alice_knows_bob.items()) == {("since", 1999)}
    assert alice_knows_bob.get("since") == 1999
    assert repr(alice_knows_bob)


def test_create_path():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
    path = Path(alice, alice_knows_bob, carol_dislikes_bob)

    assert path.start_node == alice
    assert path.end_node == carol
    assert path.nodes == (alice, bob, carol)
    assert path.relationships == (alice_knows_bob, carol_dislikes_bob)
    assert list(path) == [alice_knows_bob, carol_dislikes_bob]
    assert repr(path)


def test_hydrate_path():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    r = [gh.hydrate_unbound_relationship(1, "KNOWS", {"since": 1999}),
         gh.hydrate_unbound_relationship(2, "DISLIKES", {})]
    path = gh.hydrate_path([alice, bob, carol], r, [1, 1, -2, 2])

    assert path.start_node == alice
    assert path.end_node == carol
    assert path.nodes == (alice, bob, carol)

    expected_alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
    expected_carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})

    assert path.relationships == (expected_alice_knows_bob, expected_carol_dislikes_bob)
    assert list(path) == [expected_alice_knows_bob, expected_carol_dislikes_bob]
    assert repr(path)


def test_path_equality():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
    path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)

    assert path_1 == path_2
    assert path_1 != "this is not a path"


def test_path_hashing():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
    path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)

    assert hash(path_1) == hash(path_2)


def test_hydrate_node_structure():
    hydrant = DataHydrator()
    struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
    alice, = hydrant.hydrate([struct])

    assert isinstance(alice, Node)

    assert alice.id == 123
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"


def test_hydrating_unknown_structure_returns_same():
    hydrant = DataHydrator()
    struct = Structure(b'?', "foo")
    mystery, = hydrant.hydrate([struct])

    assert mystery == struct


def test_hydrate_node_in_list():
    hydrant = DataHydrator()
    struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
    alice_in_list, = hydrant.hydrate([[struct]])

    assert isinstance(alice_in_list, list)

    alice, = alice_in_list

    assert alice.id == 123
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"


def test_hydrate_node_in_dict():
    hydrant = DataHydrator()
    struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
    alice_in_dict, = hydrant.hydrate([{"foo": struct}])

    assert isinstance(alice_in_dict, dict)

    alice = alice_in_dict["foo"]

    assert alice.id == 123
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name"}
    assert alice.get("name") == "Alice"


def test_simple_node_data_method():
    g = Graph()
    gh = Graph.Hydrator(g)
    node = gh.hydrate_node(123, {"Test", "Node"}, {"name": "tester", "age": 33})

    data = node.data()

    assert isinstance(data, dict)
    assert data.get("id") == 123
    assert data.get("labels") == ["Node", "Test"]
    assert data.get("properties") == {"name": "tester", "age": 33}


def test_recursive_node_data_method():
    g = Graph()
    gh = Graph.Hydrator(g)
    node = gh.hydrate_node(123, {"Test", "Node"}, {
        "names": ["tester", "something"],
        "age": 33,
        "map": {"a": 1, "b": 2},
    })

    data = node.data()

    assert isinstance(data, dict)
    assert data.get("id") == 123
    assert data.get("labels") == ["Node", "Test"]
    assert data.get("properties") == {
        "names": ["tester", "something"],
        "age": 33,
        "map": {"a": 1, "b": 2},
    }


def test_temporal_spatial_node_data_method():
    g = Graph()
    gh = Graph.Hydrator(g)
    node = gh.hydrate_node(123, {"Test", "Node"}, {
        "temporal": [
            Date(1976, 6, 13),
            Time(12, 34, 56),
            DateTime(1976, 6, 13, 12, 34, 56),
            Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6.789)
        ],
        "spatial": [
            CartesianPoint((1.23, 4.56)),
            CartesianPoint((1.23, 4.56, 7.89)),
            WGS84Point((1.23, 4.56)),
            WGS84Point((1.23, 4.56, 7.89)),
        ],
    })

    data = node.data()

    assert isinstance(data, dict)
    assert data.get("id") == 123
    assert data.get("labels") == ["Node", "Test"]

    date, time, dtime, duration = data.get("properties").get("temporal")

    assert isinstance(date, datetime.date)
    assert isinstance(time, datetime.time)
    assert isinstance(dtime, datetime.datetime)
    assert isinstance(duration, dict)
    assert duration == {'days': 3, 'months': 14, 'seconds': 14706, 'subseconds': 0.789}

    p1, p2, p3, p4 = data.get("properties").get("spatial")

    assert p1 == {"srid": 7203, "x": 1.23, "y": 4.56}
    assert p2 == {"srid": 9157, "x": 1.23, "y": 4.56, "z": 7.89}
    assert p3 == {"srid": 4326, "longitude": 1.23, "latitude": 4.56}
    assert p4 == {"srid": 4979, "longitude": 1.23, "latitude": 4.56, "height": 7.89}

