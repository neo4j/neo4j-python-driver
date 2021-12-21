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


from itertools import product

import pytest

from neo4j.graph import (
    Graph,
    Node,
    Path,
    Relationship,
)


# python -m pytest -s -v tests/unit/test_types.py


# Node


def test_can_create_node():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(123, {"Person"}, {"name": "Alice", "age": 33})
    assert isinstance(alice, Node)
    assert alice.id == 123
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name", "age"}
    assert set(alice.values()) == {"Alice", 33}
    assert set(alice.items()) == {("name", "Alice"), ("age", 33)}
    assert dict(alice) == {"name": "Alice", "age": 33}
    assert alice.get("name") == "Alice"
    assert alice.get("age") == 33
    assert alice.get("unknown") is None
    assert alice.get("unknown", None) is None
    assert alice.get("unknown", False) is False
    assert alice.get("unknown", "default") == "default"
    assert len(alice) == 2
    assert alice["name"] == "Alice"
    assert alice["age"] == 33
    assert "name" in alice
    assert "age" in alice
    assert set(iter(alice)) == {"name", "age"}


def test_node_with_null_properties():
    g = Graph()
    gh = Graph.Hydrator(g)
    stuff = gh.hydrate_node(1, (), {"good": ["puppies", "kittens"],
                                    "bad": None})
    assert isinstance(stuff, Node)
    assert set(stuff.keys()) == {"good"}
    assert stuff.get("good") == ["puppies", "kittens"]
    assert stuff.get("bad") is None
    assert len(stuff) == 1
    assert stuff["good"] == ["puppies", "kittens"]
    assert stuff["bad"] is None
    assert "good" in stuff
    assert "bad" not in stuff


@pytest.mark.parametrize(("g1", "id1", "props1", "g2", "id2", "props2"), (
    (*n1, *n2)
    for n1, n2 in product(
        (
            (g, id_, props)
            for g in (0, 1)
            for id_ in (1, 1234)
            for props in (None, {}, {"a": 1})
        ),
        repeat=2
    )
))
def test_node_equality(g1, id1, props1, g2, id2, props2):
    graphs = (Graph(), Graph())
    node_1 = Node(graphs[g1], id1, props1)
    node_2 = Node(graphs[g2], id2, props2)
    if g1 == g2 and id1 == id2:
        assert node_1 == node_2
    else:
        assert node_1 != node_2
    assert node_1 != "this is not a node"


def test_node_hashing():
    g = Graph()
    node_1 = Node(g, 1234)
    node_2 = Node(g, 1234)
    node_3 = Node(g, 5678)
    assert hash(node_1) == hash(node_2)
    assert hash(node_1) != hash(node_3)


def test_node_repr():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"})
    assert repr(alice) == "<Node id=1 labels=frozenset({'Person'}) properties={'name': 'Alice'}>"


# Relationship


def test_can_create_relationship():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
    assert isinstance(alice_knows_bob, Relationship)
    assert alice_knows_bob.start_node == alice
    assert alice_knows_bob.type == "KNOWS"
    assert alice_knows_bob.end_node == bob
    assert dict(alice_knows_bob) == {"since": 1999}
    assert set(alice_knows_bob.keys()) == {"since"}
    assert set(alice_knows_bob.values()) == {1999}
    assert set(alice_knows_bob.items()) == {("since", 1999)}
    assert alice_knows_bob.get("since") == 1999


def test_relationship_repr():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob"})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
    assert repr(alice_knows_bob) == "<Relationship id=1 nodes=(<Node id=1 labels=frozenset({'Person'}) properties={'name': 'Alice'}>, <Node id=2 labels=frozenset({'Person'}) properties={'name': 'Bob'}>) type='KNOWS' properties={'since': 1999}>"


# Path


def test_can_create_path():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS",
                                              {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id,
                                                 "DISLIKES", {})
    path = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert isinstance(path, Path)
    assert path.start_node is alice
    assert path.end_node is carol
    assert path.nodes == (alice, bob, carol)
    assert path.relationships == (alice_knows_bob, carol_dislikes_bob)
    assert list(path) == [alice_knows_bob, carol_dislikes_bob]


@pytest.mark.parametrize("cyclic", (True, False))
def test_can_hydrate_path(cyclic):
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    if cyclic:
        carol = alice
    else:
        carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    r = [gh.hydrate_unbound_relationship(1, "KNOWS", {"since": 1999}),
         gh.hydrate_unbound_relationship(2, "DISLIKES", {})]
    path = gh.hydrate_path([alice, bob, carol], r, [1, 1, -2, 2])
    assert path.start_node is alice
    assert path.end_node is carol
    assert path.nodes == (alice, bob, carol)
    expected_alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id,
                                                       "KNOWS", {"since": 1999})
    expected_carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id,
                                                          "DISLIKES", {})
    assert path.relationships == (expected_alice_knows_bob,
                                  expected_carol_dislikes_bob)
    assert list(path) == [expected_alice_knows_bob, expected_carol_dislikes_bob]


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


def test_path_repr():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob"})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol"})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
    path = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert repr(path) == "<Path start=<Node id=1 labels=frozenset({'Person'}) properties={'name': 'Alice'}> end=<Node id=3 labels=frozenset({'Person'}) properties={'name': 'Carol'}> size=2>"
