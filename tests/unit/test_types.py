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


from unittest import TestCase

from neo4j.data import DataHydrator
from neo4j.graph import (
    Node,
    Path,
    Graph,
)
from neo4j.packstream import Structure

# python -m pytest -s -v tests/unit/test_types.py


# Node


def test_can_create_node():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name", "age"}
    assert set(alice.values()) == {"Alice", 33}
    assert set(alice.items()) == {("name", "Alice"), ("age", 33)}
    assert alice.get("name") == "Alice"
    assert alice.get("age") == 33
    assert repr(alice) == "<Node id=1 labels=frozenset({'Person'}) properties={'age': 33, 'name': 'Alice'}>"
    assert len(alice) == 2
    assert alice["name"] == "Alice"
    assert alice["age"] == 33
    assert "name" in alice
    assert "age" in alice
    assert set(iter(alice)) == {"name", "age"}


def test_null_properties():
    g = Graph()
    gh = Graph.Hydrator(g)
    stuff = gh.hydrate_node(1, (), {"good": ["puppies", "kittens"], "bad": None})
    assert set(stuff.keys()) == {"good"}
    assert stuff.get("good") == ["puppies", "kittens"]
    assert stuff.get("bad") is None
    assert len(stuff) == 1
    assert stuff["good"] == ["puppies", "kittens"]
    assert stuff["bad"] is None
    assert "good" in stuff
    assert "bad" not in stuff


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


# Relationship


def test_can_create_relationship():
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
    assert repr(alice_knows_bob) == "<Relationship id=1 nodes=(<Node id=1 labels=frozenset({'Person'}) properties={'age': 33, 'name': 'Alice'}>, <Node id=2 labels=frozenset({'Person'}) properties={'age': 44, 'name': 'Bob'}>) type='KNOWS' properties={'since': 1999}>"


# Path


def test_can_create_path():
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
    assert repr(path) == "<Path start=<Node id=1 labels=frozenset({'Person'}) properties={'age': 33, 'name': 'Alice'}> end=<Node id=3 labels=frozenset({'Person'}) properties={'age': 55, 'name': 'Carol'}> size=2>"


def test_can_hydrate_path():
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
    assert repr(path) == "<Path start=<Node id=1 labels=frozenset({'Person'}) properties={'age': 33, 'name': 'Alice'}> end=<Node id=3 labels=frozenset({'Person'}) properties={'age': 55, 'name': 'Carol'}> size=2>"


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
