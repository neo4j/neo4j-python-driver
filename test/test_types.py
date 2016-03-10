#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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

from neo4j.v1.packstream import Structure

from neo4j.v1.types import Node, Relationship, UnboundRelationship, Path, hydrated


class NodeTestCase(TestCase):

    def test_can_create_node(self):
        alice = Node({"Person"}, {"name": "Alice", "age": 33})
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name", "age"}
        assert set(alice.values()) == {"Alice", 33}
        assert set(alice.items()) == {("name", "Alice"), ("age", 33)}
        assert alice.get("name") == "Alice"
        assert alice.get("age") == 33
        assert repr(alice)
        assert len(alice) == 2
        assert alice["name"] == "Alice"
        assert alice["age"] == 33
        assert "name" in alice
        assert "age" in alice
        assert set(iter(alice)) == {"name", "age"}

    def test_null_properties(self):
        stuff = Node(good=["puppies", "kittens"], bad=None)
        assert set(stuff.keys()) == {"good"}
        assert stuff.get("good") == ["puppies", "kittens"]
        assert stuff.get("bad") is None
        assert len(stuff) == 1
        assert stuff["good"] == ["puppies", "kittens"]
        assert stuff["bad"] is None
        assert "good" in stuff
        assert "bad" not in stuff

    def test_node_equality(self):
        node_1 = Node()
        node_1.identity = 1234
        node_2 = Node()
        node_2.identity = 1234
        node_3 = Node()
        node_3.identity = 5678
        assert node_1 == node_2
        assert node_1 != node_3
        assert node_1 != "this is not a node"

    def test_node_hashing(self):
        node_1 = Node()
        node_1.identity = 1234
        node_2 = Node()
        node_2.identity = 1234
        node_3 = Node()
        node_3.identity = 5678
        assert hash(node_1) == hash(node_2)
        assert hash(node_1) != hash(node_3)


class RelationshipTestCase(TestCase):

    def test_can_create_relationship(self):
        alice = Node.hydrate(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = Node.hydrate(2, {"Person"}, {"name": "Bob", "age": 44})
        alice_knows_bob = Relationship(alice.identity, bob.identity, "KNOWS", {"since": 1999})
        assert alice_knows_bob.start == alice.identity
        assert alice_knows_bob.type == "KNOWS"
        assert alice_knows_bob.end == bob.identity
        assert set(alice_knows_bob.keys()) == {"since"}
        assert set(alice_knows_bob.values()) == {1999}
        assert set(alice_knows_bob.items()) == {("since", 1999)}
        assert alice_knows_bob.get("since") == 1999
        assert repr(alice_knows_bob)


class UnboundRelationshipTestCase(TestCase):

    def test_can_create_unbound_relationship(self):
        alice_knows_bob = UnboundRelationship("KNOWS", {"since": 1999})
        assert alice_knows_bob.type == "KNOWS"
        assert set(alice_knows_bob.keys()) == {"since"}
        assert set(alice_knows_bob.values()) == {1999}
        assert set(alice_knows_bob.items()) == {("since", 1999)}
        assert alice_knows_bob.get("since") == 1999
        assert repr(alice_knows_bob)


class PathTestCase(TestCase):

    def test_can_create_path(self):
        alice = Node.hydrate(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = Node.hydrate(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = Node.hydrate(3, {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = Relationship(alice.identity, bob.identity, "KNOWS", {"since": 1999})
        carol_dislikes_bob = Relationship(carol.identity, bob.identity, "DISLIKES")
        path = Path(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
        assert path.start == alice
        assert path.end == carol
        assert path.nodes == (alice, bob, carol)
        assert path.relationships == (alice_knows_bob, carol_dislikes_bob)
        assert list(path) == [alice_knows_bob, carol_dislikes_bob]
        assert repr(path)

    def test_can_hydrate_path(self):
        alice = Node.hydrate(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = Node.hydrate(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = Node.hydrate(3, {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = Relationship(alice.identity, bob.identity, "KNOWS", {"since": 1999})
        carol_dislikes_bob = Relationship(carol.identity, bob.identity, "DISLIKES")
        path = Path.hydrate([alice, bob, carol],
                            [alice_knows_bob.unbind(), carol_dislikes_bob.unbind()],
                            [1, 1, -2, 2])
        assert path.start == alice
        assert path.end == carol
        assert path.nodes == (alice, bob, carol)
        assert path.relationships == (alice_knows_bob, carol_dislikes_bob)
        assert list(path) == [alice_knows_bob, carol_dislikes_bob]
        assert repr(path)

    def test_path_equality(self):
        alice = Node.hydrate(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = Node.hydrate(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = Node.hydrate(3, {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = Relationship(alice.identity, bob.identity, "KNOWS", {"since": 1999})
        carol_dislikes_bob = Relationship(carol.identity, bob.identity, "DISLIKES")
        path_1 = Path(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
        path_2 = Path(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
        assert path_1 == path_2
        assert path_1 != "this is not a path"

    def test_path_hashing(self):
        alice = Node.hydrate(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = Node.hydrate(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = Node.hydrate(3, {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = Relationship(alice.identity, bob.identity, "KNOWS", {"since": 1999})
        carol_dislikes_bob = Relationship(carol.identity, bob.identity, "DISLIKES")
        path_1 = Path(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
        path_2 = Path(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
        assert hash(path_1) == hash(path_2)


class HydrationTestCase(TestCase):

    def test_can_hydrate_node_structure(self):
        struct = Structure(3, b'N')
        struct.append(123)
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice = hydrated(struct)
        assert alice.identity == 123
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    def test_hydrating_unknown_structure_returns_same(self):
        struct = Structure(1, b'X')
        struct.append("foo")
        mystery = hydrated(struct)
        assert mystery == struct

    def test_can_hydrate_in_list(self):
        struct = Structure(3, b'N')
        struct.append(123)
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice_in_list = hydrated([struct])
        assert isinstance(alice_in_list, list)
        alice, = alice_in_list
        assert alice.identity == 123
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    def test_can_hydrate_in_dict(self):
        struct = Structure(3, b'N')
        struct.append(123)
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice_in_dict = hydrated({"foo": struct})
        assert isinstance(alice_in_dict, dict)
        alice = alice_in_dict["foo"]
        assert alice.identity == 123
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"
