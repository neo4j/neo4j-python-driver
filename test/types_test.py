#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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


from unittest import main, TestCase

from neo4j.typesystem import Node, Relationship, Path, hydrated
from neo4j.packstream import Structure


class NodeTestCase(TestCase):

    def test_can_create_node(self):
        alice = Node("A", {"Person"}, {"name": "Alice", "age": 33})
        assert alice.identity() == "A"
        assert alice.labels() == {"Person"}
        assert alice.keys() == {"name", "age"}
        assert alice.get("name") == "Alice"
        assert alice.get("age") == 33
        assert repr(alice)


class RelationshipTestCase(TestCase):

    def test_can_create_relationship(self):
        alice = Node("A", {"Person"}, {"name": "Alice", "age": 33})
        bob = Node("B", {"Person"}, {"name": "Bob", "age": 44})
        alice_knows_bob = Relationship("AB", alice, bob, "KNOWS", {"since": 1999})
        assert alice_knows_bob.identity() == "AB"
        assert alice_knows_bob.start() is alice
        assert alice_knows_bob.type() == "KNOWS"
        assert alice_knows_bob.end() is bob
        assert alice_knows_bob.keys() == {"since"}
        assert alice_knows_bob.get("since") == 1999
        assert repr(alice_knows_bob)


class PathTestCase(TestCase):

    def test_can_create_path(self):
        alice = Node("A", {"Person"}, {"name": "Alice", "age": 33})
        bob = Node("B", {"Person"}, {"name": "Bob", "age": 44})
        carol = Node("C", {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = Relationship("AB", alice, bob, "KNOWS", {"since": 1999})
        carol_knows_bob = Relationship("CB", carol, bob, "KNOWS", {"since": 2001})
        path = Path([alice, alice_knows_bob, bob, carol_knows_bob, carol])
        assert path.start() == alice
        assert path.end() == carol
        assert path.nodes() == (alice, bob, carol)
        assert path.relationships() == (alice_knows_bob, carol_knows_bob)
        assert list(path) == [alice_knows_bob, carol_knows_bob]
        assert repr(path)


class HydrationTestCase(TestCase):

    def test_can_hydrate_node_structure(self):
        struct = Structure(3, b'N')
        struct.append("node/123")
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice = hydrated(struct)
        assert alice.identity() == "node/123"
        assert alice.labels() == {"Person"}
        assert alice.keys() == {"name"}
        assert alice.get("name") == "Alice"

    def test_hydrating_unknown_structure_returns_same(self):
        struct = Structure(1, b'X')
        struct.append("foo")
        mystery = hydrated(struct)
        assert mystery == struct

    def test_can_hydrate_in_list(self):
        struct = Structure(3, b'N')
        struct.append("node/123")
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice_in_list = hydrated([struct])
        assert isinstance(alice_in_list, list)
        alice, = alice_in_list
        assert alice.identity() == "node/123"
        assert alice.labels() == {"Person"}
        assert alice.keys() == {"name"}
        assert alice.get("name") == "Alice"

    def test_can_hydrate_in_dict(self):
        struct = Structure(3, b'N')
        struct.append("node/123")
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice_in_dict = hydrated({"foo": struct})
        assert isinstance(alice_in_dict, dict)
        alice = alice_in_dict["foo"]
        assert alice.identity() == "node/123"
        assert alice.labels() == {"Person"}
        assert alice.keys() == {"name"}
        assert alice.get("name") == "Alice"


if __name__ == "__main__":
    main()
