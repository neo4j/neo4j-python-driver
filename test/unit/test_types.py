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


from unittest import TestCase

from neobolt.types import Structure, PackStreamHydrator
from neobolt.types.graph import Node, Path, Graph


class NodeTestCase(TestCase):

    def test_can_create_node(self):
        g = Graph()
        gh = Graph.Hydrator(g)
        alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
        self.assertEqual(alice.labels, {"Person"})
        self.assertEqual(set(alice.keys()), {"name", "age"})
        self.assertEqual(set(alice.values()), {"Alice", 33})
        self.assertEqual(set(alice.items()), {("name", "Alice"), ("age", 33)})
        self.assertEqual(alice.get("name"), "Alice")
        self.assertEqual(alice.get("age"), 33)
        self.assertTrue(repr(alice))
        self.assertEqual(len(alice), 2)
        self.assertEqual(alice["name"], "Alice")
        self.assertEqual(alice["age"], 33)
        self.assertIn("name", alice)
        self.assertIn("age", alice)
        self.assertEqual(set(iter(alice)), {"name", "age"})

    def test_null_properties(self):
        g = Graph()
        gh = Graph.Hydrator(g)
        stuff = gh.hydrate_node(1, (), {"good": ["puppies", "kittens"], "bad": None})
        self.assertEqual(set(stuff.keys()), {"good"})
        self.assertEqual(stuff.get("good"), ["puppies", "kittens"])
        self.assertIsNone(stuff.get("bad"))
        self.assertEqual(len(stuff), 1)
        self.assertEqual(stuff["good"], ["puppies", "kittens"])
        self.assertIsNone(stuff["bad"])
        self.assertIn("good", stuff)
        self.assertNotIn("bad", stuff)

    def test_node_equality(self):
        g = Graph()
        node_1 = Node(g, 1234)
        node_2 = Node(g, 1234)
        node_3 = Node(g, 5678)
        self.assertEqual(node_1, node_2)
        self.assertNotEqual(node_1, node_3)
        self.assertNotEqual(node_1, "this is not a node")

    def test_node_hashing(self):
        g = Graph()
        node_1 = Node(g, 1234)
        node_2 = Node(g, 1234)
        node_3 = Node(g, 5678)
        self.assertEqual(hash(node_1), hash(node_2))
        self.assertNotEqual(hash(node_1), hash(node_3))


class RelationshipTestCase(TestCase):

    def test_can_create_relationship(self):
        g = Graph()
        gh = Graph.Hydrator(g)
        alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
        alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
        self.assertEqual(alice_knows_bob.start_node, alice)
        self.assertEqual(alice_knows_bob.type, "KNOWS")
        self.assertEqual(alice_knows_bob.end_node, bob)
        self.assertEqual(set(alice_knows_bob.keys()), {"since"})
        self.assertEqual(set(alice_knows_bob.values()), {1999})
        self.assertEqual(set(alice_knows_bob.items()), {("since", 1999)})
        self.assertEqual(alice_knows_bob.get("since"), 1999)
        self.assertTrue(repr(alice_knows_bob))


class PathTestCase(TestCase):

    def test_can_create_path(self):
        g = Graph()
        gh = Graph.Hydrator(g)
        alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
        carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
        path = Path(alice, alice_knows_bob, carol_dislikes_bob)
        self.assertEqual(path.start_node, alice)
        self.assertEqual(path.end_node, carol)
        self.assertEqual(path.nodes, (alice, bob, carol))
        self.assertEqual(path.relationships, (alice_knows_bob, carol_dislikes_bob))
        self.assertEqual(list(path), [alice_knows_bob, carol_dislikes_bob])
        self.assertTrue(repr(path))

    def test_can_hydrate_path(self):
        g = Graph()
        gh = Graph.Hydrator(g)
        alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
        r = [gh.hydrate_unbound_relationship(1, "KNOWS", {"since": 1999}),
             gh.hydrate_unbound_relationship(2, "DISLIKES", {})]
        path = gh.hydrate_path([alice, bob, carol], r, [1, 1, -2, 2])
        self.assertEqual(path.start_node, alice)
        self.assertEqual(path.end_node, carol)
        self.assertEqual(path.nodes, (alice, bob, carol))
        expected_alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
        expected_carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
        self.assertEqual(path.relationships, (expected_alice_knows_bob, expected_carol_dislikes_bob))
        self.assertEqual(list(path), [expected_alice_knows_bob, expected_carol_dislikes_bob])
        self.assertTrue(repr(path))

    def test_path_equality(self):
        g = Graph()
        gh = Graph.Hydrator(g)
        alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
        carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
        path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
        path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)
        self.assertEqual(path_1, path_2)
        self.assertNotEqual(path_1, "this is not a path")

    def test_path_hashing(self):
        g = Graph()
        gh = Graph.Hydrator(g)
        alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
        bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
        carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
        alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS", {"since": 1999})
        carol_dislikes_bob = gh.hydrate_relationship(2, carol.id, bob.id, "DISLIKES", {})
        path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
        path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)
        self.assertEqual(hash(path_1), hash(path_2))


class HydrationTestCase(TestCase):

    def setUp(self):
        self.hydrant = PackStreamHydrator(1)

    def test_can_hydrate_node_structure(self):
        struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
        alice, = self.hydrant.hydrate([struct])
        self.assertEqual(alice.id, 123)
        self.assertEqual(alice.labels, {"Person"})
        self.assertEqual(set(alice.keys()), {"name"})
        self.assertEqual(alice.get("name"), "Alice")

    def test_hydrating_unknown_structure_returns_same(self):
        struct = Structure(b'?', "foo")
        mystery, = self.hydrant.hydrate([struct])
        self.assertEqual(mystery, struct)

    def test_can_hydrate_in_list(self):
        struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
        alice_in_list, = self.hydrant.hydrate([[struct]])
        self.assertIsInstance(alice_in_list, list)
        alice, = alice_in_list
        self.assertEqual(alice.id, 123)
        self.assertEqual(alice.labels, {"Person"})
        self.assertEqual(set(alice.keys()), {"name"})
        self.assertEqual(alice.get("name"), "Alice")

    def test_can_hydrate_in_dict(self):
        struct = Structure(b'N', 123, ["Person"], {"name": "Alice"})
        alice_in_dict, = self.hydrant.hydrate([{"foo": struct}])
        self.assertIsInstance(alice_in_dict, dict)
        alice = alice_in_dict["foo"]
        self.assertEqual(alice.id, 123)
        self.assertEqual(alice.labels, {"Person"})
        self.assertEqual(set(alice.keys()), {"name"})
        self.assertEqual(alice.get("name"), "Alice")


class TemporalHydrationTestCase(TestCase):

    def setUp(self):
        self.hydrant = PackStreamHydrator(2)

    def test_can_hydrate_date_time_structure(self):
        struct = Structure(b'd', 1539344261, 474716862)
        dt, = self.hydrant.hydrate([struct])
        self.assertEqual(dt.year, 2018)
        self.assertEqual(dt.month, 10)
        self.assertEqual(dt.day, 12)
        self.assertEqual(dt.hour, 11)
        self.assertEqual(dt.minute, 37)
        self.assertEqual(dt.second, 41.474716862)
