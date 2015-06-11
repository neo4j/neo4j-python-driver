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

from neo4j import Node, Relationship, Path


class RelationshipTestCase(TestCase):

    def test_can_create_relationship(self):
        alice = Node("A", {"Person"}, {"name": "Alice", "age": 33})
        bob = Node("B", {"Person"}, {"name": "Bob", "age": 44})
        alice_knows_bob = Relationship("AB", alice, bob, "KNOWS", {"since": 1999})
        assert alice_knows_bob.identity() == "AB"
        assert alice_knows_bob.start() is alice
        assert alice_knows_bob.type() == "KNOWS"
        assert alice_knows_bob.end() is bob
        assert alice_knows_bob.property_keys() == {"since"}
        assert alice_knows_bob.property("since") == 1999


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


if __name__ == "__main__":
    main()
