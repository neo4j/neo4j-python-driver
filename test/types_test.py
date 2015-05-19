#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


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
        path = Path(alice, [alice_knows_bob, carol_knows_bob])
        assert path.start() == alice
        assert path.end() == carol
        assert path.nodes() == [alice, bob, carol]
        assert path.relationships() == [alice_knows_bob, carol_knows_bob]


if __name__ == "__main__":
    main()
