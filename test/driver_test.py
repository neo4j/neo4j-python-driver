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

from neo4j import GraphDatabase, Node, Relationship, Path


class RunTestCase(TestCase):

    def test_can_run_simple_statement(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        count = 0
        for record in session.run("RETURN 1"):
            assert record[0] == 1
            count += 1
        session.close()
        assert count == 1

    def test_can_run_statement_that_returns_multiple_records(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        count = 0
        for record in session.run("unwind(range(1, 10)) AS z RETURN z"):
            assert 1 <= record[0] <= 10
            count += 1
        session.close()
        assert count == 10

    def test_can_use_with_to_auto_close_session(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            records = session.run("RETURN 1")
            assert len(records) == 1
            for record in records:
                assert record[0] == 1

    def test_can_return_node(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            records = session.run("MERGE (a:Person {name:'Alice'}) RETURN a")
            assert len(records) == 1
            for record in records:
                alice = record[0]
                assert isinstance(alice, Node)
                assert alice.labels == {"Person"}
                assert alice.properties == {"name": "Alice"}

    def test_can_return_relationship(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            records = session.run("MERGE ()-[r:KNOWS {since:1999}]->() RETURN r")
            assert len(records) == 1
            for record in records:
                rel = record[0]
                assert isinstance(rel, Relationship)
                assert rel.type == "KNOWS"
                assert rel.properties == {"since": 1999}

    def test_can_return_path(self):
        with GraphDatabase.driver("bolt://localhost").session() as session:
            records = session.run("MERGE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'}) RETURN p")
            assert len(records) == 1
            for record in records:
                path = record[0]
                assert isinstance(path, Path)
                assert path.start.properties == {"name": "Alice"}
                assert path.end.properties == {"name": "Bob"}
                assert path.relationships[0].type == "KNOWS"
                assert len(path.nodes) == 2
                assert len(path.relationships) == 1


if __name__ == "__main__":
    main()
