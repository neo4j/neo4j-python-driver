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


from __future__ import print_function


from tests.integration.tools import IntegrationTestCase


class ReadmeTestCase(IntegrationTestCase):

    def test_should_run_readme(self):
        names = set()
        print = names.add

        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(self.bolt_uri, auth=self.auth)

        def print_friends(tx, name):
            for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) "
                                 "WHERE a.name = {name} "
                                 "RETURN friend.name", name=name):
                print(record["friend.name"])

        with driver.session() as session:
            session.run("MATCH (a) DETACH DELETE a")
            session.run("CREATE (a:Person {name:'Alice'})-[:KNOWS]->({name:'Bob'})")
            session.read_transaction(print_friends, "Alice")

        assert len(names) == 1
        assert "Bob" in names
