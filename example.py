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

# Test PR build

from neo4j.v1.session import GraphDatabase, basic_auth


driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

session.run("MERGE (a:Person {name:'Alice'})")

friends = ["Bob", "Carol", "Dave", "Eve", "Frank"]
with session.begin_transaction() as tx:
    for friend in friends:
        tx.run("MATCH (a:Person {name:'Alice'}) "
               "MERGE (a)-[:KNOWS]->(x:Person {name:{n}})", {"n": friend})
    tx.success = True

for friend, in session.run("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(x) RETURN x"):
    print('Alice says, "hello, %s"' % friend["name"])

session.close()
