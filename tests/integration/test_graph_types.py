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


from neo4j.graph import Node, Relationship, Path


def test_node(cypher_eval):
    a = cypher_eval("CREATE (a:Person {name:'Alice'}) "
                    "RETURN a")
    assert isinstance(a, Node)
    assert set(a.labels) == {"Person"}
    assert dict(a) == {"name": "Alice"}


def test_relationship(cypher_eval):
    a, b, r = cypher_eval("CREATE (a)-[r:KNOWS {since:1999}]->(b) "
                          "RETURN [a, b, r]")
    assert isinstance(r, Relationship)
    assert r.type == "KNOWS"
    assert dict(r) == {"since": 1999}
    assert r.start_node == a
    assert r.end_node == b


def test_path(cypher_eval):
    a, b, c, ab, bc, p = cypher_eval("CREATE p=(a)-[ab:X]->(b)-[bc:X]->(c) "
                                     "RETURN [a, b, c, ab, bc, p]")
    assert isinstance(p, Path)
    assert len(p) == 2
    assert p.nodes == (a, b, c)
    assert p.relationships == (ab, bc)
    assert p.start_node == a
    assert p.end_node == c
