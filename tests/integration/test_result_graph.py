#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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


import pytest

from neo4j.graph import (
    Node,
    Relationship,
    Graph,
    Path,
)
from neo4j.exceptions import Neo4jError


def test_result_graph_instance(session):
    # python -m pytest tests/integration/test_result_graph.py -s -v -k test_result_graph_instance
    result = session.run("RETURN 1")
    graph = result.graph()

    assert isinstance(graph, Graph)


def test_result_graph_case_1(session):
    # python -m pytest tests/integration/test_result_graph.py -s -v -k test_result_graph_case_1
    result = session.run("CREATE (n1:Person:LabelTest1 {name:'Alice'})-[r1:KNOWS {since:1999}]->(n2:Person:LabelTest2 {name:'Bob'}) RETURN n1, r1, n2")
    graph = result.graph()
    assert isinstance(graph, Graph)

    node_view = graph.nodes
    relationships_view = graph.relationships

    for node in node_view:
        name = node["name"]
        if name == "Alice":
            assert node.labels == frozenset(["Person", "LabelTest1"])
        elif name == "Bob":
            assert node.labels == frozenset(["Person", "LabelTest2"])
        else:
            pytest.fail("should only contain 2 nodes, Alice and Bob. {}".format(name))

    for relationship in relationships_view:
        since = relationship["since"]
        assert since == 1999
        assert relationship.type == "KNOWS"
