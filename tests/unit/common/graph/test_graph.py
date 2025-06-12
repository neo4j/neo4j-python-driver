# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import copy
import pickle
from itertools import zip_longest

import pytest

from neo4j import _typing as t
from neo4j._codec.hydration.v1.hydration_handler import _GraphHydrator
from neo4j.graph import Path


if t.TYPE_CHECKING:
    from neo4j.graph import (
        Graph,
        Node,
        Relationship,
    )


class GraphBuilder:
    def __init__(self) -> None:
        self._hydrator = _GraphHydrator()
        self._node_counter = 0
        self._relationship_counter = 0

    def with_node(self, *labels, **properties) -> t.Self:
        id_ = self._node_counter
        element_id = f"e{id_}"
        self._node_counter += 1
        self._hydrator.hydrate_node(id_, labels, properties, element_id)
        return self

    def with_relationship(
        self, start_node_id, end_node_id, type_, **properties
    ) -> t.Self:
        id_ = self._relationship_counter
        element_id = f"e{id_}"
        start_node_element_id = f"e{start_node_id}"
        end_node_element_id = f"e{end_node_id}"
        self._relationship_counter += 1
        assert start_node_element_id in self._hydrator.graph.nodes
        assert end_node_element_id in self._hydrator.graph.nodes

        self._hydrator.hydrate_relationship(
            id_,
            start_node_id,
            end_node_id,
            type_,
            properties,
            element_id,
            start_node_element_id,
            end_node_element_id,
        )
        return self

    def build(self) -> Graph:
        return self._hydrator.graph


GRAPH = (
    GraphBuilder()
    .with_node("Person", name="Alice")
    .with_node("Person", name="Bob")
    .with_node("Person", "Missed", name="Cas")
    .with_relationship(0, 1, "KNOWS", since=1999)
    .with_relationship(1, 2, "KNOWS", since=2001)
    .with_relationship(2, 0, "FOLLOWS", since=2005)
    .build()
)


def assert_graph_clone(graph1: Graph, graph2: Graph):
    assert graph1 is not graph2
    assert graph1 != graph2
    assert_graph_equality(graph1, graph2)

    for node_id in all_node_ids(graph1, graph2):
        node1 = graph1.nodes[node_id]
        node2 = graph2.nodes[node_id]
        assert_node_clone(node1, node2)

    for rel_id in all_relationship_ids(graph1, graph2):
        rel1 = graph1.relationships[rel_id]
        rel2 = graph2.relationships[rel_id]
        assert_relationship_clone(rel1, rel2)


def assert_graph_copy(graph1: Graph, graph2: Graph):
    assert graph1 is not graph2
    assert graph1 != graph2
    assert_graph_equality(graph1, graph2)

    for node_id in all_node_ids(graph1, graph2):
        node1 = graph1.nodes[node_id]
        node2 = graph2.nodes[node_id]
        assert node1 is node2

    for rel_id in all_relationship_ids(graph1, graph2):
        rel1 = graph1.relationships[rel_id]
        rel2 = graph2.relationships[rel_id]
        assert rel1 is rel2


def assert_graph_equality(graph1: Graph, graph2: Graph):
    assert len(graph1.nodes) == len(graph2.nodes)
    for node1 in graph1.nodes:
        node2 = graph2.nodes[node1.element_id]
        assert_node_equality(node1, node2)

    assert len(graph1.relationships) == len(graph2.relationships)
    for rel1 in graph1.relationships:
        rel2 = graph2.relationships[rel1.element_id]
        assert_relationship_equality(rel1, rel2)


def assert_node_clone(node1: Node, node2: Node):
    assert node1 is not node2
    assert node1 != node2
    assert_node_equality(node1, node2)


def assert_node_copy(node1: Node, node2: Node):
    assert node1 is not node2
    assert node1 == node2
    assert_node_equality(node1, node2)


def assert_node_equality(node1: Node, node2: Node):
    assert node1.labels == node2.labels
    assert node1.items() == node2.items()
    with pytest.warns(DeprecationWarning, match="element_id"):
        assert node1.id == node2.id
    assert node1.element_id == node2.element_id


def assert_relationship_clone(rel1: Relationship, rel2: Relationship):
    assert rel1 is not rel2
    assert rel1 != rel2
    assert type(rel1) is not type(rel2)
    assert_relationship_equality(rel1, rel2)


def assert_relationship_copy(rel1: Relationship, rel2: Relationship):
    assert rel1 is not rel2
    assert rel1 == rel2
    assert type(rel1) is type(rel2)
    assert_relationship_equality(rel1, rel2)


def assert_relationship_equality(rel1: Relationship, rel2: Relationship):
    assert rel1.type == rel2.type
    assert rel1.items() == rel2.items()

    if rel1.start_node is None:
        assert rel2.start_node is None
    else:
        assert rel2.start_node is not None
        assert rel1.start_node.element_id == rel2.start_node.element_id
    if rel1.end_node is None:
        assert rel2.end_node is None
    else:
        assert rel2.end_node is not None
        assert rel1.end_node.element_id == rel2.end_node.element_id


def assert_path_clone(path1: Path, path2: Path):
    assert path1 is not path2
    assert path1 != path2

    assert path1.graph is not path2.graph
    assert path1.nodes is not path2.nodes
    assert path1.relationships is not path2.relationships
    assert_path_equality(path1, path2)


def assert_path_copy(path1: Path, path2: Path):
    assert path1 is not path2
    assert path1 == path2

    assert path1.graph is path2.graph
    assert path1.nodes is path2.nodes
    assert path1.relationships is path2.relationships


def assert_path_equality(path1: Path, path2: Path):
    for node1, node2 in zip_longest(path1.nodes, path2.nodes):
        assert_node_equality(node1, node2)
    for rel1, rel2 in zip_longest(path1.relationships, path2.relationships):
        assert_relationship_equality(rel1, rel2)


def all_node_ids(*graphs: Graph) -> set[str]:
    return {node.element_id for graph in graphs for node in graph.nodes}


def all_relationship_ids(*graphs: Graph) -> set[str]:
    return {
        node.element_id for graph in graphs for node in graph.relationships
    }


def test_pickle_graph():
    graph1 = GRAPH
    graph2 = pickle.loads(pickle.dumps(graph1))

    assert_graph_clone(graph1, graph2)


@pytest.mark.parametrize("id_", ("e0", "e2"))
def test_pickle_node(id_):
    node1 = GRAPH.nodes[id_]
    node2 = pickle.loads(pickle.dumps(node1))

    assert_node_clone(node1, node2)

    graph1 = node1.graph
    graph2 = node2.graph
    assert_graph_clone(graph1, graph2)


@pytest.mark.parametrize("id_", ("e0", "e2"))
def test_pickle_relationship(id_):
    rel1 = GRAPH.relationships[id_]
    rel2 = pickle.loads(pickle.dumps(rel1))

    assert_relationship_clone(rel1, rel2)

    graph1 = rel1.graph
    graph2 = rel2.graph
    assert_graph_clone(graph1, graph2)


def test_pickle_path():
    path1 = Path(
        GRAPH.nodes["e0"],
        GRAPH.relationships["e0"],
        GRAPH.relationships["e1"],
        GRAPH.relationships["e2"],
    )
    path2 = pickle.loads(pickle.dumps(path1))

    assert_path_clone(path1, path2)

    graph1 = path1.graph
    graph2 = path2.graph
    assert_graph_clone(graph1, graph2)


def test_deepcopy_graph():
    graph1 = GRAPH
    graph2 = copy.deepcopy(graph1)

    assert_graph_clone(graph1, graph2)


@pytest.mark.parametrize("id_", ("e0", "e2"))
def test_deepcopy_node(id_):
    node1 = GRAPH.nodes[id_]
    node2 = copy.deepcopy(node1)

    assert_node_clone(node1, node2)

    graph1 = node1.graph
    graph2 = node2.graph
    assert_graph_clone(graph1, graph2)


@pytest.mark.parametrize("id_", ("e0", "e2"))
def test_deepcopy_relationship(id_):
    rel1 = GRAPH.relationships[id_]
    rel2 = copy.deepcopy(rel1)

    assert_relationship_clone(rel1, rel2)

    graph1 = rel1.graph
    graph2 = rel2.graph
    assert_graph_clone(graph1, graph2)


def test_deepcopy_path():
    path1 = Path(
        GRAPH.nodes["e0"],
        GRAPH.relationships["e0"],
        GRAPH.relationships["e1"],
        GRAPH.relationships["e2"],
    )
    path2 = copy.deepcopy(path1)

    assert_path_clone(path1, path2)

    graph1 = path1.graph
    graph2 = path2.graph
    assert_graph_clone(graph1, graph2)


def test_copy_graph():
    graph1 = GRAPH
    graph2 = copy.copy(graph1)

    assert_graph_copy(graph1, graph2)
    for node_id in all_node_ids(graph1, graph2):
        node1 = graph1.nodes[node_id]
        node2 = graph2.nodes[node_id]
        assert node1.graph is graph1
        assert node2.graph is graph1


@pytest.mark.parametrize("id_", ("e0", "e2"))
def test_copy_node(id_):
    node1 = GRAPH.nodes[id_]
    node2 = copy.copy(node1)

    assert_node_copy(node1, node2)

    graph1 = node1.graph
    graph2 = node2.graph
    assert graph1 is graph2


@pytest.mark.parametrize("id_", ("e0", "e2"))
def test_copy_relationship(id_):
    rel1 = GRAPH.relationships[id_]
    rel2 = copy.copy(rel1)

    assert_relationship_copy(rel1, rel2)

    graph1 = rel1.graph
    graph2 = rel2.graph
    assert graph1 is graph2


def test_copy_path():
    path1 = Path(
        GRAPH.nodes["e0"],
        GRAPH.relationships["e0"],
        GRAPH.relationships["e1"],
        GRAPH.relationships["e2"],
    )
    path2 = copy.copy(path1)

    assert_path_copy(path1, path2)

    graph1 = path1.graph
    graph2 = path2.graph
    assert graph1 is graph2


def test_node_id_access() -> None:
    assert 0 not in GRAPH.nodes
    with pytest.raises(KeyError):
        _ = GRAPH.nodes[0]  # type: ignore # expected to fail


def test_node_element_id_access() -> None:
    assert "e0" in GRAPH.nodes
    _ = GRAPH.nodes["e0"]


def test_relationship_id_access() -> None:
    assert 0 not in GRAPH.relationships
    with pytest.raises(KeyError):
        _ = GRAPH.relationships[0]  # type: ignore # expected to fail


def test_relationship_element_id_access() -> None:
    assert "e0" in GRAPH.relationships
    _ = GRAPH.relationships["e0"]
