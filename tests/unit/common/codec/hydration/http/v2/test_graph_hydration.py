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

import typing as t

import pytest

from neo4j._codec.hydration.http.v2 import HydrationHandler
from neo4j.graph import (
    Node,
    Path,
    Relationship,
)

from .._base import HydrationHandlerTestBase


if t.TYPE_CHECKING:
    from neo4j._codec.hydration.http._common import HydrationScopeHttp


class TestGraphHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandler:
        return HydrationHandler()

    def test_can_hydrate_node(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "Node",
            "_value": {
                "_element_id": "123",
                "_labels": ["Person"],
                "_properties": {
                    "name": {
                        "$type": "String",
                        "_value": "Alice",
                    },
                },
            },
        }
        alice = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(alice, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert alice.id == 0
        assert alice.element_id == "123"
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    def test_can_hydrate_relationship(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "Relationship",
            "_value": {
                "_element_id": "123",
                "_start_node_element_id": "456",
                "_end_node_element_id": "789",
                "_type": "KNOWS",
                "_properties": {
                    "since": {
                        "$type": "Integer",
                        "_value": "1999",
                    },
                },
            },
        }
        rel = hydration_scope.hydration_hooks[type(encoded)](encoded)

        self.assert_is_hydrated_type(rel, Relationship)
        assert isinstance(rel.start_node, Node)
        assert isinstance(rel.end_node, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.id == 0
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.start_node.id == 0
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.end_node.id == 0
        assert rel.element_id == "123"
        assert rel.start_node.element_id == "456"
        assert rel.end_node.element_id == "789"
        assert rel.type == "KNOWS"
        assert set(rel.keys()) == {"since"}
        assert rel.get("since") == 1999

    def test_can_hydrate_path(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "Path",
            "_value": [
                {
                    "$type": "Node",
                    "_value": {
                        "_element_id": "e1",
                        "_labels": ["Person"],
                        "_properties": {
                            "name": {"$type": "String", "_value": "Alice"},
                            "age": {"$type": "Integer", "_value": "42"},
                        },
                    },
                },
                {
                    "$type": "Relationship",
                    "_value": {
                        "_element_id": "e2",
                        "_start_node_element_id": "e1",
                        "_end_node_element_id": "e3",
                        "_type": "LIKES",
                        "_properties": {
                            "since": {"$type": "String", "_value": "forever!"}
                        },
                    },
                },
                {
                    "$type": "Node",
                    "_value": {
                        "_element_id": "e3",
                        "_labels": ["Person"],
                        "_properties": {
                            "name": {"$type": "String", "_value": "Bob"}
                        },
                    },
                },
                {
                    "$type": "Relationship",
                    "_value": {
                        "_element_id": "e4",
                        "_start_node_element_id": "e1",
                        "_end_node_element_id": "e3",
                        "_type": "LIKES",
                        "_properties": {
                            "since": {"$type": "String", "_value": "forever!"}
                        },
                    },
                },
                {
                    "$type": "Node",
                    "_value": {
                        "_element_id": "e1",
                        "_labels": ["Person"],
                        "_properties": {
                            "name": {"$type": "String", "_value": "Alice"},
                            "age": {"$type": "Integer", "_value": "42"},
                        },
                    },
                },
            ],
        }
        path = hydration_scope.hydration_hooks[type(encoded)](encoded)

        self.assert_is_hydrated_type(path, Path)
        start_node = path.start_node
        assert isinstance(start_node, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert start_node.id == 0
        assert start_node.element_id == "e1"
        assert start_node.labels == {"Person"}
        assert set(start_node.keys()) == {"name", "age"}
        assert start_node.get("name") == "Alice"
        assert start_node.get("age") == 42
        alice = start_node

        nodes = path.nodes
        assert isinstance(nodes, tuple)
        assert len(nodes) == 3
        assert nodes[0] is alice
        assert nodes[2] is alice
        bob = nodes[1]
        assert isinstance(bob, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert bob.id == 0
        assert bob.element_id == "e3"
        assert bob.labels == {"Person"}
        assert set(bob.keys()) == {"name"}
        assert bob.get("name") == "Bob"

        relationships = path.relationships
        assert isinstance(relationships, tuple)
        assert len(relationships) == 2

        rel1 = relationships[0]
        assert isinstance(rel1, Relationship)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel1.id == 0
        assert rel1.element_id == "e2"
        assert rel1.start_node is alice
        assert rel1.end_node is bob
        assert rel1.type == "LIKES"
        assert set(rel1.keys()) == {"since"}
        assert rel1.get("since") == "forever!"

        rel2 = relationships[1]
        assert isinstance(rel2, Relationship)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel2.id == 0
        assert rel2.element_id == "e4"
        assert rel2.start_node is alice
        assert rel2.end_node is bob
        assert rel2.type == "LIKES"
        assert set(rel2.keys()) == {"since"}
        assert rel2.get("since") == "forever!"

        assert path.end_node is alice
