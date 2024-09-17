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


import pytest

from neo4j._codec.hydration.v1 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.graph import (
    Node,
    Relationship,
)

from ._base import HydrationHandlerTestBase


class TestGraphHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    def test_can_hydrate_node_structure(self, hydration_scope):
        struct = Structure(b"N", 123, ["Person"], {"name": "Alice"})
        alice = hydration_scope.hydration_hooks[Structure](struct)

        assert isinstance(alice, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert alice.id == 123
        # for backwards compatibility, the driver should compute the element_id
        assert alice.element_id == "123"
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    def test_can_hydrate_relationship_structure(self, hydration_scope):
        struct = Structure(b"R", 123, 456, 789, "KNOWS", {"since": 1999})
        rel = hydration_scope.hydration_hooks[Structure](struct)

        assert isinstance(rel, Relationship)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.id == 123
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.start_node.id == 456
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.end_node.id == 789
        # for backwards compatibility, the driver should compute the element_id
        assert rel.element_id == "123"
        assert rel.start_node.element_id == "456"
        assert rel.end_node.element_id == "789"
        assert rel.type == "KNOWS"
        assert set(rel.keys()) == {"since"}
        assert rel.get("since") == 1999
