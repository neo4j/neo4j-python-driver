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
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)

import pytest

from neo4j._codec.hydration import (
    DehydrationHooks,
    HydrationScope,
)
from neo4j._codec.hydration.http import (
    LiteralJson,
    LiteralJsonRecursive,
)
from neo4j._codec.hydration.http._common import HydrationScopeHttp
from neo4j._codec.hydration.http.v2 import HydrationHandler
from neo4j.graph import Graph
from neo4j.spatial import (
    CartesianPoint,
    Point,
    WGS84Point,
)
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)
from neo4j.vector import Vector

from ......._optional_deps import (
    HAS_NP,
    HAS_PD,
    np,
    pd,
)
from .._base import HydrationHandlerTestBase


class Id:
    value: t.Any

    def __init__(self, value) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Id):
            return NotImplemented
        return self.value is other.value

    def __hash__(self) -> int:
        return hash(id(self.value))

    def __repr__(self) -> str:
        return f"Id({self.value})"


class TestHydrationHandler(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandler:
        return HydrationHandler()

    def test_handler_hydration_scope(
        self, hydration_handler: HydrationHandler
    ) -> None:
        scope: HydrationScopeHttp = hydration_handler.new_hydration_scope()
        assert isinstance(scope, HydrationScope)
        assert isinstance(scope, HydrationScopeHttp)

    @pytest.fixture
    def hydration_scope(
        self, hydration_handler: HydrationHandler
    ) -> HydrationScopeHttp:
        return hydration_handler.new_hydration_scope()

    def test_scope_hydration_keys(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        hooks = hydration_scope.hydration_hooks
        assert isinstance(hooks, dict)
        assert set(hooks.keys()) == {list, dict}

    def test_scope_dehydration_exact_values_keys(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        hooks = hydration_scope.dehydration_hooks
        assert isinstance(hooks, DehydrationHooks)

        expected_exact_values = {
            Id(None),
            Id(True),
            Id(False),
        }

        if HAS_NP:
            expected_exact_values.update(
                {
                    Id(np.True_),
                    Id(np.False_),
                }
            )

        if HAS_PD:
            expected_exact_values.update(
                {
                    Id(pd.NA),
                    Id(pd.NaT),
                }
            )
        exact_values = {Id(v._value) for v in hooks.exact_values}
        assert exact_values == expected_exact_values

    def test_scope_dehydration_exact_types_keys(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        hooks = hydration_scope.dehydration_hooks
        assert isinstance(hooks, DehydrationHooks)

        expected_exact_types: set[type] = {
            int,
            float,
            str,
            list,
            tuple,
            dict,
            bytes,
            bytearray,
            date,
            datetime,
            time,
            timedelta,
            Date,
            DateTime,
            Duration,
            Time,
            CartesianPoint,
            Point,
            WGS84Point,
            Vector,
            LiteralJson,
            LiteralJsonRecursive,
        }
        if HAS_NP:
            expected_exact_types.update(
                {
                    np.ndarray,
                    np.datetime64,
                    np.timedelta64,
                }
            )
        if HAS_PD:
            expected_exact_types.update(
                {
                    pd.Series,
                    pd.Categorical,
                    pd.api.extensions.ExtensionArray,
                    pd.DataFrame,
                    pd.Timestamp,
                    pd.Timedelta,
                    type(pd.NaT),
                }
            )
        assert set(hooks.exact_types.keys()) == expected_exact_types

    def test_scope_dehydration_subtypes_keys(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        hooks = hydration_scope.dehydration_hooks
        assert isinstance(hooks, DehydrationHooks)

        expected_subtypes: set[type] = {
            int,
            float,
            str,
            list,
            tuple,
            dict,
            bytes,
            bytearray,
            object,
        }
        if HAS_NP:
            expected_subtypes.update(
                {
                    np.integer,
                    np.floating,
                    np.ndarray,
                }
            )
        if HAS_PD:
            expected_subtypes.update(
                {
                    pd.Series,
                    pd.Categorical,
                    pd.api.extensions.ExtensionArray,
                    pd.DataFrame,
                }
            )
        assert set(hooks.subtypes.keys()) == expected_subtypes

    def test_scope_get_graph(
        self, hydration_scope: HydrationScopeHttp
    ) -> None:
        graph = hydration_scope.get_graph()
        assert isinstance(graph, Graph)
        assert not graph.nodes
        assert not graph.relationships
