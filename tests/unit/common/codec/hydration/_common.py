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


from datetime import (
    date,
    datetime,
    time,
    timedelta,
)

import pytest

from neo4j._codec.hydration import HydrationScope
from neo4j._codec.hydration.v1 import HydrationHandler as HydrationHandlerV1
from neo4j._codec.hydration.v2 import HydrationHandler as HydrationHandlerV2
from neo4j._codec.packstream import Structure
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


class HydrationHandlerTestBase:
    @pytest.fixture(params=[HydrationHandlerV1, HydrationHandlerV2])
    def hydration_handler(self, request):
        return request.param()

    def test_handler_hydration_scope(self, hydration_handler):
        scope = hydration_handler.new_hydration_scope()
        assert isinstance(scope, HydrationScope)

    @pytest.fixture
    def hydration_scope(self, hydration_handler):
        return hydration_handler.new_hydration_scope()

    def test_scope_hydration_keys(self, hydration_scope):
        hooks = hydration_scope.hydration_hooks
        assert isinstance(hooks, dict)
        assert set(hooks.keys()) == {Structure}

    def test_scope_dehydration_keys(self, hydration_scope):
        hooks = hydration_scope.dehydration_hooks
        assert isinstance(hooks, dict)
        assert set(hooks.keys()) == {
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
        }
