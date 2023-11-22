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
from neo4j.spatial import (
    CartesianPoint,
    Point,
    WGS84Point,
)

from ._base import HydrationHandlerTestBase


class TestSpatialDehydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    @pytest.fixture
    def transformer(self, hydration_scope):
        def transformer(value):
            transformer_ = \
                hydration_scope.dehydration_hooks.get_transformer(value)
            assert callable(transformer_)
            return transformer_(value)
        return transformer

    def test_cartesian_2d(self, transformer):
        point = CartesianPoint((1, 3.1))
        struct = transformer(point)
        assert struct == Structure(b"X", 7203, 1.0, 3.1)
        assert all(isinstance(f, float) for f in struct.fields[1:])

    def test_cartesian_3d(self, transformer):
        point = CartesianPoint((1, -2, 3.1))
        struct = transformer(point)
        assert struct == Structure(b"Y", 9157, 1.0, -2.0, 3.1)
        assert all(isinstance(f, float) for f in struct.fields[1:])

    def test_wgs84_2d(self, transformer):
        point = WGS84Point((1, 3.1))
        struct = transformer(point)
        assert struct == Structure(b"X", 4326, 1.0, 3.1)
        assert all(isinstance(f, float) for f in struct.fields[1:])

    def test_wgs84_3d(self, transformer):
        point = WGS84Point((1, -2, 3.1))
        struct = transformer(point)
        assert struct == Structure(b"Y", 4979, 1.0, -2.0, 3.1)
        assert all(isinstance(f, float) for f in struct.fields[1:])

    def test_custom_point_2d(self, transformer):
        point = Point((1, 3.1))
        point.srid = 12345
        struct = transformer(point)
        assert struct == Structure(b"X", 12345, 1.0, 3.1)
        assert all(isinstance(f, float) for f in struct.fields[1:])

    def test_custom_point_3d(self, transformer):
        point = Point((1, -2, 3.1))
        point.srid = 12345
        struct = transformer(point)
        assert struct == Structure(b"Y", 12345, 1.0, -2.0, 3.1)
        assert all(isinstance(f, float) for f in struct.fields[1:])
