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

import math
import typing as t

import pytest

from neo4j._codec.hydration.http.v2 import HydrationHandler
from neo4j.spatial import (
    CartesianPoint,
    Point,
    WGS84Point,
)

from .._base import HydrationHandlerTestBase


if t.TYPE_CHECKING:
    from neo4j._codec.hydration.http._common import HydrationScopeHttp


class TestSpatialHydration(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandler:
        return HydrationHandler()

    @pytest.mark.parametrize(
        ("coords", "encoded_coords", "srid", "point_cls"),
        (
            # 2D points
            *(
                (coords, encoded_coords, srid, point_cls)
                for coords, encoded_coords in (
                    ((1.2, 3.4), "(1.2 3.4)"),
                    ((-0.1, 2e20), "(-.1 2E20)"),
                    ((math.nan, 1.23e-5), "(NaN 1.23e-05)"),
                    ((math.inf, -math.inf), "(Infinity -Infinity)"),
                )
                for srid, point_cls in (
                    (7203, CartesianPoint),
                    (4326, WGS84Point),
                )
            ),
            # 3D points
            *(
                (coords, encoded_coords, srid, point_cls)
                for coords, encoded_coords in (
                    ((0.0, -0.0, 2.56e07), "(0 -0 2.56E+7)"),
                    ((2e700, -math.inf, math.nan), "(2E+0700 -Infinity NaN)"),
                )
                for srid, point_cls in (
                    (9157, CartesianPoint),
                    (4979, WGS84Point),
                )
            ),
            # Custom SRID points
            *(
                (coords, encoded_coords, srid, Point)
                for coords, encoded_coords in (
                    ((1.2, 3.4), "(1.2 3.4)"),
                    ((1.0, 2.0, 3.0), "(1 2 3)"),
                )
                for srid in (0, 1337, 123456789)
            ),
        ),
    )
    def test_point(
        self,
        coords: tuple[float, ...],
        encoded_coords: str,
        srid: int,
        point_cls: type[Point],
        hydration_scope: HydrationScopeHttp,
    ) -> None:
        point_type = "POINT" if len(coords) == 2 else "POINT Z"
        encoded = {
            "$type": "Point",
            "_value": f"SRID={srid};{point_type} {encoded_coords}",
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(decoded, point_cls)
        assert decoded.srid == srid
        assert all(isinstance(c, float) for c in decoded)
        assert len(decoded) == len(coords)
        assert all(
            a == b or (math.isnan(a) and math.isnan(b))
            for a, b in zip(decoded, coords, strict=True)
        )
