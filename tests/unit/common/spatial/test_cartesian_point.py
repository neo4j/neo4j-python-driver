# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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

import pytest

from neo4j.spatial import CartesianPoint


class TestCartesianPoint:

    def test_alias_3d(self) -> None:
        x, y, z = 3.2, 4.0, -1.2
        p = CartesianPoint((x, y, z))
        assert hasattr(p, "x")
        assert p.x == x
        assert hasattr(p, "y")
        assert p.y == y
        assert hasattr(p, "z")
        assert p.z == z

    def test_alias_2d(self) -> None:
        x, y = 3.2, 4.0
        p = CartesianPoint((x, y))
        assert hasattr(p, "x")
        assert p.x == x
        assert hasattr(p, "y")
        assert p.y == y
        with pytest.raises(AttributeError):
            _ = p.z
