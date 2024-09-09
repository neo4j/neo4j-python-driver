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

import pytest

from neo4j.spatial import WGS84Point


def make_reduce_points():
    return (
        WGS84Point((1, 2)),
        WGS84Point((3.2, 4.0)),
        WGS84Point((3, 4, -1)),
        WGS84Point((3.2, 4.0, -1.2)),
    )


class TestWGS84Point:
    def test_alias_3d(self) -> None:
        x, y, z = 3.2, 4.0, -1.2
        p = WGS84Point((x, y, z))

        assert hasattr(p, "longitude")
        assert p.longitude == x
        assert hasattr(p, "x")
        assert p.x == x

        assert hasattr(p, "latitude")
        assert p.latitude == y
        assert hasattr(p, "y")
        assert p.y == y

        assert hasattr(p, "height")
        assert p.height == z
        assert hasattr(p, "z")
        assert p.z == z

    def test_alias_2d(self) -> None:
        x, y = 3.2, 4.0
        p = WGS84Point((x, y))

        assert hasattr(p, "longitude")
        assert p.longitude == x
        assert hasattr(p, "x")
        assert p.x == x

        assert hasattr(p, "latitude")
        assert p.latitude == y
        assert hasattr(p, "y")
        assert p.y == y

        with pytest.raises(AttributeError):
            _ = p.height
        with pytest.raises(AttributeError):
            _ = p.z

    @pytest.mark.parametrize("p", make_reduce_points())
    def test_copy(self, p):
        p.foo = [1, 2]
        p2 = copy.copy(p)
        assert p == p2
        assert p is not p2
        assert p.foo is p2.foo

    @pytest.mark.parametrize("p", make_reduce_points())
    def test_deep_copy(self, p):
        p.foo = [1, [2]]
        p2 = copy.deepcopy(p)
        assert p == p2
        assert p is not p2
        assert p.foo == p2.foo
        assert p.foo is not p2.foo
        assert p.foo[1] is not p2.foo[1]

    @pytest.mark.parametrize("expected", make_reduce_points())
    def test_pickle(self, expected):
        expected.foo = [1, [2]]
        actual = pickle.loads(pickle.dumps(expected))
        assert expected == actual
        assert expected is not actual
        assert expected.foo == actual.foo
        assert expected.foo is not actual.foo
