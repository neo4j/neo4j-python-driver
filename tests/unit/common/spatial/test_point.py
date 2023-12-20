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
import typing as t

import pytest

from neo4j._spatial import (
    Point,
    point_type,
)


def make_reduce_points():
    return (
        Point((42,)),
        Point((69.420,)),
        Point((1, 2)),
        Point((1.2, 2.3)),
        Point((1, 3, 3, 7)),
        Point((1.0, 3.0, 3.0, 7.0)),
    )


class TestPoint:

    @pytest.mark.parametrize("argument", (
        ("a", "b"), {"x": 1.0, "y": 2.0}
    ))
    def test_wrong_type_arguments(self, argument) -> None:
        with pytest.raises(ValueError):
            Point(argument)

    @pytest.mark.parametrize("argument", (
        (1, 2), (1.2, 2.1)
    ))
    def test_number_arguments(self, argument: t.Iterable[float]) -> None:
        print(argument)
        p = Point(argument)
        assert tuple(p) == argument

    def test_immutable_coordinates(self) -> None:
        MyPoint = point_type("MyPoint", ("x", "y", "z"), {2: 1234, 3: 5678})
        coordinates = (.1, 0)
        p = MyPoint(coordinates)
        with pytest.raises(AttributeError):
            p.x = 2.0  # type: ignore[misc]
        with pytest.raises(AttributeError):
            p.y = 2.0  # type: ignore[misc]
        with pytest.raises(AttributeError):
            p.z = 2.0  # type: ignore[misc]
        with pytest.raises(TypeError):
            p[0] = 2.0  # type: ignore[index]
        with pytest.raises(TypeError):
            p[1] = 2.0  # type: ignore[index]
        with pytest.raises(TypeError):
            p[2] = 2.0  # type: ignore[index]

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
