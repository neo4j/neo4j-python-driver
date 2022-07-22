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


import pytest

from neo4j._spatial import (
    Point,
    point_type,
)


class PointTestCase:

    @pytest.mark.parametrize("argument", ("a", "b"), ({"x": 1.0, "y": 2.0}))
    def test_wrong_type_arguments(self, argument):
        with pytest.raises(ValueError):
            Point(argument)

    @pytest.mark.parametrize((1, 2), (1.2, 2.1))
    def test_wrong_type_arguments(self, argument):
        p = Point(argument)
        assert tuple(p) == argument

    def test_immutable_coordinates(self):
        MyPoint = point_type("MyPoint", ["x", "y"], {2: 1234})
        coordinates = (.1, 0)
        p = MyPoint(coordinates)
        with pytest.raises(AttributeError):
            p.x = 2.0
        with pytest.raises(AttributeError):
            p.y = 2.0
        with pytest.raises(TypeError):
            p[0] = 2.0
        with pytest.raises(TypeError):
            p[1] = 2.0
