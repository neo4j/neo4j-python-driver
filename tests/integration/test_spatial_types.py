#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from pytest import raises

from neo4j.spatial import CartesianPoint, WGS84Point


def test_cartesian_point_input(cypher_eval):
    x, y = cypher_eval("CYPHER runtime=interpreted "
                       "WITH $point AS point "
                       "RETURN [point.x, point.y]",
                       point=CartesianPoint((1.23, 4.56)))
    assert x == 1.23
    assert y == 4.56


def test_cartesian_3d_point_input(cypher_eval):
    x, y, z = cypher_eval("CYPHER runtime=interpreted "
                          "WITH $point AS point "
                          "RETURN [point.x, point.y, point.z]",
                          point=CartesianPoint((1.23, 4.56, 7.89)))
    assert x == 1.23
    assert y == 4.56
    assert z == 7.89


def test_wgs84_point_input(cypher_eval):
    lat, long = cypher_eval("CYPHER runtime=interpreted "
                            "WITH $point AS point "
                            "RETURN [point.latitude, point.longitude]",
                            point=WGS84Point((1.23, 4.56)))
    assert long == 1.23
    assert lat == 4.56


def test_wgs84_3d_point_input(cypher_eval):
    lat, long, height = cypher_eval("CYPHER runtime=interpreted "
                                    "WITH $point AS point "
                                    "RETURN [point.latitude, point.longitude, "
                                    "point.height]",
                                    point=WGS84Point((1.23, 4.56, 7.89)))
    assert long == 1.23
    assert lat == 4.56
    assert height == 7.89


def test_point_array_input(cypher_eval):
    data = [WGS84Point((1.23, 4.56)), WGS84Point((9.87, 6.54))]
    value = cypher_eval("CREATE (a {x:$x}) RETURN a.x", x=data)
    assert value == data


def test_cartesian_point_output(cypher_eval):
    value = cypher_eval("RETURN point({x:3, y:4})")
    assert isinstance(value, CartesianPoint)
    assert value.x == 3.0
    assert value.y == 4.0
    with raises(AttributeError):
        _ = value.z


def test_cartesian_3d_point_output(cypher_eval):
    value = cypher_eval("RETURN point({x:3, y:4, z:5})")
    assert isinstance(value, CartesianPoint)
    assert value.x == 3.0
    assert value.y == 4.0
    assert value.z == 5.0


def test_wgs84_point_output(cypher_eval):
    value = cypher_eval("RETURN point({latitude:3, longitude:4})")
    assert isinstance(value, WGS84Point)
    assert value.latitude == 3.0
    assert value.y == 3.0
    assert value.longitude == 4.0
    assert value.x == 4.0
    with raises(AttributeError):
        _ = value.height
    with raises(AttributeError):
        _ = value.z


def test_wgs84_3d_point_output(cypher_eval):
    value = cypher_eval("RETURN point({latitude:3, longitude:4, height:5})")
    assert isinstance(value, WGS84Point)
    assert value.latitude == 3.0
    assert value.y == 3.0
    assert value.longitude == 4.0
    assert value.x == 4.0
    assert value.height == 5.0
    assert value.z == 5.0
