#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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

import pytest

# python -m pytest tests/integration/examples/test_geospatial_types_example.py -s -v


def _echo(tx, x):
    return tx.run("RETURN $x AS fieldName", x=x).single()


def test_cartesian_point(driver):
    # tag::geospatial-types-cartesian-import[]
    from neo4j.spatial import CartesianPoint
    # end::geospatial-types-cartesian-import[]

    # tag::geospatial-types-cartesian[]
    # Creating a 2D point in Cartesian space
    point2d = CartesianPoint((1, 5.1))
    # Or in 3D
    point3d = CartesianPoint((1, -2., 3.1))
    # end::geospatial-types-cartesian[]

    # storing points for later assertions
    in_point2d = point2d
    in_point3d = point3d

    with driver.session() as session:
        record_with_2d_point = session.read_transaction(_echo, point2d)
        record_with_3d_point = session.read_transaction(_echo, point3d)

    # tag::geospatial-types-cartesian[]

    # Reading a 2D point from a record
    point2d = record_with_2d_point.get("fieldName")  # type: CartesianPoint
    str(point2d)  # POINT(1.0 5.1)
    point2d.x  # 1.0
    point2d.y  # 5.1
    # point2d.z raises AttributeError
    point2d.srid  # 7203
    len(point2d)  # 2

    # Reading a 3D point from a record
    point3d = record_with_3d_point.get("fieldName")  # type: CartesianPoint
    str(point3d)  # POINT(1.0 -2.0 3.1)
    point3d.x  # 1.0
    point3d.y  # -2.0
    point3d.z  # 3.1
    point3d.srid  # 9157
    len(point2d)  # 3
    # end::geospatial-types-cartesian[]

    assert str(point2d) == "POINT(1.0 5.1)"
    assert isinstance(point2d.x, float) and point2d.x == 1.0
    assert isinstance(point2d.y, float) and point2d.y == 5.1
    with pytest.raises(AttributeError):
        point2d.z
    assert point2d.srid == 7203
    assert len(point2d) == 2
    assert point2d == in_point2d

    assert str(point3d) == "POINT(1.0 -2.0 3.1)"
    assert isinstance(point3d.x, float) and point3d.x == 1.0
    assert isinstance(point3d.y, float) and point3d.y == -2.0
    assert isinstance(point3d.z, float) and point3d.z == 3.1
    assert point3d.srid == 9157
    assert len(point3d) == 3
    assert point3d == in_point3d


def test_wgs84_point(driver):
    # tag::geospatial-types-wgs84-import[]
    from neo4j.spatial import WGS84Point
    # end::geospatial-types-wgs84-import[]

    # tag::geospatial-types-wgs84[]
    # Creating a 2D point in WSG84 space
    point2d = WGS84Point((1, 5.1))
    # Or in 3D
    point3d = WGS84Point((1, -2., 3.1))
    # end::geospatial-types-wgs84[]

    # storing points for later assertions
    in_point2d = point2d
    in_point3d = point3d

    with driver.session() as session:
        record_with_2d_point = session.read_transaction(_echo, point2d)
        record_with_3d_point = session.read_transaction(_echo, point3d)

    # tag::geospatial-types-wgs84[]

    # Reading a 2D point from a record
    point2d = record_with_2d_point.get("fieldName")  # type: WGS84Point
    str(point2d)  # POINT(1.0 5.1)
    point2d.longitude  # 1.0 (point2d.x is an alias for longitude)
    point2d.latitude  # 5.1 (point2d.y is an alias for latitude)
    # point2d.height raises AttributeError (same with point2d.z)
    point2d.srid  # 4326
    len(point2d)  # 2

    # Reading a 3D point from a record
    point3d = record_with_3d_point.get("fieldName")  # type: WGS84Point
    str(point3d)  # POINT(1.0 -2.0 3.1)
    point3d.longitude  # 1.0 (point3d.x is an alias for longitude)
    point3d.latitude  # -2.0 (point3d.y is an alias for latitude)
    point3d.height  # 3.1 (point3d.z is an alias for height)
    point3d.srid  # 4979
    len(point2d)  # 3
    # end::geospatial-types-wgs84[]

    assert str(point2d) == "POINT(1.0 5.1)"
    assert isinstance(point2d.longitude, float) and point2d.longitude == 1.0
    assert isinstance(point2d.x, float) and point2d.x == 1.0
    assert isinstance(point2d.latitude, float) and point2d.latitude == 5.1
    assert isinstance(point2d.y, float) and point2d.y == 5.1
    with pytest.raises(AttributeError):
        point2d.height
    with pytest.raises(AttributeError):
        point2d.z
    assert point2d.srid == 4326
    assert len(point2d) == 2
    assert point2d == in_point2d

    assert str(point3d) == "POINT(1.0 -2.0 3.1)"
    assert isinstance(point3d.longitude, float) and point3d.longitude == 1.0
    assert isinstance(point3d.x, float) and point3d.x == 1.0
    assert isinstance(point3d.latitude, float) and point3d.latitude == -2.0
    assert isinstance(point3d.y, float) and point3d.y == -2.0
    assert isinstance(point3d.height, float) and point3d.height == 3.1
    assert isinstance(point3d.z, float) and point3d.z == 3.1
    assert point3d.srid == 4979
    assert len(point3d) == 3
    assert point3d == in_point3d
