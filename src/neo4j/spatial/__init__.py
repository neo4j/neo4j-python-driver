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


"""Spatial data types for interchange with the DBMS."""

__all__ = [
    "CartesianPoint",
    "Point",
    "WGS84Point",
    "dehydrate_point",
    "hydrate_point",
    "point_type",
]

from functools import wraps

from .._codec.hydration.v1 import spatial as _hydration
from .._meta import deprecated
from .._spatial import (
    CartesianPoint,
    Point,
    point_type as _point_type,
    WGS84Point,
)


# TODO: 6.0 - remove
@deprecated(
    "hydrate_point is considered an internal function and will be removed in "
    "a future version"
)
def hydrate_point(srid, *coordinates):
    """
    Create a new instance of a Point subclass from a raw set of fields.

    The subclass chosen is determined by the
    given SRID; a ValueError will be raised if no such
    subclass can be found.
    """
    return _hydration.hydrate_point(srid, *coordinates)


# TODO: 6.0 - remove
@deprecated(
    "hydrate_point is considered an internal function and will be removed in "
    "a future version"
)
@wraps(_hydration.dehydrate_point)
def dehydrate_point(value):
    """
    Dehydrator for Point data.

    :param value:
    :type value: Point
    :returns:
    """
    return _hydration.dehydrate_point(value)


# TODO: 6.0 - remove
@deprecated(
    "point_type is considered an internal function and will be removed in "
    "a future version"
)
@wraps(_point_type)
def point_type(name, fields, srid_map):
    return _point_type(name, fields, srid_map)
