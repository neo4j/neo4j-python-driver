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


"""
This module defines spatial data types.
"""

from __future__ import annotations

import typing as t
from threading import Lock


# SRID to subclass mappings
srid_table: t.Dict[int, t.Tuple[t.Type[Point], int]] = {}
srid_table_lock = Lock()


class Point(t.Tuple[float, ...]):
    """Base-class for spatial data.

    A point within a geometric space. This type is generally used via its
    subclasses and should not be instantiated directly unless there is no
    subclass defined for the required SRID.

    :param iterable:
        An iterable of coordinates.
        All items will be converted to :class:`float`.
    :type iterable: Iterable[float]
    """

    #: The SRID (spatial reference identifier) of the spatial data.
    #: A number that identifies the coordinate system the spatial type is to
    #: be interpreted in.
    srid: t.Optional[int]

    if t.TYPE_CHECKING:
        @property
        def x(self) -> float: ...

        @property
        def y(self) -> float: ...

        @property
        def z(self) -> float: ...

    def __new__(cls, iterable: t.Iterable[float]) -> Point:
        # mypy issue https://github.com/python/mypy/issues/14890
        return tuple.__new__(  # type: ignore[type-var]
            cls, map(float, iterable)
        )

    def __repr__(self) -> str:
        return "POINT(%s)" % " ".join(map(str, self))

    def __eq__(self, other: object) -> bool:
        try:
            return (type(self) is type(other)
                    and tuple(self) == tuple(t.cast(Point, other)))
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self):
        return hash(type(self)) ^ hash(tuple(self))


def point_type(
    name: str,
    fields: t.Tuple[str, str, str],
    srid_map: t.Dict[int, int]
) -> t.Type[Point]:
    """ Dynamically create a Point subclass.
    """

    def srid(self):
        try:
            return srid_map[len(self)]
        except KeyError:
            return None

    attributes = {"srid": property(srid)}

    for index, subclass_field in enumerate(fields):

        def accessor(self, i=index, f=subclass_field):
            try:
                return self[i]
            except IndexError:
                raise AttributeError(f)

        for field_alias in {subclass_field, "xyz"[index]}:
            attributes[field_alias] = property(accessor)

    cls = t.cast(t.Type[Point], type(name, (Point,), attributes))

    with srid_table_lock:
        for dim, srid_ in srid_map.items():
            srid_table[srid_] = (cls, dim)

    return cls


# Point subclass definitions
if t.TYPE_CHECKING:
    class CartesianPoint(Point):
        ...
else:
    CartesianPoint = point_type("CartesianPoint", ("x", "y", "z"),
                                {2: 7203, 3: 9157})

if t.TYPE_CHECKING:
    class WGS84Point(Point):
        @property
        def longitude(self) -> float: ...

        @property
        def latitude(self) -> float: ...

        @property
        def height(self) -> float: ...
else:
    WGS84Point = point_type("WGS84Point", ("longitude", "latitude", "height"),
                            {2: 4326, 3: 4979})
