#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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


"""
This module defines spatial data types.
"""


__all__ = [
    "Point",
    "CartesianPoint",
    "CartesianPoint3D",
    "WGS84Point",
    "WGS84Point3D",
]


class Point(tuple):
    """ A point within a geometric space. This type is generally used
    via its subclasses and should not be instantiated directly unless
    there is no subclass defined for the required SRID.
    """

    srid = None

    def __new__(cls, iterable):
        return tuple.__new__(cls, iterable)

    def __repr__(self):
        return "POINT(%s)" % " ".join(map(str, self))

    def __eq__(self, other):
        try:
            return self.srid == other.srid and tuple(self) == tuple(other)
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.srid) ^ hash(tuple(self))


def __point_subclass(srid, name, fields):
    """ Dynamically create a Point subclass.
    """
    attributes = {"srid": srid}
    for i, field in enumerate(fields):
        attributes[field] = property(lambda self, index=i: self[index])
    return type(name, (Point,), attributes)


# Point subclass definitions
CartesianPoint = __point_subclass(7203, "CartesianPoint", ["x", "y"])
CartesianPoint3D = __point_subclass(9157, "CartesianPoint3D", ["x", "y", "z"])
WGS84Point = __point_subclass(4326, "WGS84Point", ["longitude", "latitude"])
WGS84Point3D = __point_subclass(4979, "WGS84Point3D", ["longitude", "latitude", "height"])


def hydrate_point(srid, *coordinates):
    """ Create a new instance of a Point subclass from a raw
    set of fields. The subclass chosen is determined by the
    given SRID; a ValueError will be raised if no such
    subclass can be found.
    """
    point_class = None
    for subclass in Point.__subclasses__():
        if subclass.srid == srid:
            point_class = subclass
            break
    if point_class is None:
        raise ValueError("SRID %d not supported" % srid)
    if 2 <= len(coordinates) <= 3:
        inst = point_class(coordinates)
        inst.srid = srid
        return inst
    else:
        raise ValueError("%d-dimensional Point values are not supported" % len(coordinates))


hydration_functions = {
    b"X": hydrate_point,
    b"Y": hydrate_point,
}

dehydration_functions = {
    # TODO
}
