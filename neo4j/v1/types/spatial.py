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
Spatial data types
"""


__all__ = [
    "Point",
    "CartesianPoint",
    "CartesianPoint3D",
    "WGS84Point",
    "WGS84Point3D",
]


class Point(tuple):
    """ A point within a geometric space.
    """

    @classmethod
    def __get_subclass(cls, crs):
        """ Finds the Point subclass with the given CRS.
        """
        if cls.crs == crs:
            return cls
        for subclass in cls.__subclasses__():
            got = subclass.__get_subclass(crs)
            if got:
                return got
        return None

    crs = None

    @classmethod
    def hydrate(cls, crs, *coordinates):
        point_class = cls.__get_subclass(crs)
        if point_class is None:
            raise ValueError("CRS %d not supported" % crs)
        if 2 <= len(coordinates) <= 3:
            inst = point_class(coordinates)
            inst.crs = crs
            return inst
        else:
            raise ValueError("%d-dimensional Point values are not supported" % len(coordinates))

    def __new__(cls, iterable):
        return tuple.__new__(cls, iterable)

    def __repr__(self):
        return "POINT(%s)" % " ".join(map(str, self))

    def __eq__(self, other):
        try:
            return self.crs == other.crs and tuple(self) == tuple(other)
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.crs) ^ hash(tuple(self))


class CartesianPoint(Point):
    """ Point in 2-dimensional Cartesian space.
    """

    crs = 7203

    @property
    def x(self):
        return self[0]

    @property
    def y(self):
        return self[1]


class CartesianPoint3D(CartesianPoint):
    """ Point in 3-dimensional Cartesian space.
    """

    crs = 9157

    @property
    def z(self):
        return self[2]


class WGS84Point(Point):
    """ Point in 2-dimensional World Geodetic System space.
    """

    crs = 4326

    @property
    def longitude(self):
        return self[0]

    @property
    def latitude(self):
        return self[1]


class WGS84Point3D(WGS84Point):
    """ Point in 3-dimensional World Geodetic System space.
    """

    crs = 4979

    @property
    def height(self):
        return self[2]
