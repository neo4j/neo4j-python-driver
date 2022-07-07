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


"""
This module defines _spatial data types.
"""


from threading import Lock


# SRID to subclass mappings
srid_table = {}
srid_table_lock = Lock()


class Point(tuple):
    """Base-class for _spatial data.

    A point within a geometric space. This type is generally used via its
    subclasses and should not be instantiated directly unless there is no
    subclass defined for the required SRID.

    :param iterable:
        An iterable of coordinates.
        All items will be converted to :class:`float`.
    """

    #: The SRID (_spatial reference identifier) of the _spatial data.
    #: A number that identifies the coordinate system the _spatial type is to be
    #: interpreted in.
    #:
    #: :type: int
    srid = None

    def __new__(cls, iterable):
        return tuple.__new__(cls, map(float, iterable))

    def __repr__(self):
        return "POINT(%s)" % " ".join(map(str, self))

    def __eq__(self, other):
        try:
            return type(self) is type(other) and tuple(self) == tuple(other)
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(type(self)) ^ hash(tuple(self))


def point_type(name, fields, srid_map):
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

    cls = type(name, (Point,), attributes)

    with srid_table_lock:
        for dim, srid in srid_map.items():
            srid_table[srid] = (cls, dim)

    return cls


# Point subclass definitions
CartesianPoint = point_type("CartesianPoint", ["x", "y", "z"],
                            {2: 7203, 3: 9157})
WGS84Point = point_type("WGS84Point", ["longitude", "latitude", "height"],
                        {2: 4326, 3: 4979})
