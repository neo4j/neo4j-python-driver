#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import sys


__all__ = ["Record", "Node", "Relationship", "Path"]


if sys.version_info >= (3,):
    integer = int
    string = str
else:
    integer = (int, long)
    string = (str, unicode)


class Record(object):

    def __init__(self, fields, values):
        self.__fields__ = fields
        self.__values__ = values

    def __repr__(self):
        values = self.__values__
        s = []
        for i, field in enumerate(self.__fields__):
            value = values[i]
            if isinstance(value, tuple):
                signature, _ = value
                if signature == b"N":
                    s.append("%s=<Node>" % (field,))
                elif signature == b"R":
                    s.append("%s=<Relationship>" % (field,))
                else:
                    s.append("%s=<?>" % (field,))
            else:
                s.append("%s=%r" % (field, value))
        return "<Record %s>" % " ".join(s)

    def __eq__(self, other):
        try:
            return vars(self) == vars(other)
        except TypeError:
            return tuple(self) == tuple(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return self.__fields__.__len__()

    def __getitem__(self, item):
        if isinstance(item, string):
            return getattr(self, item)
        elif isinstance(item, integer):
            return getattr(self, self.__fields__[item])
        else:
            raise LookupError(item)

    def __getattr__(self, item):
        try:
            i = self.__fields__.index(item)
        except ValueError:
            raise AttributeError("No field %r" % item)
        else:
            value = self.__values__[i]
            if isinstance(value, tuple):
                value = self.__values__[i] = hydrated(value)
            return value


class Entity(object):
    """ Base class for Node and Relationship.
    """

    def __init__(self, identity, properties=None):
        self._identity = identity
        self._properties = dict(properties or {})

    def identity(self):
        return self._identity

    def property(self, key, default=None):
        return self._properties.get(key, default)

    def property_keys(self):
        return set(self._properties.keys())


class Node(Entity):
    """ Self-contained graph node.
    """

    def __init__(self, identity, labels, properties=None):
        super(Node, self).__init__(identity, properties)
        self._labels = set(labels)

    def __repr__(self):
        return "<Node identity=%r labels=%r properties=%r>" % \
               (self._identity, self._labels, self._properties)

    def labels(self):
        return self._labels


class Relationship(Entity):
    """ Self-contained graph relationship.
    """

    def __init__(self, identity, start, end, type_, properties=None):
        super(Relationship, self).__init__(identity, properties)
        self._start = start
        self._end = end
        self._type = type_

    def __repr__(self):
        return "<Relationship identity=%r start=%r end=%r type=%r properties=%r>" % \
               (self._identity, self._start, self._end, self._type, self._properties)

    def start(self):
        return self._start

    def type(self):
        return self._type

    def end(self):
        return self._end


class Path(object):
    """ Self-contained graph path.
    """

    def __init__(self, start, relationships):
        self._nodes = [start]
        self._relationships = list(relationships)
        for relationship in self._relationships:
            end = self.end()
            if end == relationship.start():
                # forward
                self._nodes.append(relationship.end())
            elif end == relationship.end():
                # reverse
                self._nodes.append(relationship.start())
            else:
                raise ValueError("Relationships do not form a continuous path")

    def __iter__(self):
        return iter(self._relationships)

    def start(self):
        return self._nodes[0]

    def end(self):
        return self._nodes[-1]

    def nodes(self):
        return self._nodes

    def relationships(self):
        return self._relationships


types = {
    b"N": Node,
    b"R": Relationship,
    b"P": Path,
}


def hydrated(obj):
    if isinstance(obj, tuple):
        signature, args = obj
        try:
            return types[signature](*args)
        except KeyError:
            raise RuntimeError("Unknown structure signature %r" % signature)
    elif isinstance(obj, list):
        return list(map(hydrated, obj))
    elif isinstance(obj, dict):
        return {key: hydrated(value) for key, value in obj.items()}
    else:
        return obj
