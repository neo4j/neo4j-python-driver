#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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
This module contains classes for modelling nodes, relationships and
paths belonging to a Neo4j graph database. The `hydration` function,
also included, allows PackStream structures to be turned into instances
of these classes.
"""


from .packstream import Structure


class Entity(object):
    """ Base class for Node and Relationship.
    """

    def __init__(self, identity, properties=None):
        self._identity = identity
        self._properties = dict((k, v) for k, v in (properties or {}).items() if v is not None)

    def __eq__(self, other):
        try:
            return self.identity() == other.identity()
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._identity)

    def __len__(self):
        return len(self._properties)

    def __getitem__(self, key):
        return self._properties.get(key)

    def __contains__(self, key):
        return key in self._properties

    def __iter__(self):
        return iter(self._properties)

    def identity(self):
        return self._identity

    def get(self, key, default=None):
        return self._properties.get(key, default)

    def keys(self):
        return set(self._properties.keys())

    def values(self):
        return set(self._properties.values())

    def items(self):
        return set(self._properties.items())


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

    def __init__(self, entities):
        self._nodes = tuple(map(hydrated, entities[0::2]))
        self._relationships = tuple(map(hydrated, entities[1::2]))
        self._directions = tuple("->" if rel.start() == self._nodes[i] else "<-"
                                 for i, rel in enumerate(self._relationships))

    def __repr__(self):
        return "<Path start=%r end=%r size=%s>" % \
               (self.start().identity(), self.end().identity(), len(self))

    def __eq__(self, other):
        try:
            return self.start() == other.start() and self.relationships() == other.relationships()
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = hash(self.start())
        for relationship in self.relationships():
            value ^= hash(relationship)
        return value

    def __len__(self):
        return len(self._relationships)

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


structures = {
    b"N": Node,
    b"R": Relationship,
    b"P": Path,
}


def hydrated(obj):
    """ Hydrate an object or a collection of nested objects by replacing
    structures with entity instances.
    """
    if isinstance(obj, Structure):
        signature, args = obj
        try:
            structure = structures[signature]
        except KeyError:
            # If we don't recognise the structure type, just return it as-is
            return obj
        else:
            # Otherwise pass the structural data to the appropriate constructor
            return structure(*args)
    elif isinstance(obj, list):
        return list(map(hydrated, obj))
    elif isinstance(obj, dict):
        return {key: hydrated(value) for key, value in obj.items()}
    else:
        return obj
