#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
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

from neo4j.packstream import Structure
from neo4j.compat import string, integer

from .api import GraphDatabase, ValueSystem


class Record(object):
    """ Record is an ordered collection of fields.

    A Record object is used for storing result values along with field names.
    Fields can be accessed by numeric or named index (``record[0]`` or
    ``record["field"]``).
    """

    def __init__(self, keys, values):
        self._keys = tuple(keys)
        self._values = tuple(values)

    def keys(self):
        """ Return the keys (key names) of the record
        """
        return self._keys

    def values(self):
        """ Return the values of the record
        """
        return self._values

    def items(self):
        """ Return the fields of the record as a list of key and value tuples
        """
        return zip(self._keys, self._values)

    def index(self, key):
        """ Return the index of the given key
        """
        try:
            return self._keys.index(key)
        except ValueError:
            raise KeyError(key)

    def __record__(self):
        return self

    def __contains__(self, key):
        return self._keys.__contains__(key)

    def __iter__(self):
        return iter(self._keys)

    def copy(self):
        return Record(self._keys, self._values)

    def __getitem__(self, item):
        if isinstance(item, string):
            return self._values[self.index(item)]
        elif isinstance(item, integer):
            return self._values[item]
        else:
            raise TypeError(item)

    def __len__(self):
        return len(self._keys)

    def __repr__(self):
        values = self._values
        s = []
        for i, field in enumerate(self._keys):
            s.append("%s=%r" % (field, values[i]))
        return "<Record %s>" % " ".join(s)

    def __hash__(self):
        return hash(self._keys) ^ hash(self._values)

    def __eq__(self, other):
        try:
            return self._keys == tuple(other.keys()) and self._values == tuple(other.values())
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class Entity(object):
    """ Base class for Node and Relationship.
    """
    id = None
    properties = None

    def __init__(self, properties=None, **kwproperties):
        properties = dict(properties or {}, **kwproperties)
        self.properties = dict((k, v) for k, v in properties.items() if v is not None)

    def __eq__(self, other):
        try:
            return self.id == other.id
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.id)

    def __len__(self):
        return len(self.properties)

    def __getitem__(self, name):
        return self.properties.get(name)

    def __contains__(self, name):
        return name in self.properties

    def __iter__(self):
        return iter(self.properties)

    def get(self, name, default=None):
        """ Get a property value by name, optionally with a default.
        """
        return self.properties.get(name, default)

    def keys(self):
        """ Return an iterable of all property names.
        """
        return self.properties.keys()

    def values(self):
        """ Return an iterable of all property values.
        """
        return self.properties.values()

    def items(self):
        """ Return an iterable of all property name-value pairs.
        """
        return self.properties.items()


class Node(Entity):
    """ Self-contained graph node.
    """
    labels = None

    @classmethod
    def hydrate(cls, id_, labels, properties=None):
        inst = cls(labels, properties)
        inst.id = id_
        return inst

    def __init__(self, labels=None, properties=None, **kwproperties):
        super(Node, self).__init__(properties, **kwproperties)
        self.labels = set(labels or set())

    def __repr__(self):
        return "<Node id=%r labels=%r properties=%r>" % \
               (self.id, self.labels, self.properties)


class BaseRelationship(Entity):
    """ Base class for Relationship and UnboundRelationship.
    """
    type = None

    def __init__(self, type, properties=None, **kwproperties):
        super(BaseRelationship, self).__init__(properties, **kwproperties)
        self.type = type


class Relationship(BaseRelationship):
    """ Self-contained graph relationship.
    """

    #: The start node of this relationship
    start = None

    #: The end node of this relationship
    end = None

    @classmethod
    def hydrate(cls, id_, start, end, type, properties=None):
        inst = cls(start, end, type, properties)
        inst.id = id_
        return inst

    def __init__(self, start, end, type, properties=None, **kwproperties):
        assert isinstance(start, int)
        assert isinstance(end, int)
        super(Relationship, self).__init__(type, properties, **kwproperties)
        self.start = start
        self.end = end

    def __repr__(self):
        return "<Relationship id=%r start=%r end=%r type=%r properties=%r>" % \
               (self.id, self.start, self.end, self.type, self.properties)


class UnboundRelationship(BaseRelationship):
    """ Self-contained graph relationship without endpoints.
    """

    @classmethod
    def hydrate(cls, id_, type, properties=None):
        inst = cls(type, properties)
        inst.id = id_
        return inst

    def __init__(self, type, properties=None, **kwproperties):
        super(UnboundRelationship, self).__init__(type, properties, **kwproperties)

    def __repr__(self):
        return "<UnboundRelationship id=%r type=%r properties=%r>" % \
               (self.id, self.type, self.properties)

    def bind(self, start, end):
        inst = Relationship(start, end, self.type, self.properties)
        inst.id = self.id
        return inst


class Path(object):
    """ Self-contained graph path.
    """
    nodes = None
    relationships = None

    @classmethod
    def hydrate(cls, nodes, rels, sequence):
        assert len(nodes) >= 1
        assert len(sequence) % 2 == 0
        last_node = nodes[0]
        entities = [last_node]
        for i, rel_index in enumerate(sequence[::2]):
            assert rel_index != 0
            next_node = nodes[sequence[2 * i + 1]]
            if rel_index > 0:
                entities.append(rels[rel_index - 1].bind(last_node.id, next_node.id))
            else:
                entities.append(rels[-rel_index - 1].bind(next_node.id, last_node.id))
            entities.append(next_node)
            last_node = next_node
        return cls(*entities)

    def __init__(self, start_node, *rels_and_nodes):
        self.nodes = (start_node,) + rels_and_nodes[1::2]
        self.relationships = rels_and_nodes[0::2]

    def __repr__(self):
        return "<Path start=%r end=%r size=%s>" % \
               (self.start.id, self.end.id, len(self))

    def __eq__(self, other):
        try:
            return self.start == other.start and self.relationships == other.relationships
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = hash(self.start)
        for relationship in self.relationships:
            value ^= hash(relationship)
        return value

    def __len__(self):
        return len(self.relationships)

    def __iter__(self):
        return iter(self.relationships)

    @property
    def start(self):
        return self.nodes[0]

    @property
    def end(self):
        return self.nodes[-1]


class PackStreamValueSystem(ValueSystem):

    def hydrate(self, values):

        def hydrate_(obj):
            if isinstance(obj, Structure):
                signature, args = obj
                if signature == b"N":
                    return Node.hydrate(*map(hydrate_, args))
                elif signature == b"R":
                    return Relationship.hydrate(*map(hydrate_, args))
                elif signature == b"r":
                    return UnboundRelationship.hydrate(*map(hydrate_, args))
                elif signature == b"P":
                    return Path.hydrate(*map(hydrate_, args))
                else:
                    # If we don't recognise the structure type, just return it as-is
                    return obj
            elif isinstance(obj, list):
                return list(map(hydrate_, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_(value) for key, value in obj.items()}
            else:
                return obj

        return tuple(map(hydrate_, values))


GraphDatabase.value_systems["packstream"] = PackStreamValueSystem()
