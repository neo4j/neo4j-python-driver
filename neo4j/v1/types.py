#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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
    id = None
    properties = None

    def __init__(self, properties=None, **kw_properties):
        properties = dict(properties or {}, **kw_properties)
        self.properties = properties

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

    def __getitem__(self, key):
        return self.properties.get(key)

    def __contains__(self, key):
        return key in self.properties

    def __iter__(self):
        return iter(self.properties)

    @property
    def props(self):
        # TODO: deprecate "properties" as the decorated attribute
        return self.properties

    @property
    def properties(self):
        return self._properties

    @properties.setter
    def properties(self, properties:dict):
        self._properties = {
            k: v for k, v in properties.items() if v is not None
        }

    def get(self, key, default=None):
        return self.properties.get(key, default)

    def keys(self):
        return self.properties.keys()

    def values(self):
        return self.properties.values()

    def items(self):
        return self.properties.items()


class Node(Entity):
    """ Self-contained graph node.
    """
    labels = None

    @classmethod
    def hydrate(cls, id_, labels, properties=None, cypher_obj=None, **kwargs):
        if cypher_obj:
            inst = cypher_obj.new_node(
                    labels=labels,
                    properties=properties,
                    )
        else:
            inst = cls(labels, properties)
        inst.id = id_
        return inst

    def __init__(self, labels=None, properties=None, **kw_properties):
        super(Node, self).__init__(properties, **kw_properties)
        self.labels = set(labels or set())

    def __repr__(self):
        return "<Node id=%r labels=%r properties=%r>" % \
               (self.id, self.labels, self.properties)


class BaseRelationship(Entity):
    """ Base class for Relationship and UnboundRelationship.
    """
    type = None

    def __init__(self, type, properties=None, **kw_properties):
        super(BaseRelationship, self).__init__(properties, **kw_properties)
        self.type = type


class Relationship(BaseRelationship):
    """ Self-contained graph relationship.
    """
    start = None
    end = None

    @classmethod
    def hydrate(cls, id_, start, end, type, properties=None, **kwargs):
        inst = cls(start, end, type, properties)
        inst.id = id_
        return inst

    def __init__(self, start, end, type, properties=None, **kw_properties):
        assert isinstance(start, int)
        assert isinstance(end, int)
        super(Relationship, self).__init__(type, properties, **kw_properties)
        self.start = start
        self.end = end

    def __repr__(self):
        return "<Relationship id=%r start=%r end=%r type=%r properties=%r>" % \
               (self.id, self.start, self.end, self.type, self.properties)

    def unbind(self):
        inst = UnboundRelationship(self.type, self.properties)
        inst.id = self.id
        return inst


class UnboundRelationship(BaseRelationship):
    """ Self-contained graph relationship without endpoints.
    """

    @classmethod
    def hydrate(cls, id_, type, properties=None, **kwargs):
        inst = cls(type, properties)
        inst.id = id_
        return inst

    def __init__(self, type, properties=None, **kw_properties):
        super(UnboundRelationship, self).__init__(type, properties, **kw_properties)

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
    def hydrate(cls, nodes, rels, sequence, **kwargs):
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


hydration_functions = {
    b"N": Node.hydrate,
    b"R": Relationship.hydrate,
    b"r": UnboundRelationship.hydrate,
    b"P": Path.hydrate,
}


def hydrated(obj, cypher_obj=None):
    """ Hydrate an object or a collection of nested objects by replacing
    structures with entity instances.
    """
    if isinstance(obj, Structure):
        signature, args = obj
        try:
            hydration_function = hydration_functions[signature]
        except KeyError:
            # If we don't recognise the structure type, just return it as-is
            return obj
        else:
            # Otherwise pass the structural data to the appropriate hydration function
            hydration_targets = [
                    hydrated(arg, cypher_obj=cypher_obj) for arg in args
                    ]
            return hydration_function(*hydration_targets, cypher_obj=cypher_obj)
    elif isinstance(obj, list):
        return [hydrated(arg, cypher_obj=cypher_obj) for arg in obj]
    elif isinstance(obj, dict):
        return {
            key: hydrated(arg, cypher_obj=cypher_obj)
            for key, arg in obj.items()
            }
    else:
        return obj
