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

"""
Graph data types
"""


from collections.abc import Mapping


__all__ = [
    "Graph",
    "Node",
    "Relationship",
    "Path",
]


class Graph:
    """ Local, self-contained graph object that acts as a container for
    :class:`.Node` and :class:`.Relationship` instances.
    """

    def __init__(self):
        self._nodes = {}
        self._relationships = {}
        self._relationship_types = {}
        self._node_set_view = EntitySetView(self._nodes)
        self._relationship_set_view = EntitySetView(self._relationships)

    @property
    def nodes(self):
        """ Access a set view of the nodes in this graph.
        """
        return self._node_set_view

    @property
    def relationships(self):
        """ Access a set view of the relationships in this graph.
        """
        return self._relationship_set_view

    def relationship_type(self, name):
        """ Obtain a :class:`.Relationship` subclass for a given
        relationship type name.
        """
        try:
            cls = self._relationship_types[name]
        except KeyError:
            cls = self._relationship_types[name] = type(str(name), (Relationship,), {})
        return cls

    class Hydrator:

        def __init__(self, graph):
            self.graph = graph

        def hydrate_node(self, n_id, n_labels=None, properties=None):
            assert isinstance(self.graph, Graph)
            try:
                inst = self.graph._nodes[n_id]
            except KeyError:
                inst = self.graph._nodes[n_id] = Node(self.graph, n_id, n_labels, properties)
            else:
                # If we have already hydrated this node as the endpoint of
                # a relationship, it won't have any labels or properties.
                # Therefore, we need to add the ones we have here.
                if n_labels:
                    inst._labels = inst._labels.union(n_labels)  # frozen_set
                if properties:
                    inst._properties.update(properties)
            return inst

        def hydrate_relationship(self, r_id, n0_id, n1_id, r_type, properties=None):
            inst = self.hydrate_unbound_relationship(r_id, r_type, properties)
            inst._start_node = self.hydrate_node(n0_id)
            inst._end_node = self.hydrate_node(n1_id)
            return inst

        def hydrate_unbound_relationship(self, r_id, r_type, properties=None):
            assert isinstance(self.graph, Graph)
            try:
                inst = self.graph._relationships[r_id]
            except KeyError:
                r = self.graph.relationship_type(r_type)
                inst = self.graph._relationships[r_id] = r(self.graph, r_id, properties)
            return inst

        def hydrate_path(self, nodes, relationships, sequence):
            assert isinstance(self.graph, Graph)
            assert len(nodes) >= 1
            assert len(sequence) % 2 == 0
            last_node = nodes[0]
            entities = [last_node]
            for i, rel_index in enumerate(sequence[::2]):
                assert rel_index != 0
                next_node = nodes[sequence[2 * i + 1]]
                if rel_index > 0:
                    r = relationships[rel_index - 1]
                    r._start_node = last_node
                    r._end_node = next_node
                    entities.append(r)
                else:
                    r = relationships[-rel_index - 1]
                    r._start_node = next_node
                    r._end_node = last_node
                    entities.append(r)
                last_node = next_node
            return Path(*entities)


class Entity(Mapping):
    """ Base class for :class:`.Node` and :class:`.Relationship` that
    provides :class:`.Graph` membership and property containment
    functionality.
    """

    def __init__(self, graph, id, properties):
        self._graph = graph
        self._id = id
        self._properties = dict((k, v) for k, v in (properties or {}).items() if v is not None)

    def __eq__(self, other):
        try:
            return type(self) == type(other) and self.graph == other.graph and self.id == other.id
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.id)

    def __len__(self):
        return len(self._properties)

    def __getitem__(self, name):
        return self._properties.get(name)

    def __contains__(self, name):
        return name in self._properties

    def __iter__(self):
        return iter(self._properties)

    @property
    def graph(self):
        """ The :class:`.Graph` to which this entity belongs.
        """
        return self._graph

    @property
    def id(self):
        """ The identity of this entity in its container :class:`.Graph`.
        """
        return self._id

    def get(self, name, default=None):
        """ Get a property value by name, optionally with a default.
        """
        return self._properties.get(name, default)

    def keys(self):
        """ Return an iterable of all property names.
        """
        return self._properties.keys()

    def values(self):
        """ Return an iterable of all property values.
        """
        return self._properties.values()

    def items(self):
        """ Return an iterable of all property name-value pairs.
        """
        return self._properties.items()


class EntitySetView(Mapping):
    """ View of a set of :class:`.Entity` instances within a :class:`.Graph`.
    """

    def __init__(self, entity_dict):
        self._entity_dict = entity_dict

    def __getitem__(self, e_id):
        return self._entity_dict[e_id]

    def __len__(self):
        return len(self._entity_dict)

    def __iter__(self):
        return iter(self._entity_dict.values())


class Node(Entity):
    """ Self-contained graph node.
    """

    def __init__(self, graph, n_id, n_labels=None, properties=None):
        Entity.__init__(self, graph, n_id, properties)
        self._labels = frozenset(n_labels or ())

    def __repr__(self):
        return "<Node id=%r labels=%r properties=%r>" % (self._id, self._labels, self._properties)

    @property
    def labels(self):
        """ The set of labels attached to this node.
        """
        return self._labels


class Relationship(Entity):
    """ Self-contained graph relationship.
    """

    def __init__(self, graph, r_id, properties):
        Entity.__init__(self, graph, r_id, properties)
        self._start_node = None
        self._end_node = None

    def __repr__(self):
        return "<Relationship id=%r nodes=(%r, %r) type=%r properties=%r>" % (
            self._id, self._start_node, self._end_node, self.type, self._properties)

    @property
    def nodes(self):
        """ The pair of nodes which this relationship connects.
        """
        return self._start_node, self._end_node

    @property
    def start_node(self):
        """ The start node of this relationship.
        """
        return self._start_node

    @property
    def end_node(self):
        """ The end node of this relationship.
        """
        return self._end_node

    @property
    def type(self):
        """ The type name of this relationship.
        This is functionally equivalent to ``type(relationship).__name__``.
        """
        return type(self).__name__


class Path:
    """ Self-contained graph path.
    """

    def __init__(self, start_node, *relationships):
        assert isinstance(start_node, Node)
        nodes = [start_node]
        for i, relationship in enumerate(relationships, start=1):
            assert isinstance(relationship, Relationship)
            if relationship.start_node == nodes[-1]:
                nodes.append(relationship.end_node)
            elif relationship.end_node == nodes[-1]:
                nodes.append(relationship.start_node)
            else:
                raise ValueError("Relationship %d does not connect to the last node" % i)
        self._nodes = tuple(nodes)
        self._relationships = relationships

    def __repr__(self):
        return "<Path start=%r end=%r size=%s>" % \
               (self.start_node, self.end_node, len(self))

    def __eq__(self, other):
        try:
            return self.start_node == other.start_node and self.relationships == other.relationships
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = hash(self._nodes[0])
        for relationship in self._relationships:
            value ^= hash(relationship)
        return value

    def __len__(self):
        return len(self._relationships)

    def __iter__(self):
        return iter(self._relationships)

    @property
    def graph(self):
        """ The :class:`.Graph` to which this path belongs.
        """
        return self._nodes[0].graph

    @property
    def nodes(self):
        """ The sequence of :class:`.Node` objects in this path.
        """
        return self._nodes

    @property
    def start_node(self):
        """ The first :class:`.Node` in this path.
        """
        return self._nodes[0]

    @property
    def end_node(self):
        """ The last :class:`.Node` in this path.
        """
        return self._nodes[-1]

    @property
    def relationships(self):
        """ The sequence of :class:`.Relationship` objects in this path.
        """
        return self._relationships
