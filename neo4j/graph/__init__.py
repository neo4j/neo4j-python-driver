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


__all__ = [
    "Graph",
    "Node",
    "Path",
    "Relationship",
]


from collections.abc import Mapping

from ..meta import (
    deprecated,
    deprecation_warn,
)


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

        def hydrate_node(self, id_, labels=None,
                         properties=None, element_id=None):
            assert isinstance(self.graph, Graph)
            # backwards compatibility with Neo4j < 5.0
            if element_id is None:
                element_id = str(id_)

            try:
                inst = self.graph._nodes[element_id]
            except KeyError:
                inst = self.graph._nodes[element_id] = Node(
                    self.graph, element_id, id_, labels, properties
                )
            else:
                # If we have already hydrated this node as the endpoint of
                # a relationship, it won't have any labels or properties.
                # Therefore, we need to add the ones we have here.
                if labels:
                    inst._labels = inst._labels.union(labels)  # frozen_set
                if properties:
                    inst._properties.update(properties)
            return inst

        def hydrate_relationship(self, id_, n0_id, n1_id, type_,
                                 properties=None, element_id=None,
                                 n0_element_id=None, n1_element_id=None):
            # backwards compatibility with Neo4j < 5.0
            if element_id is None:
                element_id = str(id_)
            if n0_element_id is None:
                n0_element_id = str(n0_id)
            if n1_element_id is None:
                n1_element_id = str(n1_id)

            inst = self.hydrate_unbound_relationship(id_, type_, properties,
                                                     element_id)
            inst._start_node = self.hydrate_node(n0_id,
                                                 element_id=n0_element_id)
            inst._end_node = self.hydrate_node(n1_id, element_id=n1_element_id)
            return inst

        def hydrate_unbound_relationship(self, id_, type_, properties=None,
                                         element_id=None):
            assert isinstance(self.graph, Graph)
            # backwards compatibility with Neo4j < 5.0
            if element_id is None:
                element_id = str(id_)

            try:
                inst = self.graph._relationships[element_id]
            except KeyError:
                r = self.graph.relationship_type(type_)
                inst = self.graph._relationships[element_id] = r(
                    self.graph, element_id, id_, properties
                )
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

    def __init__(self, graph, element_id, id_, properties):
        self._graph = graph
        self._element_id = element_id
        self._id = id_
        self._properties = {
            k: v for k, v in (properties or {}).items() if v is not None
        }

    def __eq__(self, other):
        try:
            return (type(self) == type(other)
                    and self.graph == other.graph
                    and self.element_id == other.element_id)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._element_id)

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
    @deprecated("`id` is deprecated, use `element_id` instead")
    def id(self):
        """The legacy identity of this entity in its container :class:`.Graph`.

        Depending on the version of the server this entity was retrieved from,
        this may be empty (None).

        .. deprecated:: 5.0
            Use :attr:`.element_id` instead.

        :rtype: int
        """
        return self._id

    @property
    def element_id(self):
        """The identity of this entity in its container :class:`.Graph`.

        .. added:: 5.0

        :rtype: str
        """
        return self._element_id

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
        # TODO: 6.0 - remove this compatibility shim
        if isinstance(e_id, (int, float, complex)):
            deprecation_warn(
                "Accessing entities by an integer id is deprecated, "
                "use the new style element_id (str) instead"
            )
            if isinstance(e_id, float) and int(e_id) == e_id:
                # Non-int floats would always fail for legacy IDs
                e_id = int(e_id)
            elif isinstance(e_id, complex) and int(e_id.real) == e_id:
                # complex numbers with imaginary parts or non-integer real
                # parts would always fail for legacy IDs
                e_id = int(e_id.real)
            e_id = str(e_id)
        return self._entity_dict[e_id]

    def __len__(self):
        return len(self._entity_dict)

    def __iter__(self):
        return iter(self._entity_dict.values())


class Node(Entity):
    """ Self-contained graph node.
    """

    def __init__(self, graph, element_id, id_, n_labels=None,
                 properties=None):
        Entity.__init__(self, graph, element_id, id_, properties)
        self._labels = frozenset(n_labels or ())

    def __repr__(self):
        return (f"<Node element_id={self._element_id!r} "
                f"labels={self._labels!r} properties={self._properties!r}>")

    @property
    def labels(self):
        """ The set of labels attached to this node.
        """
        return self._labels


class Relationship(Entity):
    """ Self-contained graph relationship.
    """

    def __init__(self, graph, element_id, id_, properties):
        Entity.__init__(self, graph, element_id, id_, properties)
        self._start_node = None
        self._end_node = None

    def __repr__(self):
        return (f"<Relationship element_id={self._element_id!r} "
                f"nodes={self.nodes!r} type={self.type!r} "
                f"properties={self._properties!r}>")

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
