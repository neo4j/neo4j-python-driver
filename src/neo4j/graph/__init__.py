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
Graph data types
"""


from __future__ import annotations

import typing as t
from collections.abc import Mapping

from .._meta import (
    deprecated,
    deprecation_warn,
)


__all__ = [
    "Graph",
    "Node",
    "Path",
    "Relationship",
]


_T = t.TypeVar("_T")


class Graph:
    """A graph of nodes and relationships.

    Local, self-contained graph object that acts as a container for
    :class:`.Node` and :class:`.Relationship` instances.
    This is typically obtained via :meth:`.Result.graph` or
    :meth:`.AsyncResult.graph`.
    """

    def __init__(self) -> None:
        self._nodes: t.Dict[str, Node] = {}
        self._legacy_nodes: t.Dict[int, Node] = {}  # TODO: 6.0 - remove
        self._relationships: t.Dict[str, Relationship] = {}
        # TODO: 6.0 - remove
        self._legacy_relationships: t.Dict[int, Relationship] = {}
        self._relationship_types: t.Dict[str, t.Type[Relationship]] = {}
        self._node_set_view = EntitySetView(self._nodes, self._legacy_nodes)
        self._relationship_set_view = EntitySetView(self._relationships,
                                                    self._legacy_relationships)

    @property
    def nodes(self) -> EntitySetView[Node]:
        """ Access a set view of the nodes in this graph.
        """
        return self._node_set_view

    @property
    def relationships(self) -> EntitySetView[Relationship]:
        """ Access a set view of the relationships in this graph.
        """
        return self._relationship_set_view

    def relationship_type(self, name: str) -> t.Type[Relationship]:
        """ Obtain a :class:`.Relationship` subclass for a given
        relationship type name.
        """
        try:
            cls = self._relationship_types[name]
        except KeyError:
            cls = self._relationship_types[name] = t.cast(
                t.Type[Relationship],
                type(str(name), (Relationship,), {})
            )
        return cls


class Entity(t.Mapping[str, t.Any]):
    """ Base class for :class:`.Node` and :class:`.Relationship` that
    provides :class:`.Graph` membership and property containment
    functionality.
    """

    def __init__(
        self,
        graph: Graph,
        element_id: str,
        id_: int,
        properties: t.Optional[t.Dict[str, t.Any]]
    ) -> None:
        self._graph = graph
        self._element_id = element_id
        self._id = id_
        self._properties = {
            k: v for k, v in (properties or {}).items() if v is not None
        }

    def __eq__(self, other: t.Any) -> bool:
        try:
            return (type(self) == type(other)
                    and self.graph == other.graph
                    and self.element_id == other.element_id)
        except AttributeError:
            return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._element_id)

    def __len__(self) -> int:
        return len(self._properties)

    def __getitem__(self, name: str) -> t.Any:
        return self._properties.get(name)

    def __contains__(self, name: object) -> bool:
        return name in self._properties

    def __iter__(self) -> t.Iterator[str]:
        return iter(self._properties)

    @property
    def graph(self) -> Graph:
        """ The :class:`.Graph` to which this entity belongs.
        """
        return self._graph

    @property  # type: ignore
    @deprecated("`id` is deprecated, use `element_id` instead")
    def id(self) -> int:
        """The legacy identity of this entity in its container :class:`.Graph`.

        Depending on the version of the server this entity was retrieved from,
        this may be empty (None).

        .. warning::
            This value can change for the same entity across multiple
            transactions. Don't rely on it for cross-transactional
            computations.

        .. deprecated:: 5.0
            Use :attr:`.element_id` instead.
        """
        return self._id

    @property
    def element_id(self) -> str:
        """The identity of this entity in its container :class:`.Graph`.

        .. warning::
            This value can change for the same entity across multiple
            transactions. Don't rely on it for cross-transactional
            computations.

        .. versionadded:: 5.0
        """
        return self._element_id

    def get(self, name: str, default: t.Optional[object] = None) -> t.Any:
        """ Get a property value by name, optionally with a default.
        """
        return self._properties.get(name, default)

    def keys(self) -> t.KeysView[str]:
        """ Return an iterable of all property names.
        """
        return self._properties.keys()

    def values(self) -> t.ValuesView[t.Any]:
        """ Return an iterable of all property values.
        """
        return self._properties.values()

    def items(self) -> t.ItemsView[str, t.Any]:
        """ Return an iterable of all property name-value pairs.
        """
        return self._properties.items()


class EntitySetView(Mapping, t.Generic[_T]):
    """ View of a set of :class:`.Entity` instances within a :class:`.Graph`.
    """

    def __init__(
        self,
        entity_dict: t.Dict[str, _T],
        legacy_entity_dict: t.Dict[int, _T],
    ) -> None:
        self._entity_dict = entity_dict
        self._legacy_entity_dict = legacy_entity_dict  # TODO: 6.0 - remove

    def __getitem__(self, e_id: t.Union[int, str]) -> _T:
        # TODO: 6.0 - remove this compatibility shim
        if isinstance(e_id, (int, float, complex)):
            deprecation_warn(
                "Accessing entities by an integer id is deprecated, "
                "use the new style element_id (str) instead"
            )
            return self._legacy_entity_dict[e_id]
        return self._entity_dict[e_id]

    def __len__(self) -> int:
        return len(self._entity_dict)

    def __iter__(self) -> t.Iterator[_T]:
        return iter(self._entity_dict.values())


class Node(Entity):
    """ Self-contained graph node.
    """

    def __init__(
        self,
        graph: Graph,
        element_id: str,
        id_: int,
        n_labels: t.Optional[t.Iterable[str]] = None,
        properties: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        Entity.__init__(self, graph, element_id, id_, properties)
        self._labels = frozenset(n_labels or ())

    def __repr__(self) -> str:
        return (f"<Node element_id={self._element_id!r} "
                f"labels={self._labels!r} properties={self._properties!r}>")

    @property
    def labels(self) -> t.FrozenSet[str]:
        """ The set of labels attached to this node.
        """
        return self._labels


class Relationship(Entity):
    """ Self-contained graph relationship.
    """

    def __init__(
        self,
        graph: Graph,
        element_id: str,
        id_: int,
        properties: t.Dict[str, t.Any],
    ) -> None:
        Entity.__init__(self, graph, element_id, id_, properties)
        self._start_node: t.Optional[Node] = None
        self._end_node: t.Optional[Node] = None

    def __repr__(self) -> str:
        return (f"<Relationship element_id={self._element_id!r} "
                f"nodes={self.nodes!r} type={self.type!r} "
                f"properties={self._properties!r}>")

    @property
    def nodes(self) -> t.Tuple[t.Optional[Node], t.Optional[Node]]:
        """ The pair of nodes which this relationship connects.
        """
        return self._start_node, self._end_node

    @property
    def start_node(self) -> t.Optional[Node]:
        """ The start node of this relationship.
        """
        return self._start_node

    @property
    def end_node(self) -> t.Optional[Node]:
        """ The end node of this relationship.
        """
        return self._end_node

    @property
    def type(self) -> str:
        """ The type name of this relationship.
        This is functionally equivalent to ``type(relationship).__name__``.
        """
        return type(self).__name__


class Path:
    """ Self-contained graph path.
    """

    def __init__(self, start_node: Node, *relationships: Relationship) -> None:
        assert isinstance(start_node, Node)
        nodes = [start_node]
        for i, relationship in enumerate(relationships, start=1):
            assert isinstance(relationship, Relationship)
            if relationship.start_node == nodes[-1]:
                nodes.append(t.cast(Node, relationship.end_node))
            elif relationship.end_node == nodes[-1]:
                nodes.append(t.cast(Node, relationship.start_node))
            else:
                raise ValueError("Relationship %d does not connect to the last node" % i)
        self._nodes = tuple(nodes)
        self._relationships = relationships

    def __repr__(self) -> str:
        return "<Path start=%r end=%r size=%s>" % \
               (self.start_node, self.end_node, len(self))

    def __eq__(self, other: t.Any) -> bool:
        try:
            return self.start_node == other.start_node and self.relationships == other.relationships
        except AttributeError:
            return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self):
        value = hash(self._nodes[0])
        for relationship in self._relationships:
            value ^= hash(relationship)
        return value

    def __len__(self) -> int:
        return len(self._relationships)

    def __iter__(self) -> t.Iterator[Relationship]:
        return iter(self._relationships)

    @property
    def graph(self) -> Graph:
        """ The :class:`.Graph` to which this path belongs.
        """
        return self._nodes[0].graph

    @property
    def nodes(self) -> t.Tuple[Node, ...]:
        """ The sequence of :class:`.Node` objects in this path.
        """
        return self._nodes

    @property
    def start_node(self) -> Node:
        """ The first :class:`.Node` in this path.
        """
        return self._nodes[0]

    @property
    def end_node(self) -> Node:
        """ The last :class:`.Node` in this path.
        """
        return self._nodes[-1]

    @property
    def relationships(self) -> t.Tuple[Relationship, ...]:
        """ The sequence of :class:`.Relationship` objects in this path.
        """
        return self._relationships
