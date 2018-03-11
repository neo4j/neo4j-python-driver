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
This module contains classes for modelling nodes, relationships and
paths belonging to a Neo4j graph database. The `hydration` function,
also included, allows PackStream structures to be turned into instances
of these classes.
"""

from functools import reduce
from operator import xor as xor_operator

from neo4j.packstream import Structure
from neo4j.compat import string, integer, ustr

from .api import GraphDatabase, Hydrant


def iter_items(iterable):
    """ Iterate through all items (key-value pairs) within an iterable
    dictionary-like object. If the object has a `keys` method, this is
    used along with `__getitem__` to yield each pair in turn. If no
    `keys` method exists, each iterable element is assumed to be a
    2-tuple of key and value.
    """
    if hasattr(iterable, "keys"):
        for key in iterable.keys():
            yield key, iterable[key]
    else:
        for key, value in iterable:
            yield key, value


class Record(tuple):
    """ A :class:`.Record` is an immutable ordered collection of key-value
    pairs. It is generally closer to a :py:class:`namedtuple` than to a
    :py:class:`OrderedDict` inasmuch as iteration of the collection will
    yield values rather than keys.
    """

    __keys = None

    def __new__(cls, iterable):
        keys = []
        values = []
        for key, value in iter_items(iterable):
            keys.append(key)
            values.append(value)
        inst = tuple.__new__(cls, values)
        inst.__keys = tuple(keys)
        return inst

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__,
                            " ".join("%s=%r" % (field, self[i]) for i, field in enumerate(self.__keys)))

    def __eq__(self, other):
        return dict(self) == dict(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return reduce(xor_operator, map(hash, self.items()))

    def __getitem__(self, key):
        if isinstance(key, slice):
            keys = self.__keys[key]
            values = super(Record, self).__getitem__(key)
            return self.__class__(zip(keys, values))
        index = self.index(key)
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return None

    def __getslice__(self, start, stop):
        key = slice(start, stop)
        keys = self.__keys[key]
        values = tuple(self)[key]
        return self.__class__(zip(keys, values))

    def get(self, key, default=None):
        try:
            index = self.__keys.index(ustr(key))
        except ValueError:
            return default
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return default

    def index(self, key):
        """ Return the index of the given item.
        """
        if isinstance(key, integer):
            if 0 <= key < len(self.__keys):
                return key
            raise IndexError(key)
        elif isinstance(key, string):
            try:
                return self.__keys.index(key)
            except ValueError:
                raise KeyError(key)
        else:
            raise TypeError(key)

    def value(self, key=0, default=None):
        """ Obtain a single value from the record by index or key. If no
        index or key is specified, the first value is returned. If the
        specified item does not exist, the default value is returned.

        :param key:
        :param default:
        :return:
        """
        try:
            index = self.index(key)
        except (IndexError, KeyError):
            return default
        else:
            return self[index]

    def keys(self):
        """ Return the keys of the record.

        :return: list of key names
        """
        return list(self.__keys)

    def values(self, *keys):
        """ Return the values of the record, optionally filtering to
        include only certain values by index or key.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: list of values
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append(None)
                else:
                    d.append(self[i])
            return d
        return list(self)

    def items(self, *keys):
        """ Return the fields of the record as a list of key and value tuples

        :return:
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append((key, None))
                else:
                    d.append((self.__keys[i], self[i]))
            return d
        return list((self.__keys[i], super(Record, self).__getitem__(i)) for i in range(len(self)))

    def data(self, *keys):
        """ Return the keys and values of this record as a dictionary,
        optionally including only certain values by index or key. Keys
        provided in the items that are not in the record will be
        inserted with a value of :py:const:`None`; indexes provided
        that are out of bounds will trigger an :py:`IndexError`.

        :param keys: indexes or keys of the items to include; if none
                      are provided, all values will be included
        :return: dictionary of values, keyed by field name
        :raises: :py:`IndexError` if an out-of-bounds index is specified
        """
        if keys:
            d = {}
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d[key] = None
                else:
                    d[self.__keys[i]] = self[i]
            return d
        return dict(self)


class Graph(object):

    def __init__(self):
        self.__nodes = {}
        self.__relationships = {}


class Entity(object):
    """ Base class for Node and Relationship.
    """
    graph = None
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


class Relationship(Entity):
    """ Self-contained graph relationship.
    """

    #: The start node of this relationship
    start = None

    #: The end node of this relationship
    end = None

    #: The type of this relationship
    type = None

    @classmethod
    def hydrate(cls, id_, start, end, type, properties=None):
        inst = cls(start, end, type, properties)
        inst.id = id_
        return inst

    @classmethod
    def hydrate_unbound(cls, id_, type, properties=None):
        return cls.hydrate(id_, None, None, type, properties)

    def __init__(self, start, end, type, properties=None, **kwproperties):
        super(Relationship, self).__init__(properties, **kwproperties)
        self.start = start
        self.end = end
        self.type = type

    def __repr__(self):
        return "<Relationship id=%r start=%r end=%r type=%r properties=%r>" % \
               (self.id, self.start, self.end, self.type, self.properties)

    @property
    def nodes(self):
        return self.start, self.end


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
                r = rels[rel_index - 1]
                r.start = last_node.id
                r.end = next_node.id
                entities.append(r)
            else:
                r = rels[-rel_index - 1]
                r.start = next_node.id
                r.end = last_node.id
                entities.append(r)
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
    """ A point in 2-dimensional Cartesian space.
    """

    crs = 7203

    @property
    def x(self):
        return self[0]

    @property
    def y(self):
        return self[1]


class CartesianPoint3D(CartesianPoint):
    """ A point in 3-dimensional Cartesian space.
    """

    crs = 9157

    @property
    def z(self):
        return self[2]


class WGS84Point(Point):
    """

    """

    crs = 4326

    @property
    def longitude(self):
        return self[0]

    @property
    def latitude(self):
        return self[1]


class WGS84Point3D(WGS84Point):
    """

    """

    crs = 4979

    @property
    def height(self):
        return self[2]


class PackStreamHydrant(Hydrant):

    def __init__(self, graph):
        super(PackStreamHydrant, self).__init__()
        self.graph = graph
        self.structure_hydrants = {
            b"N": Node.hydrate,
            b"R": Relationship.hydrate,
            b"r": Relationship.hydrate_unbound,
            b"P": Path.hydrate,
            b"X": Point.hydrate,
            b"Y": Point.hydrate,
        }

    def hydrate(self, values):

        def hydrate_(obj):
            if isinstance(obj, Structure):
                tag, args = obj
                try:
                    hydrant = self.structure_hydrants[tag]
                except KeyError:
                    # If we don't recognise the structure type, just return it as-is
                    return obj
                else:
                    return hydrant(*map(hydrate_, args))
            elif isinstance(obj, list):
                return list(map(hydrate_, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_(value) for key, value in obj.items()}
            else:
                return obj

        return tuple(map(hydrate_, values))
