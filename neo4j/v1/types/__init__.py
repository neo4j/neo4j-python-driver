#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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
This package contains classes for modelling the standard set of data types
available within a Neo4j graph database. Most non-primitive types are
represented by PackStream structures on the wire before being converted
into concrete values through the PackStreamHydrant.
"""


from collections import Mapping
from functools import reduce
from operator import xor as xor_operator

from neo4j.packstream import Structure
from neo4j.compat import map_type, string, integer, ustr

from .graph import Graph, hydration_functions as graph_hydration_functions, \
                          dehydration_functions as graph_dehydration_functions
from .spatial import hydration_functions as spatial_hydration_functions, \
                     dehydration_functions as spatial_dehydration_functions
from .temporal import hydration_functions as temporal_hydration_functions, \
                      dehydration_functions as temporal_dehydration_functions

# These classes are imported in order to retain backward compatibility with 1.5.
# They should be removed in 2.0.
from .graph import Entity, Node, Relationship, Path


INT64_MIN = -(2 ** 63)
INT64_MAX = (2 ** 63) - 1


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


def fix_parameters(parameters, protocol_version, **kwargs):
    if not parameters:
        return {}
    dehydrator = PackStreamDehydrator(protocol_version, **kwargs)
    try:
        dehydrated, = dehydrator.dehydrate([parameters])
    except TypeError as error:
        value = error.args[0]
        raise TypeError("Parameters of type {} are not supported".format(type(value).__name__))
    else:
        return dehydrated


class Record(tuple, Mapping):
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
        inserted with a value of :const:`None`; indexes provided
        that are out of bounds will trigger an :exc:`IndexError`.

        :param keys: indexes or keys of the items to include; if none
                      are provided, all values will be included
        :return: dictionary of values, keyed by field name
        :raises: :exc:`IndexError` if an out-of-bounds index is specified
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


class PackStreamHydrator(object):

    def __init__(self, protocol_version):
        super(PackStreamHydrator, self).__init__()
        self.graph = Graph()
        self.hydration_functions = {}
        self.hydration_functions.update(graph_hydration_functions(self.graph))
        if protocol_version >= 2:
            self.hydration_functions.update(spatial_hydration_functions())
            self.hydration_functions.update(temporal_hydration_functions())

    def hydrate(self, values):
        """ Convert PackStream values into native values.
        """

        def hydrate_(obj):
            if isinstance(obj, Structure):
                try:
                    f = self.hydration_functions[obj.tag]
                except KeyError:
                    # If we don't recognise the structure type, just return it as-is
                    return obj
                else:
                    return f(*map(hydrate_, obj.fields))
            elif isinstance(obj, list):
                return list(map(hydrate_, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_(value) for key, value in obj.items()}
            else:
                return obj

        return tuple(map(hydrate_, values))


class PackStreamDehydrator(object):

    def __init__(self, protocol_version, supports_bytes=False):
        self.supports_bytes = supports_bytes
        self.dehydration_functions = {}
        self.dehydration_functions.update(graph_dehydration_functions())
        if protocol_version >= 2:
            self.dehydration_functions.update(spatial_dehydration_functions())
            self.dehydration_functions.update(temporal_dehydration_functions())

    def dehydrate(self, values):
        """ Convert native values into PackStream values.
        """

        def dehydrate_(obj):
            try:
                f = self.dehydration_functions[type(obj)]
            except KeyError:
                pass
            else:
                return f(obj)
            if obj is None:
                return None
            elif isinstance(obj, bool):
                return obj
            elif isinstance(obj, integer):
                if INT64_MIN <= obj <= INT64_MAX:
                    return obj
                raise ValueError("Integer out of bounds (64-bit signed integer values only)")
            elif isinstance(obj, float):
                return obj
            elif isinstance(obj, string):
                return ustr(obj)
            elif isinstance(obj, (bytes, bytearray)):  # order is important here - bytes must be checked after string
                if self.supports_bytes:
                    return obj
                else:
                    raise TypeError("This PackSteam channel does not support BYTES (consider upgrading to Neo4j 3.2+)")
            elif isinstance(obj, (list, map_type)):
                return list(map(dehydrate_, obj))
            elif isinstance(obj, dict):
                return {key: dehydrate_(value) for key, value in obj.items()}
            else:
                raise TypeError(obj)

        return tuple(map(dehydrate_, values))
