#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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


from abc import ABCMeta, abstractmethod
from collections.abc import Sequence, Set, Mapping
from datetime import date, time, datetime, timedelta
from functools import reduce
from operator import xor as xor_operator

from neo4j.conf import iter_items
from neo4j.graph import Graph, Node, Relationship, Path
from neo4j.packstream import INT64_MIN, INT64_MAX, Structure
from neo4j.spatial import Point, hydrate_point, dehydrate_point
from neo4j.time import Date, Time, DateTime, Duration
from neo4j.time.hydration import (
    hydrate_date, dehydrate_date,
    hydrate_time, dehydrate_time,
    hydrate_datetime, dehydrate_datetime,
    hydrate_duration, dehydrate_duration, dehydrate_timedelta,
)


map_type = type(map(str, range(0)))


class Record(tuple, Mapping):
    """ A :class:`.Record` is an immutable ordered collection of key-value
    pairs. It is generally closer to a :py:class:`namedtuple` than to a
    :py:class:`OrderedDict` inasmuch as iteration of the collection will
    yield values rather than keys.
    """

    __keys = None

    def __new__(cls, iterable=()):
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
        """ In order to be flexible regarding comparison, the equality rules
        for a record permit comparison with any other Sequence or Mapping.

        :param other:
        :return:
        """
        compare_as_sequence = isinstance(other, Sequence)
        compare_as_mapping = isinstance(other, Mapping)
        if compare_as_sequence and compare_as_mapping:
            return list(self) == list(other) and dict(self) == dict(other)
        elif compare_as_sequence:
            return list(self) == list(other)
        elif compare_as_mapping:
            return dict(self) == dict(other)
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return reduce(xor_operator, map(hash, self.items()))

    def __getitem__(self, key):
        if isinstance(key, slice):
            keys = self.__keys[key]
            values = super(Record, self).__getitem__(key)
            return self.__class__(zip(keys, values))
        try:
            index = self.index(key)
        except IndexError:
            return None
        else:
            return super(Record, self).__getitem__(index)

    def __getslice__(self, start, stop):
        key = slice(start, stop)
        keys = self.__keys[key]
        values = tuple(self)[key]
        return self.__class__(zip(keys, values))

    def get(self, key, default=None):
        """ Obtain a value from the record by key, returning a default
        value if the key does not exist.

        :param key: a key
        :param default: default value
        :return: a value
        """
        try:
            index = self.__keys.index(str(key))
        except ValueError:
            return default
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return default

    def index(self, key):
        """ Return the index of the given item.

        :param key: a key
        :return: index
        :rtype: int
        """
        if isinstance(key, int):
            if 0 <= key < len(self.__keys):
                return key
            raise IndexError(key)
        elif isinstance(key, str):
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

        :param key: an index or key
        :param default: default value
        :return: a single value
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
        :rtype: list
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

        :return: a list of value tuples
        :rtype: list
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
        return RecordExporter().transform(dict(self.items(*keys)))


class DataTransformer(metaclass=ABCMeta):
    """ Abstract base class for transforming data from one form into
    another.
    """

    @abstractmethod
    def transform(self, x):
        """ Transform a value, or collection of values.

        :param x: input value
        :return: output value
        """


class RecordExporter(DataTransformer):
    """ Transformer class used by the :meth:`.Record.data` method.
    """

    def transform(self, x):
        if isinstance(x, Node):
            return self.transform(dict(x))
        elif isinstance(x, Relationship):
            return (self.transform(dict(x.start_node)),
                    x.__class__.__name__,
                    self.transform(dict(x.end_node)))
        elif isinstance(x, Path):
            path = [self.transform(x.start_node)]
            for i, relationship in enumerate(x.relationships):
                path.append(self.transform(relationship.__class__.__name__))
                path.append(self.transform(x.nodes[i + 1]))
            return path
        elif isinstance(x, str):
            return x
        elif isinstance(x, Sequence):
            t = type(x)
            return t(map(self.transform, x))
        elif isinstance(x, Set):
            t = type(x)
            return t(map(self.transform, x))
        elif isinstance(x, Mapping):
            t = type(x)
            return t((k, self.transform(v)) for k, v in x.items())
        else:
            return x


class DataHydrator:
    # TODO: extend DataTransformer

    def __init__(self):
        super(DataHydrator, self).__init__()
        self.graph = Graph()
        self.graph_hydrator = Graph.Hydrator(self.graph)
        self.hydration_functions = {
            b"N": self.graph_hydrator.hydrate_node,
            b"R": self.graph_hydrator.hydrate_relationship,
            b"r": self.graph_hydrator.hydrate_unbound_relationship,
            b"P": self.graph_hydrator.hydrate_path,
            b"X": hydrate_point,
            b"Y": hydrate_point,
            b"D": hydrate_date,
            b"T": hydrate_time,         # time zone offset
            b"t": hydrate_time,         # no time zone
            b"F": hydrate_datetime,     # time zone offset
            b"f": hydrate_datetime,     # time zone name
            b"d": hydrate_datetime,     # no time zone
            b"E": hydrate_duration,
        }

    def hydrate(self, values):
        """ Convert PackStream values into native values.
        """

        def hydrate_(obj):
            if isinstance(obj, Structure):
                try:
                    f = self.hydration_functions[obj.tag]
                except KeyError:
                    # If we don't recognise the structure
                    # type, just return it as-is
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

    def hydrate_records(self, keys, record_values):
        for values in record_values:
            yield Record(zip(keys, self.hydrate(values)))


class DataDehydrator:
    # TODO: extend DataTransformer

    @classmethod
    def fix_parameters(cls, parameters):
        if not parameters:
            return {}
        dehydrator = cls()
        try:
            dehydrated, = dehydrator.dehydrate([parameters])
        except TypeError as error:
            value = error.args[0]
            raise TypeError("Parameters of type {} are not supported".format(type(value).__name__))
        else:
            return dehydrated

    def __init__(self):
        self.dehydration_functions = {}
        self.dehydration_functions.update({
            Point: dehydrate_point,
            Date: dehydrate_date,
            date: dehydrate_date,
            Time: dehydrate_time,
            time: dehydrate_time,
            DateTime: dehydrate_datetime,
            datetime: dehydrate_datetime,
            Duration: dehydrate_duration,
            timedelta: dehydrate_timedelta,
        })
        # Allow dehydration from any direct Point subclass
        self.dehydration_functions.update({cls: dehydrate_point for cls in Point.__subclasses__()})

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
            elif isinstance(obj, int):
                if INT64_MIN <= obj <= INT64_MAX:
                    return obj
                raise ValueError("Integer out of bounds (64-bit signed "
                                 "integer values only)")
            elif isinstance(obj, float):
                return obj
            elif isinstance(obj, str):
                return obj
            elif isinstance(obj, (bytes, bytearray)):
                # order is important here - bytes must be checked after str
                return obj
            elif isinstance(obj, (list, map_type)):
                return list(map(dehydrate_, obj))
            elif isinstance(obj, dict):
                if any(not isinstance(key, str) for key in obj.keys()):
                    raise TypeError("Non-string dictionary keys are "
                                    "not supported")
                return {key: dehydrate_(value) for key, value in obj.items()}
            else:
                raise TypeError(obj)

        return tuple(map(dehydrate_, values))
