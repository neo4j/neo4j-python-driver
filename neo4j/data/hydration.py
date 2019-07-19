#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from datetime import date, time, datetime, timedelta

from neo4j.data import Record, Structure
from neo4j.graph import Graph
from neo4j.data.packing import INT64_MIN, INT64_MAX
from neo4j.spatial import Point, hydrate_point, dehydrate_point
from neo4j.time import Date, Time, DateTime, Duration
from neo4j.time.hydration import (
    hydrate_date, dehydrate_date,
    hydrate_time, dehydrate_time,
    hydrate_datetime, dehydrate_datetime,
    hydrate_duration,dehydrate_duration, dehydrate_timedelta,
)


map_type = type(map(str, range(0)))


class DataHydrator(object):

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


class DataDehydrator(object):

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
