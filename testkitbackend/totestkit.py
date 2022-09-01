# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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


import math

from neo4j.graph import (
    Node,
    Path,
    Relationship,
)
from neo4j.spatial import (
    CartesianPoint,
    WGS84Point,
)
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)

from ._warning_check import warning_check


def record(rec):
    fields = []
    for f in rec:
        fields.append(field(f))
    return {"values": fields}


def field(v):
    def to(name, val):
        return {"name": name, "data": {"value": val}}

    if v is None:
        return {"name": "CypherNull"}
    if isinstance(v, bool):
        return to("CypherBool", v)
    if isinstance(v, int):
        return to("CypherInt", v)
    if isinstance(v, float):
        if math.isinf(v):
            return to("CypherFloat", "+Infinity" if v > 0 else "-Infinity")
        if math.isnan(v):
            return to("CypherFloat", "NaN")
        return to("CypherFloat", v)
    if isinstance(v, str):
        return to("CypherString", v)
    if isinstance(v, list) or isinstance(v, frozenset) or isinstance(v, set):
        ls = []
        for x in v:
            ls.append(field(x))
        return to("CypherList", ls)
    if isinstance(v, dict):
        mp = {}
        for k, v in v.items():
            mp[k] = field(v)
        return to("CypherMap", mp)
    if isinstance(v, (bytes, bytearray)):
        return to("CypherBytes", " ".join("{:02x}".format(byte) for byte in v))
    if isinstance(v, Node):
        with warning_check(
            DeprecationWarning, "`id` is deprecated, use `element_id` instead"
        ):
            id_ = v.id
        node = {
            "id": field(id_),
            "labels": field(v.labels),
            "props": field(v._properties),
            "elementId": field(v.element_id),
        }
        return {"name": "Node", "data": node}
    if isinstance(v, Relationship):
        with warning_check(
            DeprecationWarning, "`id` is deprecated, use `element_id` instead"
        ):
            id_ = v.id
        with warning_check(
            DeprecationWarning, "`id` is deprecated, use `element_id` instead"
        ):
            start_id = v.start_node.id
        with warning_check(
            DeprecationWarning, "`id` is deprecated, use `element_id` instead"
        ):
            end_id = v.end_node.id
        rel = {
            "id": field(id_),
            "startNodeId": field(start_id),
            "endNodeId": field(end_id),
            "type": field(v.type),
            "props": field(v._properties),
            "elementId": field(v.element_id),
            "startNodeElementId": field(v.start_node.element_id),
            "endNodeElementId": field(v.end_node.element_id),
        }
        return {"name": "Relationship", "data": rel}
    if isinstance(v, Path):
        path = {
            "nodes": field(list(v.nodes)),
            "relationships": field(list(v.relationships)),
        }
        return {"name": "Path", "data": path}
    if isinstance(v, CartesianPoint):
        return {
            "name": "CypherPoint",
            "data": {
                "system": "cartesian",
                "x": v.x,
                "y": v.y,
                "z": getattr(v, "z", None)
            },
        }
    if isinstance(v, WGS84Point):
        return {
            "name": "CypherPoint",
            "data": {
                "system": "wgs84",
                "x": v.x,
                "y": v.y,
                "z": getattr(v, "z", None)
            },
        }
    if isinstance(v, Date):
        return {
            "name": "CypherDate",
            "data": {
                "year": v.year,
                "month": v.month,
                "day": v.day
            }
        }
    if isinstance(v, Time):
        data = {
            "hour": v.hour,
            "minute": v.minute,
            "second": v.second,
            "nanosecond": v.nanosecond
        }
        if v.tzinfo is not None:
            data["utc_offset_s"] = v.tzinfo.utcoffset(v).total_seconds()
        return {
            "name": "CypherTime",
            "data": data
        }
    if isinstance(v, DateTime):
        data = {
            "year": v.year,
            "month": v.month,
            "day": v.day,
            "hour": v.hour,
            "minute": v.minute,
            "second": v.second,
            "nanosecond": v.nanosecond
        }
        if v.tzinfo is not None:
            data["utc_offset_s"] = v.tzinfo.utcoffset(v).total_seconds()
            for attr in ("zone", "key"):
                timezone_id = getattr(v.tzinfo, attr, None)
                if isinstance(timezone_id, str):
                    data["timezone_id"] = timezone_id
        return {
            "name": "CypherDateTime",
            "data": data,
        }
    if isinstance(v, Duration):
        return {
            "name": "CypherDuration",
            "data": {
                "months": v.months,
                "days": v.days,
                "seconds": v.seconds,
                "nanoseconds": v.nanoseconds
             },
        }

    raise ValueError("Unhandled type:" + str(type(v)))
