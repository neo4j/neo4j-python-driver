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


from __future__ import annotations

import math

import neo4j
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
    return {"values": [field(f) for f in rec]}


def summary(summary_: neo4j.ResultSummary) -> dict:
    def serialize_notification(n: neo4j.SummaryNotification) -> dict:
        res: dict = {
            "title": n.title,
            "code": n.code,
            "description": n.description,
            "severityLevel": n.severity_level.name,
            "category": n.category.name,
            "severity": n.raw_severity_level,
            "rawCategory": n.raw_category,
            "rawSeverityLevel": n.raw_severity_level,
        }
        if n.position is not None:
            res["position"] = {
                "column": n.position.column,
                "offset": n.position.offset,
                "line": n.position.line,
            }
        return res

    def serialize_notifications() -> list[dict] | None:
        if summary_.notifications is None:
            gql_aware_protocol = summary_.server.protocol_version >= (5, 5)
            return [] if gql_aware_protocol else None
        return [
            serialize_notification(n) for n in summary_.summary_notifications
        ]

    def serialize_gql_status_object(o: neo4j.GqlStatusObject) -> dict:
        res: dict = {
            "isNotification": o.is_notification,
            "gqlStatus": o.gql_status,
            "statusDescription": o.status_description,
            "rawClassification": o.raw_classification,
            "classification": o.classification,
            "rawSeverity": o.raw_severity,
            "severity": o.severity,
            "diagnosticRecord": {
                k: field(v) for k, v in o.diagnostic_record.items()
            },
        }
        position = o.position
        if position is not None:
            res["position"] = {
                "column": position.column,
                "offset": position.offset,
                "line": position.line,
            }
        else:
            res["position"] = None
        return res

    def serialize_gql_status_objects() -> list[dict]:
        with warning_check(neo4j.PreviewWarning, r".*\bGQLSTATUS\b.*"):
            return [
                serialize_gql_status_object(o)
                for o in summary_.gql_status_objects
            ]

    def format_address(address: neo4j.Address):
        if len(address) == 2:
            return f"{address.host}:{address.port}"
        if len(address) == 4:
            return f"[{address.host}]:{address.port}"
        else:
            raise ValueError(f"Unexpected address format: {address}")

    counters = summary_.counters

    return {
        "serverInfo": {
            "address": format_address(summary_.server.address),
            "agent": summary_.server.agent,
            "protocolVersion": ".".join(
                map(str, summary_.server.protocol_version)
            ),
        },
        "counters": (
            None
            if not counters
            else {
                "constraintsAdded": counters.constraints_added,
                "constraintsRemoved": counters.constraints_removed,
                "containsSystemUpdates": counters.contains_system_updates,
                "containsUpdates": counters.contains_updates,
                "indexesAdded": counters.indexes_added,
                "indexesRemoved": counters.indexes_removed,
                "labelsAdded": counters.labels_added,
                "labelsRemoved": counters.labels_removed,
                "nodesCreated": counters.nodes_created,
                "nodesDeleted": counters.nodes_deleted,
                "propertiesSet": counters.properties_set,
                "relationshipsCreated": counters.relationships_created,
                "relationshipsDeleted": counters.relationships_deleted,
                "systemUpdates": counters.system_updates,
            }
        ),
        "database": summary_.database,
        "notifications": serialize_notifications(),
        "gqlStatusObjects": serialize_gql_status_objects(),
        "plan": summary_.plan,
        "profile": summary_.profile,
        "query": {
            "text": summary_.query,
            "parameters": {
                k: field(v) for k, v in (summary_.parameters or {}).items()
            },
        },
        "queryType": summary_.query_type,
        "resultAvailableAfter": summary_.result_available_after,
        "resultConsumedAfter": summary_.result_consumed_after,
    }


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
    if isinstance(v, (list, frozenset, set)):
        return to("CypherList", list(map(field, v)))
    if isinstance(v, dict):
        return to("CypherMap", {k: field(x) for k, x in v.items()})
    if isinstance(v, (bytes, bytearray)):
        return to("CypherBytes", " ".join(f"{byte:02x}" for byte in v))
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
                "z": getattr(v, "z", None),
            },
        }
    if isinstance(v, WGS84Point):
        return {
            "name": "CypherPoint",
            "data": {
                "system": "wgs84",
                "x": v.x,
                "y": v.y,
                "z": getattr(v, "z", None),
            },
        }
    if isinstance(v, Date):
        return {
            "name": "CypherDate",
            "data": {"year": v.year, "month": v.month, "day": v.day},
        }
    if isinstance(v, Time):
        data = {
            "hour": v.hour,
            "minute": v.minute,
            "second": v.second,
            "nanosecond": v.nanosecond,
        }
        if v.tzinfo is not None:
            data["utc_offset_s"] = v.tzinfo.utcoffset(v).total_seconds()
        return {"name": "CypherTime", "data": data}
    if isinstance(v, DateTime):
        data = {
            "year": v.year,
            "month": v.month,
            "day": v.day,
            "hour": v.hour,
            "minute": v.minute,
            "second": v.second,
            "nanosecond": v.nanosecond,
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
                "nanoseconds": v.nanoseconds,
            },
        }

    raise ValueError("Unhandled type:" + str(type(v)))


def auth_token(auth):
    return {"name": "AuthorizationToken", "data": vars(auth)}
