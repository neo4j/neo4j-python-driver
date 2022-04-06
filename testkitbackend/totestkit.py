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


import math

from neo4j import (
    _exceptions as _neo_exc,
    exceptions as neo_exc,
)
from neo4j.graph import (
    Node,
    Path,
    Relationship,
)

from .exceptions import MarkdAsDriverException


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
        node = {
            "id": field(v.id),
            "labels": field(v.labels),
            "props": field(v._properties),
            "elementId": field(v.element_id),
        }
        return {"name": "Node", "data": node}
    if isinstance(v, Relationship):
        rel = {
            "id": field(v.id),
            "startNodeId": field(v.start_node.id),
            "endNodeId": field(v.end_node.id),
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

    raise Exception("Unhandled type:" + str(type(v)))


_error_classifier = None


def _get_error_classifier():
    global _error_classifier

    if _error_classifier is not None:
        return _error_classifier

    _error_classifier = [

    ]


def error(exc):
    payload = {}

    if isinstance(exc, MarkdAsDriverException):
        exc = exc.wrapped_exc

    if isinstance(exc, neo_exc.Neo4jError) and exc.message is not None:
        payload["msg"] = str(exc.message)
    elif exc.args:
        payload["msg"] = str(exc.args[0])
    else:
        payload["msg"] = ""

    if isinstance(exc, neo_exc.Neo4jError):
        payload["code"] = exc.code

    error_type = str(type(exc))

    if isinstance(exc, neo_exc.SessionExpired):
        error_type = "session_expired_error"
    elif isinstance(exc, neo_exc.IncompleteCommit):
        error_type = "incomplete_commit_error"
    elif isinstance(exc, neo_exc.ServiceUnavailable):
        error_type = "service_unavailable_error"
    elif isinstance(exc, neo_exc.NotALeader):
        error_type = "not_leader_error"
    elif isinstance(exc, neo_exc.ForbiddenOnReadOnlyDatabase):
        error_type = "forbidden_on_read_only_database_error"
    elif isinstance(exc, AttributeError):
        error_type = "illegal_state_error"
    elif isinstance(exc, neo_exc.TokenExpired):
        error_type = "token_expired_error"
    elif isinstance(exc, neo_exc.ResultNotSingleError):
        error_type = "not_single_error"
    elif isinstance(exc, neo_exc.ConfigurationError):
        error_type = "invalid_configuration_error"
    elif isinstance(exc, neo_exc.ResultConsumedError):
        error_type = "result_consumed_error"
    elif isinstance(exc, (TypeError, ValueError)):
        error_type = "illegal_argument_error"
    elif isinstance(exc, _neo_exc.BoltProtocolError):
        error_type = "protocol_error"
    elif isinstance(exc, neo_exc.TransactionError):
        error_type = "transaction_error"
    elif isinstance(exc, neo_exc.UnsupportedServerProduct):
        error_type = "untrusted_server_error"
    elif isinstance(exc, neo_exc.TransientError):
        if exc.code == "Neo.ClientError.Security.AuthorizationExpired":
            # we should probably not remap this error to TransientError in the
            # driver...
            error_type = "authorization_expired_error"
        else:
            error_type = "transient_error"
    elif isinstance(exc, neo_exc.ClientError):
        if exc.code and exc.code.startswith("Neo.ClientError.Security."):
            # maybe there should be a SecurityError class...
            error_type = "security_error"
        else:
            error_type = "client_error"

    payload["errorType"] = error_type

    return payload
