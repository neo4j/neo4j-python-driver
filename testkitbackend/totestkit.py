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
from neo4j.graph import (
    Node,
    Path,
    Relationship,
)


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
    if isinstance(v, Node):
        node = {
            "id": field(v.id),
            "labels": field(v.labels),
            "props": field(v._properties),
        }
        return {"name": "Node", "data": node}
    if isinstance(v, Relationship):
        rel = {
            "id": field(v.id),
            "startNodeId": field(v.start_node.id),
            "endNodeId": field(v.end_node.id),
            "type": field(v.type),
            "props": field(v._properties),
        }
        return {"name": "Relationship", "data": rel}
    if isinstance(v, Path):
        path = {
            "nodes": field(list(v.nodes)),
            "relationships": field(list(v.relationships)),
        }
        return {"name": "Path", "data": path}

    raise Exception("Unhandled type:" + str(type(v)))
