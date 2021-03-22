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

from neo4j.work.simple import Query


def to_cypher_and_params(data):
    from .backend import Request
    params = data["params"]
    # Optional
    if params is None:
        return data["cypher"], None
    # Transform the params to Python native
    for p in params:
        if isinstance(params[p], Request):
            params[p].mark_all_as_read(recursive=True)
        params[p] = to_param(params[p])
    return data["cypher"], params


def to_meta_and_timeout(data):
    from .backend import Request
    metadata = data.get('txMeta', None)
    if isinstance(metadata, Request):
        metadata.mark_all_as_read()
    timeout = data.get('timeout', None)
    if timeout:
        timeout = float(timeout) / 1000
    return metadata, timeout


def to_query_and_params(data):
    cypher, param = to_cypher_and_params(data)
    metadata, timeout = to_meta_and_timeout(data)
    query = Query(cypher, metadata=metadata, timeout=timeout)
    return query, param


def to_param(m):
    """ Converts testkit parameter format to driver (python) parameter
    """
    data = m["data"]
    name = m["name"]
    if name == "CypherString":
        return str(data["value"])
    if name == "CypherBool":
        return bool(data["value"])
    if name == "CypherInt":
        return int(data["value"])
    if name == "CypherFloat":
        return float(data["value"])
    if name == "CypherString":
        return str(data["value"])
    if name == "CypherNull":
        return None
    if name == "CypherList":
        ls = []
        for x in data["value"]:
            ls.append(to_param(x))
        return ls
    if name == "CypherMap":
        mp = {}
        for k, v in data["value"].items():
            mp[k] = to_param(v)
        return mp
    raise Exception("Unknown param type " + name)
