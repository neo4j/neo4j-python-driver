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


from neo4j import Query


def to_cypher_and_params(data):
    from .backend import Request
    params = data["params"]
    # Optional
    if params is None:
        return data["cypher"], None
    # Transform the params to Python native
    params_dict = {p: to_param(params[p]) for p in params}
    return data["cypher"], params_dict


def to_tx_kwargs(data):
    from .backend import Request
    kwargs = {}
    if "txMeta" in data:
        kwargs["metadata"] = data["txMeta"]
        if isinstance(kwargs["metadata"], Request):
            kwargs["metadata"].mark_all_as_read()
    if "timeout" in data:
        kwargs["timeout"] = data["timeout"]
        if kwargs["timeout"] is not None:
            kwargs["timeout"] /= 1000
    return kwargs


def to_query_and_params(data):
    cypher, param = to_cypher_and_params(data)
    tx_kwargs = to_tx_kwargs(data)
    query = Query(cypher, **tx_kwargs)
    return query, param


def to_param(m):
    """ Converts testkit parameter format to driver (python) parameter
    """
    value = m["data"]["value"]
    name = m["name"]
    if name == "CypherNull":
        return None
    if name == "CypherString":
        return str(value)
    if name == "CypherBool":
        return bool(value)
    if name == "CypherInt":
        return int(value)
    if name == "CypherFloat":
        return float(value)
    if name == "CypherString":
        return str(value)
    if name == "CypherBytes":
        return bytearray([int(byte, 16) for byte in value.split()])
    if name == "CypherList":
        return [to_param(v) for v in value]
    if name == "CypherMap":
        return {k: to_param(value[k]) for k in value}
    raise Exception("Unknown param type " + name)
