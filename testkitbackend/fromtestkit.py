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

def toCypherAndParams(data):
    cypher = data["cypher"]
    params = data["params"]
    # Optional
    if params is None:
        return cypher, None
    # Transform the params to Python native
    for p in params:
        params[p] = toParam(params[p])
    return cypher, params


def toParam(m):
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
            ls.append(toParam(x))
        return ls
    if name == "CypherMap":
        mp = {}
        for k, v in data["value"].items():
            mp[k] = toParam(v)
        return mp
    raise Exception("Unknown param type " + name)
