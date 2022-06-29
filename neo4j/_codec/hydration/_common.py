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


from ...graph import Graph
from ...packstream import Structure


class GraphHydrator:
    def __init__(self):
        self.graph = Graph()
        self.struct_hydration_functions = {}


class HydrationScope:

    def __init__(self, hydration_handler, graph_hydrator):
        self._hydration_handler = hydration_handler
        self._graph_hydrator = graph_hydrator
        self._struct_hydration_functions = {
            **hydration_handler.struct_hydration_functions,
            **graph_hydrator.struct_hydration_functions,
        }
        self.hydration_hooks = {
            Structure: self._hydrate_structure,
        }
        self.dehydration_hooks = hydration_handler.dehydration_functions

    def _hydrate_structure(self, value):
        f = self._struct_hydration_functions.get(value.tag)
        if not f:
            return value
        return f(*value.fields)

    def get_graph(self):
        return self._graph_hydrator.graph
