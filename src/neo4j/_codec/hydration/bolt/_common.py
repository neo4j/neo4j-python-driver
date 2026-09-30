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

import abc

from .... import _typing as t
from ...packstream import Structure
from .._common import (
    BrokenHydrationObject,
    DehydrationHooks,
    GraphHydrator,
    HydrationScope,
)


if t.TYPE_CHECKING:
    from .._common import T_TYPE_MAP_DICT

    T_STRUCT_MAP_DICT: t.TypeAlias = dict[bytes, t.Callable[..., t.Any]]


__all__ = [
    "GraphHydratorBolt",
    "HydrationHandlerBoltBase",
    "HydrationScopeBolt",
]


class HydrationHandlerBoltBase(abc.ABC):
    struct_hydration_functions: T_STRUCT_MAP_DICT

    def __init__(self):
        self.struct_hydration_functions = {}
        self.dehydration_hooks = DehydrationHooks(
            exact_types={}, subtypes={}, exact_values={}
        )

    @abc.abstractmethod
    def new_hydration_scope(self) -> HydrationScopeBolt: ...


class GraphHydratorBolt(GraphHydrator):
    struct_hydration_functions: T_STRUCT_MAP_DICT

    def __init__(self):
        super().__init__()
        self.struct_hydration_functions = {}


class HydrationScopeBolt(HydrationScope):
    _struct_hydration_functions: T_STRUCT_MAP_DICT
    _graph_hydrator: GraphHydratorBolt

    def __init__(
        self,
        hydration_handler: HydrationHandlerBoltBase,
        graph_hydrator: GraphHydratorBolt,
    ) -> None:
        hydration_hooks: T_TYPE_MAP_DICT = {
            Structure: self._hydrate_structure,
            list: self._hydrate_list,
            dict: self._hydrate_dict,
        }
        dehydration_hooks = hydration_handler.dehydration_hooks
        super().__init__(hydration_hooks, dehydration_hooks, graph_hydrator)
        self._struct_hydration_functions = {
            **hydration_handler.struct_hydration_functions,
            **graph_hydrator.struct_hydration_functions,
        }

    def _hydrate_structure(self, value: Structure) -> t.Any:
        f = self._struct_hydration_functions.get(value.tag)
        try:
            if not f:
                raise ValueError(
                    f"Protocol error: unknown Structure tag: {value.tag!r}"
                )
            return f(*value.fields)
        except Exception as e:
            return BrokenHydrationObject(e, value)

    @staticmethod
    def _hydrate_list(value: list) -> t.Any:
        for v in value:
            if isinstance(v, BrokenHydrationObject):
                return BrokenHydrationObject(v.error, value)
        return value

    @staticmethod
    def _hydrate_dict(value: dict) -> t.Any:
        for v in value.values():
            if isinstance(v, BrokenHydrationObject):
                return BrokenHydrationObject(v.error, value)
        return value
