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

from copy import copy
from dataclasses import dataclass

from ... import _typing as t
from ...graph import Graph


if t.TYPE_CHECKING:
    T_TYPE_MAP_DICT: t.TypeAlias = dict[type, t.Callable[[t.Any], t.Any]]
    T_VALUE_MAP_DICT: t.TypeAlias = dict[t.Any, t.Callable[[t.Any], t.Any]]


@dataclass
class DehydrationHooks:
    exact_values: T_VALUE_MAP_DICT
    exact_types: T_TYPE_MAP_DICT
    subtypes: T_TYPE_MAP_DICT

    def update(
        self,
        *,
        exact_values: T_VALUE_MAP_DICT | None = None,
        exact_types: T_TYPE_MAP_DICT | None = None,
        subtypes: T_TYPE_MAP_DICT | None = None,
    ) -> None:
        exact_values = self._wrap_exact_values(exact_values or {})
        exact_types = exact_types or {}
        subtypes = subtypes or {}
        self.exact_values.update(exact_values)
        self.exact_types.update(exact_types)
        self.subtypes.update(subtypes)

    def extend(
        self,
        *,
        exact_values: T_VALUE_MAP_DICT | None = None,
        exact_types: T_TYPE_MAP_DICT | None = None,
        subtypes: T_TYPE_MAP_DICT | None = None,
    ) -> DehydrationHooks:
        exact_values = self._wrap_exact_values(exact_values or {})
        exact_types = exact_types or {}
        subtypes = subtypes or {}
        return DehydrationHooks(
            exact_values={**self.exact_values, **exact_values},
            exact_types={**self.exact_types, **exact_types},
            subtypes={**self.subtypes, **subtypes},
        )

    def get_transformer(
        self, item: t.Any
    ) -> t.Callable[[t.Any], t.Any] | None:
        try:
            transformer = self.exact_values.get(item)
        except TypeError:
            transformer = None
        if transformer is not None:
            return transformer
        type_ = type(item)
        transformer = self.exact_types.get(type_)
        if transformer is not None:
            return transformer
        return next(
            (
                f
                for super_type, f in self.subtypes.items()
                if isinstance(item, super_type)
            ),
            None,
        )

    @staticmethod
    def _wrap_exact_values(exact_values: T_VALUE_MAP_DICT) -> T_VALUE_MAP_DICT:
        return {ExactValueKey.wrap(k): v for k, v in exact_values.items()}


class ExactValueKey:
    def __init__(self, value: t.Any) -> None:
        self._value = value

    def __eq__(self, other: t.Any) -> bool:
        return self._value is other

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return f"ExactValueKey({self._value!r})"

    @classmethod
    def wrap(cls, value: t.Any) -> t.Self:
        if isinstance(value, cls):
            return value
        return cls(value)


class BrokenHydrationObject:
    """
    Represents an object from the server, not understood by the driver.

    A :class:`neo4j.Record` might contain a ``BrokenHydrationObject`` object
    if the driver received data from the server that it did not understand.
    This can for instance happen if the server sends a zoned datetime with a
    zone name unknown to the driver.

    There is no need to explicitly check for this type. Any method on the
    :class:`neo4j.Record` that would return a ``BrokenHydrationObject``, will
    raise a :exc:`neo4j.exceptions.BrokenRecordError`
    with the original exception as cause.
    """

    def __init__(self, error, raw_data):
        self.error = error
        "The exception raised while decoding the received object."
        self.raw_data = raw_data
        """The raw data that caused the exception."""

    def exception_copy(self):
        exc_copy = copy(self.error)
        exc_copy.with_traceback(self.error.__traceback__)
        return exc_copy


class GraphHydrator:
    graph: Graph

    def __init__(self):
        self.graph = Graph()


class HydrationScope:
    hydration_hooks: T_TYPE_MAP_DICT
    dehydration_hooks: DehydrationHooks
    _graph_hydrator: GraphHydrator

    def __init__(
        self,
        hydration_hooks: T_TYPE_MAP_DICT,
        dehydration_hooks: DehydrationHooks,
        graph_hydrator: GraphHydrator,
    ) -> None:
        self.hydration_hooks = hydration_hooks
        self.dehydration_hooks = dehydration_hooks
        self._graph_hydrator = graph_hydrator

    def get_graph(self):
        return self._graph_hydrator.graph
