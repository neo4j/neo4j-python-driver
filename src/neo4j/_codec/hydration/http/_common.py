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
from dataclasses import dataclass

from .... import _typing as t
from ...._exceptions import QueryApiHttpError
from .._common import (
    BrokenHydrationObject,
    DehydrationHooks,
    GraphHydrator,
    HydrationScope,
)


T_JSON: t.TypeAlias = t.Union[
    dict[str, "T_JSON_BOUND"],
    list["T_JSON_BOUND"],
    str,
    int,
    float,
    bool,
    "LiteralJson[T_JSON_BOUND]",
    "LiteralJsonRecursive[T_JSON_BOUND]",
    None,
]
T_JSON_BOUND = t.TypeVar("T_JSON_BOUND", bound=T_JSON)

_default = object()


if t.TYPE_CHECKING:
    from .._common import T_TYPE_MAP_DICT

    T = t.TypeVar("T")

    T_STRUCT_MAP_DICT: t.TypeAlias = dict[str, t.Callable[[t.Any], t.Any]]
    ValueDict = t.TypedDict(
        "ValueDict", {"$type": str, "_value": T_JSON_BOUND}
    )


__all__ = [
    "T_JSON",
    "T_JSON_BOUND",
    "GraphHydratorHttp",
    "HydrationHandlerHttpBase",
    "HydrationScopeHttp",
    "LiteralJson",
    "LiteralJsonRecursive",
    "make_value_dict",
    "value_as_bool",
    "value_as_dict",
    "value_as_int",
    "value_as_list",
    "value_as_list_list",
    "value_as_list_str",
    "value_as_str",
    "value_dict_key",
]

if t.TYPE_CHECKING:
    __all__.extend(
        (
            "T_STRUCT_MAP_DICT",
            "ValueDict",
        )
    )


def make_value_dict(
    type_: str, value: T_JSON_BOUND
) -> ValueDict[T_JSON_BOUND]:
    return {"$type": type_, "_value": value}


def value_as_str(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, BrokenHydrationObject) and isinstance(
        value.raw_data, str
    ):
        return value.raw_data
    raise protocol_error("string", value)


def value_as_list(value: object) -> list:
    if isinstance(value, list):
        return value

    if isinstance(value, BrokenHydrationObject) and isinstance(
        value.raw_data, list
    ):
        return value.raw_data
    raise protocol_error("list", value)


def value_as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raise protocol_error("bool", value)


def value_as_int(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    raise protocol_error("int", value)


def value_as_list_dict(value: object) -> list[dict]:
    value = value_as_list(value)
    return list(map(value_as_dict, value))


def value_as_list_list(value: object) -> list[list]:
    value = value_as_list(value)
    return list(map(value_as_list, value))


def value_as_list_str(value: object) -> list[str]:
    value = value_as_list(value)
    return list(map(value_as_str, value))


def value_as_dict(value: object) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, BrokenHydrationObject) and isinstance(
        value.raw_data, dict
    ):
        return value.raw_data
    raise protocol_error("dict", value)


def optional_value(
    converter: t.Callable[[object], T],
    value: object,
) -> T | None:
    if value is None:
        return None
    return converter(value)


def value_dict_key(value: dict, key: str) -> t.Any:
    try:
        return value[key]
    except KeyError:
        raise protocol_error(f"dict with key {key!r}", value) from None


def protocol_error(expected_type: str, value: object) -> Exception:
    return QueryApiHttpError(f"expected {expected_type}, got: {value!r}")


@dataclass
class LiteralJsonRecursive(t.Generic[T_JSON_BOUND]):
    value: T_JSON_BOUND


@dataclass
class LiteralJson(t.Generic[T_JSON_BOUND]):
    value: T_JSON_BOUND


class HydrationHandlerHttpBase(abc.ABC):
    struct_hydration_functions: T_STRUCT_MAP_DICT

    def __init__(self) -> None:
        self.struct_hydration_functions = {}
        self.dehydration_hooks = DehydrationHooks(
            exact_types={},
            exact_values={},
            subtypes={},
        )

    @abc.abstractmethod
    def new_hydration_scope(self) -> HydrationScopeHttp: ...


class GraphHydratorHttp(GraphHydrator):
    struct_hydration_functions: T_STRUCT_MAP_DICT

    def __init__(self) -> None:
        super().__init__()
        self.struct_hydration_functions = {}


class HydrationScopeHttp(HydrationScope):
    def __init__(
        self,
        hydration_handler: HydrationHandlerHttpBase,
        graph_hydrator: GraphHydratorHttp,
    ) -> None:
        hydration_hooks: T_TYPE_MAP_DICT = {
            dict: self._hydrate_dict,
            list: self._hydrate_list,
        }
        dehydration_hooks = hydration_handler.dehydration_hooks
        super().__init__(hydration_hooks, dehydration_hooks, graph_hydrator)
        self._struct_hydration_functions = {
            **hydration_handler.struct_hydration_functions,
            **graph_hydrator.struct_hydration_functions,
        }

    def _hydrate_dict(self, value: dict) -> t.Any:
        for k, v in value.items():
            value[k] = self._hydrate(v)
        if len(value) != 2:
            return self._hydrate_non_encoded_dict(value)
        if (key := value.get("$type", _default)) is _default:
            return self._hydrate_non_encoded_dict(value)
        if (inner_value := value.get("_value", _default)) is _default:
            return self._hydrate_non_encoded_dict(value)
        if isinstance(inner_value, BrokenHydrationObject):
            return BrokenHydrationObject(inner_value.error, value)

        hydrator = self._struct_hydration_functions.get(key)
        if not hydrator:
            raise QueryApiHttpError(
                f"unknown application/vnd.neo4j.query $type: {key!r}"
            )
        try:
            return hydrator(inner_value)
        except Exception as e:
            return BrokenHydrationObject(e, value)

    @staticmethod
    def _hydrate_non_encoded_dict(value: dict) -> dict | BrokenHydrationObject:
        for v in value.values():
            if isinstance(v, BrokenHydrationObject):
                return BrokenHydrationObject(v.error, value)
        return value

    def _hydrate_list(self, value: list) -> list | BrokenHydrationObject:
        for i in range(len(value)):
            value[i] = self._hydrate(value[i])
        for item in value:
            if isinstance(item, BrokenHydrationObject):
                return BrokenHydrationObject(item.error, value)
        return value

    def _hydrate(self, value: t.Any) -> t.Any:
        transformer = self.hydration_hooks.get(type(value), None)
        if transformer is not None:
            try:
                return transformer(value)
            except Exception as e:
                return BrokenHydrationObject(e, value)
        return value
