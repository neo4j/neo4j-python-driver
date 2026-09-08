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

import base64

from ..... import _typing as t
from ....._exceptions import QueryApiHttpError
from .._common import make_value_dict


if t.TYPE_CHECKING:
    from .._common import (
        HydrationHandlerHttpBase,
        ValueDict,
    )

INT64_MIN: t.Final[int] = -(2**63)
INT64_MAX: t.Final[int] = (2**63) - 1


def hydrate_null(value: object) -> None:
    if value is not None:
        raise QueryApiHttpError(f"expected null value, got: {value!r}")


def dehydrate_null(_) -> ValueDict[None]:
    return make_value_dict("Null", None)


def hydrate_boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise QueryApiHttpError(f"expected bool value, got: {value!r}")
    return value


def dehydrate_true(_) -> ValueDict[bool]:
    return make_value_dict("Boolean", True)


def dehydrate_false(_) -> ValueDict[bool]:
    return make_value_dict("Boolean", False)


def hydrate_integer(value: object) -> int:
    if not isinstance(value, str):
        raise QueryApiHttpError(f"expected string value, got: {value!r}")
    try:
        return int(value)
    except Exception as e:
        raise QueryApiHttpError(
            f"expected string convertible to int, got: {value!r}"
        ) from e


def dehydrate_integer(value: t.Any) -> ValueDict[str]:
    if not (INT64_MIN <= int(value) <= INT64_MAX):
        raise OverflowError(f"Integer {value} out of range")
    return make_value_dict("Integer", str(value))


def hydrate_float(value: object) -> float:
    if not isinstance(value, str):
        raise QueryApiHttpError(f"expected string value, got: {value!r}")
    try:
        return float(value)
    except Exception as e:
        raise QueryApiHttpError(
            f"expected string convertible to float, got: {value!r}"
        ) from e


def dehydrate_float(value: t.Any) -> ValueDict[str]:
    value_str = str(value).replace("nan", "NaN").replace("inf", "Infinity")
    return make_value_dict("Float", value_str)


def hydrate_string(value: object) -> str:
    if not isinstance(value, str):
        raise QueryApiHttpError(f"expected string value, got: {value!r}")
    return value


def dehydrate_string(value: str) -> ValueDict[str]:
    assert isinstance(value, str)
    return make_value_dict("String", value)


def hydrate_byte_array(value: object) -> bytes:
    if not isinstance(value, str):
        raise QueryApiHttpError(f"expected string value, got: {value!r}")
    try:
        return base64.b64decode(value)
    except Exception as e:
        raise QueryApiHttpError(
            f"expected base64 encoded string, got: {value!r}"
        ) from e


def dehydrate_byte_array(value: t.Any) -> ValueDict[str]:
    encoded = base64.b64encode(value).decode("ascii")
    return make_value_dict("Base64", encoded)


def hydrate_list(value: object) -> list:
    if not isinstance(value, list):
        raise QueryApiHttpError(f"expected list value, got: {value!r}")
    return value


def dehydrate_raw_list(
    value: t.Any, hydration_handler: HydrationHandlerHttpBase
) -> list:
    def transform(item: t.Any) -> t.Any:
        transformer = hydration_handler.dehydration_hooks.get_transformer(item)
        if transformer is not None:
            return transformer(item)
        return item

    return list(map(transform, value))


def dehydrate_list(
    value: list, hydration_handler: HydrationHandlerHttpBase
) -> ValueDict[list]:
    dehydrated_list = dehydrate_raw_list(value, hydration_handler)
    return make_value_dict("List", dehydrated_list)


def dehydrate_raw_pd_list(
    value: t.Any, hydration_handler: HydrationHandlerHttpBase
) -> list:
    def transform(item: t.Any) -> t.Any:
        transformer = hydration_handler.dehydration_hooks.get_transformer(item)
        if transformer is not None:
            return transformer(item)
        return item

    return [transform(value[i]) for i in range(len(value))]


def dehydrate_pd_list(
    value: list, hydration_handler: HydrationHandlerHttpBase
) -> ValueDict[list]:
    dehydrated_list = dehydrate_raw_pd_list(value, hydration_handler)
    return make_value_dict("List", dehydrated_list)


def hydrate_map(value: object) -> dict:
    if not isinstance(value, dict):
        raise QueryApiHttpError(f"expected dict value, got: {value!r}")
    return value


def dehydrate_raw_map(
    value: t.Any, hydration_handler: HydrationHandlerHttpBase
) -> dict:
    def transform(item: t.Any) -> t.Any:
        transformer = hydration_handler.dehydration_hooks.get_transformer(item)
        if transformer is not None:
            return transformer(item)
        return item

    def checked_key(key: t.Any) -> str:
        if not isinstance(key, str):
            raise TypeError(f"Map keys must be strings, not {type(key)}")
        return key

    return {checked_key(k): transform(v) for k, v in value.items()}


def dehydrate_map(
    value: t.Any, hydration_handler: HydrationHandlerHttpBase
) -> ValueDict[dict]:
    dehydrated_map = dehydrate_raw_map(value, hydration_handler)
    return make_value_dict("Map", dehydrated_map)
