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
import math
import typing as t

import pytest

from neo4j._codec.hydration.http.v2 import HydrationHandler
from neo4j.time import (
    MAX_INT64,
    MIN_INT64,
)

from .._base import HydrationHandlerTestBase


if t.TYPE_CHECKING:
    from neo4j._codec.hydration.http._common import HydrationScopeHttp


class TestHydrateBaseTypes(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandler:
        return HydrationHandler()

    def test_null(self, hydration_scope: HydrationScopeHttp) -> None:
        encoded = {
            "$type": "Null",
            "_value": None,
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
        assert decoded is None

    @pytest.mark.parametrize("value", (True, False))
    def test_bool(
        self, value: bool, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "Boolean",
            "_value": value,
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
        assert decoded is value

    @pytest.mark.parametrize(
        "value",
        (MIN_INT64, -1337, -1, 0, 1, 1337, MAX_INT64),
    )
    def test_int(
        self, value: int, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "Integer",
            "_value": str(value),
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(decoded, int)
        assert decoded == value

    @pytest.mark.parametrize(
        ("value_str", "value"),
        (
            ("0.0", 0.0),
            (".0", 0.0),
            ("-0.0", -0.0),
            ("-1.0", -1.0),
            ("Infinity", float("inf")),
            ("-Infinity", float("-inf")),
            ("NaN", float("nan")),
            ("0.001", 0.001),
            ("9.0E-4", 0.0009),
            ("9e-05", 0.00009),
            ("-0.001", -0.001),
            ("-0.0009", -0.0009),
            ("-9e-05", -0.00009),
            ("1000000.0", 1000000.0),
            ("1.0E7", 10000000.0),
            ("1000000000000000.0", 1000000000000000.0),
            ("1E+16", 10000000000000000.0),
        ),
    )
    def test_float(
        self, value_str: str, value: float, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "Float",
            "_value": value_str,
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(decoded, float)
        if math.isnan(value):
            assert math.isnan(decoded)
        else:
            assert decoded == value

    @pytest.mark.parametrize(
        "value",
        (
            "",
            "Hello, World!",
            "こんにちは、世界！",  # noqa: RUF001
            "👋🌍",
            pytest.param("A" * 100_000, id="AAA...AAA"),
        ),
    )
    def test_str(
        self, value: str, hydration_scope: HydrationScopeHttp
    ) -> None:
        encoded = {
            "$type": "String",
            "_value": value,
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
        self.assert_is_hydrated_type(decoded, str)
        assert decoded == value

    def test_bytes(self, hydration_scope: HydrationScopeHttp) -> None:
        for value in (
            b"",
            b"\x00\x01\x02\x03\x04",
            b"Hello, World!",
            bytes(i % 255 for i in range(1_000_000)),
        ):
            encoded = {
                "$type": "Base64",
                "_value": base64.b64encode(value).decode("ascii"),
            }
            decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
            self.assert_is_hydrated_type(decoded, bytes)
            assert decoded == value

    @pytest.mark.parametrize(
        ("value", "encoded"),
        (
            (
                [],
                [],
            ),
            (
                [1, 2, 3],
                [
                    {"$type": "Integer", "_value": "1"},
                    {"$type": "Integer", "_value": "2"},
                    {"$type": "Integer", "_value": "3"},
                ],
            ),
            (
                [{"foo": None}, [1, 2, True], "bar"],
                [
                    {
                        "$type": "Map",
                        "_value": {"foo": {"$type": "Null", "_value": None}},
                    },
                    {
                        "$type": "List",
                        "_value": [
                            {"$type": "Integer", "_value": "1"},
                            {"$type": "Integer", "_value": "2"},
                            {"$type": "Boolean", "_value": True},
                        ],
                    },
                    {"$type": "String", "_value": "bar"},
                ],
            ),
        ),
    )
    def test_list(
        self, value: list, encoded: list, hydration_scope: HydrationScopeHttp
    ) -> None:
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)
        assert decoded == value

    @pytest.mark.parametrize(
        "key",
        (
            "",
            "foo",
            "こんにちは",
            pytest.param("A" * 80000, id="80k_A"),
        ),
    )
    def test_map_key(
        self, key: str, hydration_scope: HydrationScopeHttp
    ) -> None:
        value = {key: 42}
        encoded = {
            "$type": "Map",
            "_value": {
                key: {
                    "$type": "Integer",
                    "_value": "42",
                }
            },
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)

        assert decoded == value

    @pytest.mark.parametrize(
        ("values", "encoded_values"),
        (
            ([], []),
            (
                [
                    [],
                ],
                [
                    {"$type": "List", "_value": []},
                ],
            ),
            (
                [
                    1,
                    2.0,
                    3,
                ],
                [
                    {"$type": "Integer", "_value": "1"},
                    {"$type": "Float", "_value": "2.0"},
                    {"$type": "Integer", "_value": "3"},
                ],
            ),
            (
                [
                    None,
                    "foo",
                    [1, 2],
                    {"bar": True},
                ],
                [
                    {"$type": "Null", "_value": None},
                    {"$type": "String", "_value": "foo"},
                    {
                        "$type": "List",
                        "_value": [
                            {"$type": "Integer", "_value": "1"},
                            {"$type": "Integer", "_value": "2"},
                        ],
                    },
                    {
                        "$type": "Map",
                        "_value": {
                            "bar": {"$type": "Boolean", "_value": True},
                        },
                    },
                ],
            ),
        ),
    )
    def test_map(
        self,
        values: list,
        encoded_values: list,
        hydration_scope: HydrationScopeHttp,
    ) -> None:
        value = {str(i): value for (i, value) in enumerate(values)}
        encoded = {
            "$type": "Map",
            "_value": {
                str(i): encoded_value
                for (i, encoded_value) in enumerate(encoded_values)
            },
        }
        decoded = hydration_scope.hydration_hooks[type(encoded)](encoded)

        assert decoded == value
