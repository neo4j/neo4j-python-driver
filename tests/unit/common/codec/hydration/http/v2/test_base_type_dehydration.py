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
import re
import typing as t
from uuid import uuid4

import pytest

from neo4j._codec.hydration.http import (
    LiteralJson,
    LiteralJsonRecursive,
)
from neo4j._codec.hydration.http.v2 import HydrationHandler
from neo4j.graph import (
    Graph,
    Node,
    Path,
    Relationship,
)
from neo4j.time import (
    MAX_INT64,
    MIN_INT64,
)

from ......._optional_deps import (
    HAS_PA,
    mark_skip_without_optional_dependency,
    np,
    pd,
    skip_if_mocked_dependency,
)
from .._base import HydrationHandlerTestBase


if t.TYPE_CHECKING:
    from .._base import T_Transformer


GRAPH = Graph()


class TestDehydrateBaseTypes(HydrationHandlerTestBase):
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandler:
        return HydrationHandler()

    @pytest.mark.parametrize("value", (None, pd.NA))
    def test_null(self, value: t.Any, transformer: T_Transformer) -> None:
        skip_if_mocked_dependency(value)

        encoded = transformer(value)
        assert isinstance(encoded, dict)
        assert len(encoded) == 2
        assert encoded["$type"] == "Null"
        assert encoded["_value"] is None

    @pytest.mark.parametrize("value", (True, False))
    def test_bool(
        self, value: bool, bool_type, transformer: T_Transformer
    ) -> None:
        encoded = transformer(bool_type(value))
        assert isinstance(encoded, dict)
        assert len(encoded) == 2
        assert encoded["$type"] == "Boolean"
        assert encoded["_value"] is value

    @pytest.mark.parametrize("dtype", (bool, pd.BooleanDtype()))
    @mark_skip_without_optional_dependency(pd)
    def test_bool_pandas_series(
        self, dtype, transformer: T_Transformer
    ) -> None:
        value = [True, False]
        value_series = pd.Series(value, dtype=dtype)

        encoded = transformer(value_series)

        assert encoded == {
            "$type": "List",
            "_value": [
                {"$type": "Boolean", "_value": True},
                {"$type": "Boolean", "_value": False},
            ],
        }

    def test_int(self, int_type, transformer: T_Transformer) -> None:
        for value in (MIN_INT64, -1337, -1, 0, 1, 1337, MAX_INT64):
            value_typed = int_type(value)
            if value != int(value_typed):
                return  # not representable
            encoded = transformer(value)
            assert isinstance(encoded, dict)
            assert len(encoded) == 2
            assert encoded["$type"] == "Integer"
            assert isinstance(encoded["_value"], str)
            assert encoded["_value"] == str(value)

    @pytest.mark.parametrize(
        "dtype",
        (
            int,
            pd.Int8Dtype(),
            pd.Int16Dtype(),
            pd.Int32Dtype(),
            pd.Int64Dtype(),
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            np.longlong,
        ),
    )
    @mark_skip_without_optional_dependency(pd)
    def test_int_pandas_series(
        self, dtype, transformer: T_Transformer
    ) -> None:
        skip_if_mocked_dependency(dtype)

        value = []
        expected_value = []
        for z in (
            MIN_INT64,
            2**30,
            2**14,
            -1337,
            -1,
            0,
            1,
            1337,
            -(2**14),
            -(2**30),
            MAX_INT64,
        ):
            z_typed_i64 = pd.Series(z, dtype=pd.Int64Dtype())
            z_typed = z_typed_i64.astype(dtype)
            if (z_typed_i64 != z_typed).any():
                continue  # not representable
            value.append(z)
            expected_value.append({"$type": "Integer", "_value": str(z)})

        value_series = pd.Series(value, dtype=dtype)
        encoded = transformer(value_series)

        assert encoded == {
            "$type": "List",
            "_value": expected_value,
        }

    @pytest.mark.parametrize("value", (MIN_INT64 - 1, MAX_INT64 + 1))
    def test_int_overflow(
        self, value: bool, int_type, transformer: T_Transformer
    ) -> None:
        try:
            value_typed = int_type(value)
        except OverflowError:
            pytest.skip("not representable")
        if value != int(value_typed):
            pytest.skip("not representable")

        with pytest.raises(OverflowError):
            transformer(value)

    def test_float(
        self, float_type: t.Any, transformer: T_Transformer
    ) -> None:
        for value in (
            0.0,
            -0.0,
            -1.0,
            float("inf"),
            float("-inf"),
            float("nan"),
            float("-nan"),
            0.001,
            0.0009,
            0.00009,
            -0.001,
            -0.0009,
            -0.00009,
            1000000000000000.0,
            10000000000000000.0,
        ):
            try:
                z_typed = float_type(value)
            except FloatingPointError:
                continue  # not representable

            encoded = transformer(z_typed)

            assert isinstance(encoded, dict)
            assert len(encoded) == 2
            assert encoded["$type"] == "Float"
            assert isinstance(encoded["_value"], str)
            if math.isnan(float(z_typed)):
                assert encoded["_value"] == "NaN"
            elif math.isinf(float(z_typed)):
                if z_typed > 0:
                    assert encoded["_value"] == "Infinity"
                else:
                    assert encoded["_value"] == "-Infinity"
            else:
                assert float_type(encoded["_value"]) == z_typed
                assert float(float_type(encoded["_value"])) == float(z_typed)
                assert re.match(
                    r"^-?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$",
                    encoded["_value"],
                )

    @pytest.mark.parametrize(
        "dtype",
        (
            float,
            pd.Float32Dtype(),
            pd.Float64Dtype(),
            np.float16,
            np.float32,
            np.float64,
            np.longdouble,
        ),
    )
    @mark_skip_without_optional_dependency(pd)
    def test_float_pandas_series(
        self,
        dtype: t.Any,
        np_float_overflow_as_error: None,
        transformer: T_Transformer,
    ):
        skip_if_mocked_dependency(dtype)
        for value in (
            0.0,
            -0.0,
            math.pi,
            2 * math.pi,
            float("inf"),
            float("-inf"),
            float("nan"),
            *(float(2**e) + 0.5 for e in range(100)),
            *(-float(2**e) + 0.5 for e in range(100)),
        ):
            expected_encoded_list = []
            try:
                value_typed = pd.Series(value, dtype=dtype)
            except FloatingPointError:
                continue  # not representable
            if value_typed[0] is pd.NA:  # encoded as NULL
                expected_encoded_list.append(
                    {
                        "$type": "Null",
                        "_value": None,
                    }
                )
            else:
                expected_str = (
                    str(value_typed[0])
                    .replace("nan", "NaN")
                    .replace("inf", "Infinity")
                )
                expected_encoded_list.append(
                    {"$type": "Float", "_value": expected_str}
                )

            encoded = transformer(value_typed)

            assert encoded == {
                "$type": "List",
                "_value": expected_encoded_list,
            }

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
        self, str_type: t.Any, value: str, transformer: T_Transformer
    ) -> None:
        encoded = transformer(str_type(value))
        assert isinstance(encoded, dict)
        assert len(encoded) == 2
        assert encoded["$type"] == "String"
        assert isinstance(encoded["_value"], str)
        assert encoded["_value"] == value

    @pytest.mark.parametrize(
        "value",
        (
            "",
            "Hello, World!",
            "こんにちは、世界！",  # noqa: RUF001
            "👋🌍",
            "'",
            '"',
            "{}",
            pytest.param("A" * 100_000, id="AAA...AAA"),
        ),
    )
    @pytest.mark.parametrize(
        "dtype",
        (
            str,
            np.str_,
            pd.StringDtype("python"),
            *((pd.StringDtype("pyarrow"),) if HAS_PA else ()),
        ),
    )
    @mark_skip_without_optional_dependency(pd)
    def test_str_pandas_series(
        self, dtype: t.Any, value: str, transformer: T_Transformer
    ) -> None:
        skip_if_mocked_dependency(dtype)
        value_typed = pd.Series([value], dtype=dtype)
        encoded = transformer(value_typed)
        assert encoded == {
            "$type": "List",
            "_value": [
                {
                    "$type": "String",
                    "_value": value,
                }
            ],
        }

    def test_bytes(
        self, bytes_type: t.Any, transformer: T_Transformer
    ) -> None:
        for value in (
            b"",
            b"\x00\x01\x02\x03\x04",
            b"Hello, World!",
            bytes(i % 255 for i in range(1_000_000)),
        ):
            value_base64 = base64.b64encode(value).decode("ascii")
            value_typed = bytes_type(value)
            encoded = transformer(value_typed)
            assert isinstance(encoded, dict)
            assert len(encoded) == 2
            assert encoded["$type"] == "Base64"
            assert isinstance(encoded["_value"], str)
            assert encoded["_value"] == value_base64

    @mark_skip_without_optional_dependency(pd)
    def test_bytes_pandas_series(self, transformer: T_Transformer) -> None:
        for value in (
            b"",
            b"\x00\x01\x02\x03\x04",
            b"Hello, World!",
            bytes(i % 255 for i in range(1_000_000)),
        ):
            value_base64 = base64.b64encode(value).decode("ascii")
            value_typed = pd.Series([value])
            encoded = transformer(value_typed)

            assert encoded == {
                "$type": "List",
                "_value": [
                    {
                        "$type": "Base64",
                        "_value": value_base64,
                    }
                ],
            }

    @pytest.mark.parametrize("inner_as_list", (True, False))
    def test_nested_lists(
        self,
        inner_as_list: bool,
        sequence_type: t.Any,
        transformer: T_Transformer,
    ) -> None:
        list_: list = [[[]]]
        if inner_as_list:
            l_typed = sequence_type(list_)
        else:
            l_typed = sequence_type([sequence_type([sequence_type([])])])
        encoded = transformer(l_typed)

        assert encoded == {
            "$type": "List",
            "_value": [
                {
                    "$type": "List",
                    "_value": [
                        {
                            "$type": "List",
                            "_value": [],
                        }
                    ],
                }
            ],
        }

    @pytest.mark.parametrize(
        ("value", "encoded_value"),
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
                [[1.2, float("nan"), 0.0], [3.2, float("inf"), float("-inf")]],
                [
                    {
                        "$type": "List",
                        "_value": [
                            {"$type": "Float", "_value": "1.2"},
                            {"$type": "Float", "_value": "NaN"},
                            {"$type": "Float", "_value": "0.0"},
                        ],
                    },
                    {
                        "$type": "List",
                        "_value": [
                            {"$type": "Float", "_value": "3.2"},
                            {"$type": "Float", "_value": "Infinity"},
                            {"$type": "Float", "_value": "-Infinity"},
                        ],
                    },
                ],
            ),
        ),
    )
    @pytest.mark.parametrize("sequence_type", (list, tuple))
    def test_matrix_like_list(
        self,
        value: list,
        encoded_value: list,
        sequence_type: t.Any,
        transformer: T_Transformer,
    ) -> None:
        value = sequence_type(value)
        encoded = transformer(value)
        assert isinstance(encoded, dict)
        assert len(encoded) == 2
        assert encoded["$type"] == "List"
        assert isinstance(encoded["_value"], list)
        assert encoded["_value"] == encoded_value

    @pytest.mark.parametrize(
        ("value", "encoded_value"),
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
    def test_mixed_list(
        self,
        value: list,
        encoded_value: list,
        sequence_type: t.Any,
        transformer: T_Transformer,
    ) -> None:
        value = sequence_type(value)
        encoded = transformer(value)
        assert isinstance(encoded, dict)
        assert len(encoded) == 2
        assert encoded["$type"] == "List"
        assert isinstance(encoded["_value"], list)
        assert encoded["_value"] == encoded_value

    @pytest.mark.parametrize("as_series", (True, False))
    @mark_skip_without_optional_dependency(pd)
    def test_list_pandas_categorical(
        self, as_series: bool, transformer: T_Transformer
    ) -> None:
        animals = ["cat", "dog", "cat", "cat", "dog", "horse"]
        # TODO: 7.0 - make type hint more precise
        animals_typed: t.Any = pd.Categorical(animals)
        if as_series:
            animals_typed = pd.Series(animals_typed)
        encoded = transformer(animals_typed)
        assert encoded == {
            "$type": "List",
            "_value": [
                {"$type": "String", "_value": animal} for animal in animals
            ],
        }

    @pytest.mark.parametrize(
        "key",
        (
            "",
            "foo",
            "こんにちは",
            pytest.param("A" * 80000, id="80k_A"),
        ),
    )
    def test_map_key(self, key: str, transformer: T_Transformer) -> None:
        value = {key: 42}
        encoded = transformer(value)

        assert encoded == {
            "$type": "Map",
            "_value": {
                key: {
                    "$type": "Integer",
                    "_value": "42",
                }
            },
        }

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
        self, values: list, encoded_values: list, transformer: T_Transformer
    ) -> None:
        value = {str(i): value for (i, value) in enumerate(values)}
        encoded = transformer(value)

        assert encoded == {
            "$type": "Map",
            "_value": {
                str(i): encoded_value
                for (i, encoded_value) in enumerate(encoded_values)
            },
        }

    @mark_skip_without_optional_dependency(pd)
    def test_map_pandas_dataframe(self, transformer: T_Transformer) -> None:
        data = {
            "a": [1, 2],
            "b": ["foo", "bar"],
            "c": [True, False],
        }
        df = pd.DataFrame(data)
        encoded = transformer(df)

        assert encoded == {
            "$type": "Map",
            "_value": {
                "a": {
                    "$type": "List",
                    "_value": [
                        {"$type": "Integer", "_value": "1"},
                        {"$type": "Integer", "_value": "2"},
                    ],
                },
                "b": {
                    "$type": "List",
                    "_value": [
                        {"$type": "String", "_value": "foo"},
                        {"$type": "String", "_value": "bar"},
                    ],
                },
                "c": {
                    "$type": "List",
                    "_value": [
                        {"$type": "Boolean", "_value": True},
                        {"$type": "Boolean", "_value": False},
                    ],
                },
            },
        }

    @pytest.mark.parametrize(
        ("map_", "exc_type"),
        (
            ({1: "1"}, TypeError),
            (pd.DataFrame({1: ["1"]}), TypeError),
            (pd.DataFrame({(1, 2): ["1"]}), TypeError),
            ({"x": {1: "eins", 2: "zwei", 3: "drei"}}, TypeError),
            ({"x": {(1, 2): "1+2i", (2, 0): "2"}}, TypeError),
        ),
    )
    def test_map_key_type(
        self,
        map_: t.Any,
        exc_type: type[Exception],
        transformer: T_Transformer,
    ) -> None:
        skip_if_mocked_dependency(map_)
        # maps must have string keys
        with pytest.raises(exc_type, match="strings"):
            transformer(map_)

    @pytest.mark.parametrize(
        "value",
        (
            uuid4(),
            object(),
            {1, 2, 3},
            Node(GRAPH, "e1", 1, [], {}),
            Relationship(GRAPH, "e1", 1, {}),
            Path(Node(GRAPH, "n1", 1, [], {})),
        ),
    )
    def test_unsupported_type(
        self, value: object, transformer: T_Transformer
    ) -> None:
        with pytest.raises(ValueError, match=str(type(value).__name__)):
            transformer(value)

    def test_literal_json(self, transformer: T_Transformer) -> None:
        value = LiteralJson([1, {"a": [2, LiteralJson(3)]}])
        encoded = transformer(value)
        assert encoded == [
            {"$type": "Integer", "_value": "1"},
            {
                "$type": "Map",
                "_value": {
                    "a": {
                        "$type": "List",
                        "_value": [
                            {"$type": "Integer", "_value": "2"},
                            3,
                        ],
                    },
                },
            },
        ]

    def test_literal_json_recursive(self, transformer: T_Transformer) -> None:
        value = LiteralJsonRecursive([1, {"a": 2}])
        encoded = transformer(value)
        assert encoded == [1, {"a": 2}]
