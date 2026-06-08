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
import math
import random
import struct
import sys
import typing as t

import pytest

from neo4j._optional_deps import (
    np,
    pa,
    pa_compute,
)
from neo4j.vector import (
    _swap_endian,
    Vector,
    VectorDType,
    VectorEndian,
)


if t.TYPE_CHECKING:
    import numpy
    import pyarrow
    from pytest_mock import MockFixture

    T_ENDIAN_LITERAL: t.TypeAlias = t.Literal["big", "little"] | VectorEndian
    T_DTYPE_LITERAL: t.TypeAlias = (
        t.Literal["i8", "i16", "i32", "i64", "f32", "f64"] | VectorDType
    )
    T_DTYPE_INT_LITERAL: t.TypeAlias = t.Literal[
        "i8",
        "i16",
        "i32",
        "i64",
        VectorDType.I8,
        VectorDType.I16,
        VectorDType.I32,
        VectorDType.I64,
    ]
    T_DTYPE_FLOAT_LITERAL: t.TypeAlias = t.Literal[
        "f32", "f64", VectorDType.F32, VectorDType.F64
    ]
    T_EXT_LITERAL: t.TypeAlias = t.Literal["numpy", "rust", "python"]


ENDIAN_LITERALS: tuple[T_ENDIAN_LITERAL, ...] = (
    "big",
    "little",
    *VectorEndian,
)
DTYPE_LITERALS: tuple[T_DTYPE_LITERAL, ...] = (
    "i8",
    "i16",
    "i32",
    "i64",
    "f32",
    "f64",
    *VectorDType,
)
DTYPE_INT_LITERALS: tuple[T_DTYPE_INT_LITERAL, ...] = (
    "i8",
    "i16",
    "i32",
    "i64",
    VectorDType.I8,
    VectorDType.I16,
    VectorDType.I32,
    VectorDType.I64,
)
DTYPE_FLOAT_LITERALS: tuple[T_DTYPE_FLOAT_LITERAL, ...] = (
    "f32",
    "f64",
    VectorDType.F32,
    VectorDType.F64,
)


def _max_value_be_bytes(size: t.Literal[1, 2, 4, 8], count: int = 1) -> bytes:
    def generator(count_: int) -> t.Iterable[int]:
        pack_format = {
            1: ">b",
            2: ">h",
            4: ">i",
            8: ">q",
        }[size]
        if count_ <= 0:
            return
        yield from struct.pack(pack_format, 0)
        count_ -= 1
        i = 0
        min_value = -(2 ** (size * 8 - 1))
        max_value = 2 ** (size * 8 - 1) - 1
        while True:
            if count_ <= 0:
                return
            yield from struct.pack(pack_format, min_value + i)
            count_ -= 1
            if count_ == 0:
                return
            yield from struct.pack(pack_format, max_value - i)
            count_ -= 1
            i += 1
            i %= 2 ** (size * 8)

    return bytes(generator(count))


def _random_value_be_bytes(
    size: t.Literal[1, 2, 4, 8], count: int = 1
) -> bytes:
    def generator(count_: int) -> t.Iterable[int]:
        pack_format = {
            1: ">B",
            2: ">H",
            4: ">I",
            8: ">Q",
        }[size]
        while count_ > 0:
            yield from struct.pack(
                pack_format, random.randint(0, 2 ** (size * 8) - 1)
            )
            count_ -= 1

    return bytes(generator(count))


def _get_type_size(dtype: str) -> t.Literal[1, 2, 4, 8]:
    lookup: dict[str, t.Literal[1, 2, 4, 8]] = {
        "i8": 1,
        "i16": 2,
        "i32": 4,
        "i64": 8,
        "f32": 4,
        "f64": 8,
    }
    return lookup[dtype]


class NormalizableBytes(abc.ABC):
    @abc.abstractmethod
    def normalized_bytes(self) -> bytes: ...

    @abc.abstractmethod
    def raw_bytes(self) -> bytes: ...


class Bytes(NormalizableBytes):
    _data: bytes

    def __init__(self, data: bytes) -> None:
        self._data = data

    def normalized_bytes(self) -> bytes:
        return self._data

    def raw_bytes(self) -> bytes:
        return self._data


class Float32NanPayloadBytes(NormalizableBytes):
    _data: bytes

    def __init__(self, data: bytes) -> None:
        self._data = data

    def normalized_bytes(self) -> bytes:
        type_size = _get_type_size("f32")
        pack_format = _dtype_to_pack_format("f32")

        # Python <3.14 does not preserve NaN payloads on struct pack/unpack
        # for float32:
        # https://github.com/python/cpython/issues/130317
        if sys.version_info >= (3, 14):
            return self._data
        chunks = (
            self._data[i : i + type_size]
            for i in range(0, len(self._data), type_size)
        )
        return bytes(
            b
            for chunk in chunks
            for b in struct.pack(
                pack_format, struct.unpack(pack_format, chunk)[0]
            )
        )

    def raw_bytes(self) -> bytes:
        return self._data


def _dtype_to_pack_format(dtype: str) -> str:
    return {
        "i8": ">b",
        "i16": ">h",
        "i32": ">i",
        "i64": ">q",
        "f32": ">f",
        "f64": ">d",
    }[dtype]


def _mock_mask_extensions(
    used_ext: T_EXT_LITERAL, mocker: MockFixture
) -> None:
    from neo4j.vector import (
        _swap_endian_unchecked_np,
        _swap_endian_unchecked_py,
        _swap_endian_unchecked_rust,
    )

    match used_ext:
        case "numpy":
            if np is None:
                pytest.skip("numpy not installed")
            mocker.patch(
                "neo4j.vector._swap_endian_unchecked",
                new=_swap_endian_unchecked_np,
            )
        case "rust":
            if _swap_endian_unchecked_rust is None:
                pytest.skip("rust extensions are not installed")
            mocker.patch(
                "neo4j.vector._swap_endian_unchecked",
                new=_swap_endian_unchecked_rust,
            )
        case "python":
            mocker.patch(
                "neo4j.vector._swap_endian_unchecked",
                new=_swap_endian_unchecked_py,
            )
        case _:
            raise ValueError(f"Invalid ext value {used_ext}")


@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
def test_swap_endian(mocker: MockFixture, ext: T_EXT_LITERAL) -> None:
    data = bytes(range(1, 17))
    _mock_mask_extensions(ext, mocker)
    res = _swap_endian(2, data)
    assert isinstance(res, bytes)
    assert res == bytes(
        (2, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11, 14, 13, 16, 15)
    )
    res = _swap_endian(4, data)
    assert isinstance(res, bytes)
    assert res == bytes(
        (4, 3, 2, 1, 8, 7, 6, 5, 12, 11, 10, 9, 16, 15, 14, 13)
    )
    res = _swap_endian(8, data)
    assert isinstance(res, bytes)
    assert res == bytes(
        (8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9)
    )


@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
@pytest.mark.parametrize("type_size", (-1, 0, 3, 5, 7, 9, 16, 32))
def test_swap_endian_unhandled_size(
    ext: T_EXT_LITERAL, type_size: int, mocker: MockFixture
) -> None:
    data = bytes(i % 256 for i in range(1, abs(type_size) * 4))
    _mock_mask_extensions(ext, mocker)

    with pytest.raises(ValueError, match=str(type_size)):
        _swap_endian(type_size, data)


@pytest.mark.parametrize(
    ("dtype", "data"),
    (
        pytest.param(
            "i8",
            b"",
            id="i8-empty",
        ),
        pytest.param(
            "i8",
            bytes.fromhex("01"),
            id="i8-single",
        ),
        pytest.param(
            "i8",
            bytes.fromhex("01020304"),
            id="i8-some",
        ),
        pytest.param(
            "i8",
            _max_value_be_bytes(1, 4096),
            id="i8-limit",
        ),
        pytest.param(
            "i16",
            b"",
            id="i16-empty",
        ),
        pytest.param(
            "i16",
            bytes.fromhex("0001"),
            id="i16-single",
        ),
        pytest.param(
            "i16",
            bytes.fromhex("00010002"),
            id="i16-some",
        ),
        pytest.param(
            "i16",
            _max_value_be_bytes(2, 4096),
            id="i16-limit",
        ),
        pytest.param(
            "i32",
            b"",
            id="i32-empty",
        ),
        pytest.param(
            "i32",
            bytes.fromhex("00000001"),
            id="i32-single",
        ),
        pytest.param(
            "i32",
            bytes.fromhex("0000000100000002"),
            id="i32-some",
        ),
        pytest.param(
            "i32",
            _max_value_be_bytes(4, 4096),
            id="i32-limit",
        ),
        pytest.param(
            "i64",
            b"",
            id="i64-empty",
        ),
        pytest.param(
            "i64",
            bytes.fromhex("0000000000000001"),
            id="i64-single",
        ),
        pytest.param(
            "i64",
            bytes.fromhex("0000000000000001 0000000000000002"),
            id="i64-some",
        ),
        pytest.param(
            "i64",
            _max_value_be_bytes(8, 4096),
            id="i64-limit",
        ),
        pytest.param(
            "f32",
            b"",
            id="f32-empty",
        ),
        pytest.param(
            "f32",
            _random_value_be_bytes(4, 4096),
            id="f32-limit",
        ),
        pytest.param(
            "f64",
            b"",
            id="f64-empty",
        ),
        pytest.param(
            "f64",
            _random_value_be_bytes(8, 4096),
            id="f64-limit",
        ),
    ),
)
@pytest.mark.parametrize("input_endian", (None, *ENDIAN_LITERALS))
@pytest.mark.parametrize("as_bytearray", (False, True))
def test_raw_data_limits(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    data: bytes,
    input_endian: T_ENDIAN_LITERAL | None,
    as_bytearray: bool,
) -> None:
    swapped_data = _swap_endian(_get_type_size(dtype), data)
    if input_endian is None:
        input_data = bytearray(data) if as_bytearray else data
        v = Vector(input_data, dtype)
    elif input_endian == "big":
        input_data = bytearray(data) if as_bytearray else data
        v = Vector(input_data, dtype, byteorder=input_endian)
    elif input_endian == "little":
        input_data = bytearray(swapped_data) if as_bytearray else swapped_data
        v = Vector(input_data, dtype, byteorder=input_endian)
    else:
        raise ValueError(f"Invalid input_endian {input_endian}")
    assert v.dtype == dtype
    assert v.raw() == data
    assert v.raw(byteorder="big") == data
    assert v.raw(byteorder=VectorEndian.BIG) == data
    assert v.raw(byteorder="little") == swapped_data
    assert v.raw(byteorder=VectorEndian.LITTLE) == swapped_data


def nan_equals(a: list[object], b: list[object]) -> bool:
    if len(a) != len(b):
        return False
    for i in range(len(a)):
        ai = a[i]
        bi = b[i]
        if ai != bi and not (
            isinstance(ai, float)
            and isinstance(bi, float)
            and math.isnan(ai)
            and math.isnan(bi)
        ):
            return False
    return True


@pytest.mark.parametrize("dtype", DTYPE_INT_LITERALS)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
@pytest.mark.parametrize("use_init", (False, True))
def test_from_native_int_random(
    dtype: T_DTYPE_INT_LITERAL,
    repeat: int,
    size: int,
    use_init: bool,
) -> None:
    type_size = _get_type_size(dtype)
    for _ in range(repeat):
        data = _random_value_be_bytes(type_size, size)
        values = [
            struct.unpack(
                _dtype_to_pack_format(dtype), data[i : i + type_size]
            )[0]
            for i in range(0, len(data), type_size)
        ]
        assert all(type(v) is int for v in values)
        if use_init:
            v = Vector(values, dtype)
        else:
            v = Vector.from_native(values, dtype)
        expected_raw = data
        if dtype == "f32":
            expected_raw = Float32NanPayloadBytes(data).normalized_bytes()
        assert v.raw() == expected_raw


@pytest.mark.parametrize("dtype", DTYPE_FLOAT_LITERALS)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
@pytest.mark.parametrize("use_init", (False, True))
def test_from_native_float_random(
    dtype: T_DTYPE_FLOAT_LITERAL,
    repeat: int,
    size: int,
    use_init: bool,
) -> None:
    type_size = _get_type_size(dtype)
    for _ in range(repeat):
        data = _random_value_be_bytes(type_size, size)
        values = [
            struct.unpack(
                _dtype_to_pack_format(dtype), data[i : i + type_size]
            )[0]
            for i in range(0, len(data), type_size)
        ]
        assert all(type(v) is float for v in values)
        if use_init:
            v = Vector(values, dtype)
        else:
            v = Vector.from_native(values, dtype)
        expected_raw = data
        if dtype == "f32":
            expected_raw = Float32NanPayloadBytes(data).normalized_bytes()
        assert v.raw() == expected_raw


SPECIAL_INT_VALUES: tuple[
    tuple[T_DTYPE_INT_LITERAL, int, NormalizableBytes], ...
] = (
    # (dtype, value, packed_bytes_be)
    # i8
    ("i8", -128, Bytes(bytes.fromhex("80"))),
    ("i8", 0, Bytes(bytes.fromhex("00"))),
    ("i8", 127, Bytes(bytes.fromhex("7f"))),
    # i16
    ("i16", -32768, Bytes(bytes.fromhex("8000"))),
    ("i16", 0, Bytes(bytes.fromhex("0000"))),
    ("i16", 32767, Bytes(bytes.fromhex("7fff"))),
    # i32
    ("i32", -2147483648, Bytes(bytes.fromhex("80000000"))),
    ("i32", 0, Bytes(bytes.fromhex("00000000"))),
    ("i32", 2147483647, Bytes(bytes.fromhex("7fffffff"))),
    # i64
    ("i64", -9223372036854775808, Bytes(bytes.fromhex("8000000000000000"))),
    ("i64", 0, Bytes(bytes.fromhex("0000000000000000"))),
    ("i64", 9223372036854775807, Bytes(bytes.fromhex("7fffffffffffffff"))),
)
SPECIAL_FLOAT_VALUES: tuple[
    tuple[T_DTYPE_FLOAT_LITERAL, float, NormalizableBytes], ...
] = (
    # (dtype, value, packed_bytes_be)
    # f32
    # NaN
    (
        "f32",
        float("nan"),
        Bytes(bytes.fromhex("7fc00000")),
    ),
    (
        "f32",
        float("-nan"),
        Bytes(bytes.fromhex("ffc00000")),
    ),
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("7fc00011"))[0],
        Bytes(bytes.fromhex("7fc00011")),
    ),
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("7f800001"))[0],
        Float32NanPayloadBytes(bytes.fromhex("7f800001")),
    ),
    # ±inf
    (
        "f32",
        float("inf"),
        Bytes(bytes.fromhex("7f800000")),
    ),
    (
        "f32",
        float("-inf"),
        Bytes(bytes.fromhex("ff800000")),
    ),
    # ±0.0
    (
        "f32",
        0.0,
        Bytes(bytes.fromhex("00000000")),
    ),
    (
        "f32",
        -0.0,
        Bytes(bytes.fromhex("80000000")),
    ),
    # smallest normal
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("00800000"))[0],
        Bytes(bytes.fromhex("00800000")),
    ),
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("80800000"))[0],
        Bytes(bytes.fromhex("80800000")),
    ),
    # subnormal
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("00000001"))[0],
        Bytes(bytes.fromhex("00000001")),
    ),
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("80000001"))[0],
        Bytes(bytes.fromhex("80000001")),
    ),
    # largest normal
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("7f7fffff"))[0],
        Bytes(bytes.fromhex("7f7fffff")),
    ),
    (
        "f32",
        struct.unpack(">f", bytes.fromhex("ff7fffff"))[0],
        Bytes(bytes.fromhex("ff7fffff")),
    ),
    # very small f64 being rounded to ±0 in f32
    (
        "f32",
        struct.unpack(">d", bytes.fromhex("3686d601ad376ab9"))[0],
        Bytes(bytes.fromhex("00000000")),
    ),
    (
        "f32",
        struct.unpack(">d", bytes.fromhex("b686d601ad376ab9"))[0],
        Bytes(bytes.fromhex("80000000")),
    ),
    # f64
    # NaN
    (
        "f64",
        float("nan"),
        Bytes(bytes.fromhex("7ff8000000000000")),
    ),
    (
        "f64",
        float("-nan"),
        Bytes(bytes.fromhex("fff8000000000000")),
    ),
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("7ff8000000000011"))[0],
        Bytes(bytes.fromhex("7ff8000000000011")),
    ),
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("7ff0000100000001"))[0],
        Bytes(bytes.fromhex("7ff0000100000001")),
    ),
    # ±inf
    (
        "f64",
        float("inf"),
        Bytes(bytes.fromhex("7ff0000000000000")),
    ),
    (
        "f64",
        float("-inf"),
        Bytes(bytes.fromhex("fff0000000000000")),
    ),
    # ±0.0
    (
        "f64",
        0.0,
        Bytes(bytes.fromhex("0000000000000000")),
    ),
    (
        "f64",
        -0.0,
        Bytes(bytes.fromhex("8000000000000000")),
    ),
    # smallest normal
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("0010000000000000"))[0],
        Bytes(bytes.fromhex("0010000000000000")),
    ),
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("8010000000000000"))[0],
        Bytes(bytes.fromhex("8010000000000000")),
    ),
    # subnormal
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("0000000000000001"))[0],
        Bytes(bytes.fromhex("0000000000000001")),
    ),
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("8000000000000001"))[0],
        Bytes(bytes.fromhex("8000000000000001")),
    ),
    # largest normal
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("7fefffffffffffff"))[0],
        Bytes(bytes.fromhex("7fefffffffffffff")),
    ),
    (
        "f64",
        struct.unpack(">d", bytes.fromhex("ffefffffffffffff"))[0],
        Bytes(bytes.fromhex("ffefffffffffffff")),
    ),
)
SPECIAL_VALUES = SPECIAL_INT_VALUES + SPECIAL_FLOAT_VALUES


@pytest.mark.parametrize(("dtype", "value", "data_be_raw"), SPECIAL_VALUES)
def test_from_native_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    data_be_raw: NormalizableBytes,
) -> None:
    data_be = data_be_raw.normalized_bytes()
    if dtype in {"f32", "f64"}:
        assert isinstance(value, float)
        dtype_f = t.cast(t.Literal["f32", "f64"], dtype)
        v = Vector.from_native([value], dtype_f)
    elif dtype in {"i8", "i16", "i32", "i64"}:
        assert isinstance(value, int)
        dtype_i = t.cast(t.Literal["i8", "i16", "i32", "i64"], dtype)
        v = Vector.from_native([value], dtype_i)
    else:
        raise ValueError(f"Invalid dtype {dtype}")
    assert v.raw() == data_be


@pytest.mark.parametrize(
    ("dtype", "value"),
    (
        ("i8", "1"),
        ("i8", None),
        ("i8", 1.0),
        ("i16", "1"),
        ("i16", None),
        ("i16", 1.0),
        ("i32", "1"),
        ("i32", None),
        ("i32", 1.0),
        ("i64", "1"),
        ("i64", None),
        ("i64", 1.0),
        ("f32", "1.0"),
        ("f32", None),
        ("f32", 1),
        ("f64", "1.0"),
        ("f64", None),
        ("f64", 1),
    ),
)
def test_from_native_wrong_type(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
) -> None:
    with pytest.raises(TypeError) as exc:
        Vector.from_native([value], dtype)  # type: ignore

    assert dtype in str(exc.value)
    assert str(type(value).__name__) in str(exc.value)


@pytest.mark.parametrize(
    ("dtype", "value"),
    (
        ("i8", -129),
        ("i8", 128),
        ("i16", -32769),
        ("i16", 32768),
        ("i32", -2147483649),
        ("i32", 2147483648),
        ("i64", -9223372036854775809),
        ("i64", 9223372036854775808),
        # positive value, positive exponent overflow
        ("f32", struct.unpack(">d", bytes.fromhex("47f0000020000000"))[0]),
        # negative value, positive exponent overflow
        ("f32", struct.unpack(">d", bytes.fromhex("c7f0000020000000"))[0]),
        # no such thing as negative exponent overflow:
        # very small values become 0.0
        # positive value, positive exponent, mantiassa overflow
        ("f32", struct.unpack(">d", bytes.fromhex("47effffff0000000"))[0]),
        # negative value, positive exponent, mantiassa overflow
        ("f32", struct.unpack(">d", bytes.fromhex("c7effffff0000000"))[0]),
    ),
)
def test_from_native_overflow(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
) -> None:
    with pytest.raises(OverflowError) as exc:
        Vector.from_native([value], dtype)  # type: ignore

    assert dtype in str(exc.value)


def _vector_from_data(
    data: bytes,
    dtype: T_DTYPE_LITERAL,
    endian: T_ENDIAN_LITERAL | None,
) -> Vector:
    match endian:
        case None:
            return Vector(data, dtype)
        case "big":
            return Vector(data, dtype, byteorder=endian)
        case "little":
            type_size = _get_type_size(dtype)
            data_le = _swap_endian(type_size, data)
            return Vector(data_le, dtype, byteorder=endian)
        case _:
            raise ValueError(f"Invalid endian {endian}")


@pytest.mark.parametrize("dtype", DTYPE_LITERALS)
@pytest.mark.parametrize(
    "endian",
    (
        None,
        *ENDIAN_LITERALS,
    ),
)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
def test_to_native_random(
    dtype: T_DTYPE_LITERAL,
    endian: T_ENDIAN_LITERAL | None,
    repeat: int,
    size: int,
) -> None:
    type_size = _get_type_size(dtype)
    for _ in range(repeat):
        data = _random_value_be_bytes(type_size, size)
        expected = [
            struct.unpack(
                _dtype_to_pack_format(dtype), data[i : i + type_size]
            )[0]
            for i in range(0, len(data), type_size)
        ]
        v = _vector_from_data(data, dtype, endian)
        assert nan_equals(v.to_native(), expected)


@pytest.mark.parametrize(("dtype", "value", "data_be_raw"), SPECIAL_VALUES)
def test_to_native_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    data_be_raw: NormalizableBytes,
) -> None:
    data_be = data_be_raw.raw_bytes()
    type_size = _get_type_size(dtype)
    pack_format = _dtype_to_pack_format(dtype)
    expected = [
        struct.unpack(pack_format, data_be[i : i + type_size])[0]
        for i in range(0, len(data_be), type_size)
    ]
    v = Vector(data_be, dtype)
    assert nan_equals(v.to_native(), expected)


def _get_numpy_dtype(dtype: str) -> str:
    return {
        "i8": "i1",
        "i16": "i2",
        "i32": "i4",
        "i64": "i8",
        "f32": "f4",
        "f64": "f8",
    }[dtype]


def _get_numpy_array(
    data_be: bytes, dtype: str, endian: t.Literal["big", "little", "native"]
) -> numpy.ndarray:
    np_type = _get_numpy_dtype(dtype)
    type_size = _get_type_size(dtype)
    data_in = data_be
    match endian:
        case "big":
            data_in = data_be
            np_type = f">{np_type}"
        case "little":
            data_in = _swap_endian(type_size, data_be)
            np_type = f"<{np_type}"
        case "native":
            if sys.byteorder == "little":
                data_in = _swap_endian(type_size, data_be)
            np_type = f"={np_type}"
    return np.frombuffer(data_in, dtype=np_type)


@pytest.mark.skipif(np is None, reason="numpy not installed")
@pytest.mark.parametrize("dtype", ("i8", "i16", "i32", "i64", "f32", "f64"))
@pytest.mark.parametrize("endian", ("big", "little", "native"))
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
@pytest.mark.parametrize("use_init", (False, True))
def test_from_numpy_random(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: t.Literal["big", "little", "native"],
    repeat: int,
    size: int,
    use_init: bool,
) -> None:
    type_size = _get_type_size(dtype)
    for _ in range(repeat):
        data_be = _random_value_be_bytes(type_size, size)
        array = _get_numpy_array(data_be, dtype, endian)
        v = Vector(array) if use_init else Vector.from_numpy(array)
        assert v.dtype == dtype
        assert v.raw() == data_be
        assert nan_equals(array.tolist(), v.to_native())


@pytest.mark.skipif(np is None, reason="numpy not installed")
@pytest.mark.parametrize(("dtype", "value", "data_be_raw"), SPECIAL_VALUES)
@pytest.mark.parametrize("endian", ("big", "little", "native"))
def test_from_numpy_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: t.Literal["big", "little", "native"],
    value: object,
    data_be_raw: NormalizableBytes,
) -> None:
    data_be = data_be_raw.raw_bytes()
    array = _get_numpy_array(data_be, dtype, endian)
    v = Vector.from_numpy(array)
    assert v.dtype == dtype
    assert v.raw() == data_be
    assert nan_equals(array.tolist(), v.to_native())


@pytest.mark.skipif(np is None, reason="numpy not installed")
@pytest.mark.parametrize("dtype", ("i8", "i16", "i32", "i64", "f32", "f64"))
@pytest.mark.parametrize(
    "endian",
    (
        None,
        *ENDIAN_LITERALS,
    ),
)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
def test_to_numpy_random(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: T_ENDIAN_LITERAL | None,
    repeat: int,
    size: int,
) -> None:
    type_size = _get_type_size(dtype)
    np_type = _get_numpy_dtype(dtype)
    for _ in range(repeat):
        data = _random_value_be_bytes(type_size, size)
        v = _vector_from_data(data, dtype, endian)
        array = v.to_numpy()
        assert array.dtype == np.dtype(f">{np_type}")
        assert array.size == len(data) // type_size
        assert array.tobytes() == data
        assert nan_equals(array.tolist(), v.to_native())


@pytest.mark.skipif(np is None, reason="numpy not installed")
@pytest.mark.parametrize(("dtype", "value", "data_be_raw"), SPECIAL_VALUES)
@pytest.mark.parametrize(
    "endian",
    (
        None,
        *ENDIAN_LITERALS,
    ),
)
def test_to_numpy_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: T_ENDIAN_LITERAL | None,
    value: object,
    data_be_raw: NormalizableBytes,
) -> None:
    data_be = data_be_raw.raw_bytes()
    np_type = _get_numpy_dtype(dtype)
    v = _vector_from_data(data_be, dtype, endian)
    array = v.to_numpy()
    assert array.dtype == np.dtype(f">{np_type}")
    assert array.size == 1
    assert array.tobytes() == data_be
    assert nan_equals(array.tolist(), v.to_native())


def _get_pyarrow_dtype(dtype: str) -> pyarrow.DataType:
    return {
        "i8": pa.int8(),
        "i16": pa.int16(),
        "i32": pa.int32(),
        "i64": pa.int64(),
        "f32": pa.float32(),
        "f64": pa.float64(),
    }[dtype]


def _get_pyarrow_array(data_be: bytes, dtype: str) -> pyarrow.Array:
    type_size = _get_type_size(dtype)
    length = len(data_be) // type_size
    data_in = data_be
    if sys.byteorder == "little":
        data_in = _swap_endian(type_size, data_be)
    pa_type = _get_pyarrow_dtype(dtype)
    buffers = [None, pa.py_buffer(data_in)]
    return pa.Array.from_buffers(pa_type, length, buffers, 0)


@pytest.mark.skipif(pa is None, reason="pyarrow not installed")
@pytest.mark.parametrize("dtype", ("i8", "i16", "i32", "i64", "f32", "f64"))
@pytest.mark.parametrize("endian", ("big", "little", "native"))
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
@pytest.mark.parametrize("use_init", (False, True))
def test_from_pyarrow_random(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: t.Literal["big", "little", "native"],
    repeat: int,
    size: int,
    use_init: bool,
) -> None:
    type_size = _get_type_size(dtype)
    for _ in range(repeat):
        data_be = _random_value_be_bytes(type_size, size)
        array = _get_pyarrow_array(data_be, dtype)

        v = Vector(array) if use_init else Vector.from_pyarrow(array)
        assert v.dtype == dtype
        assert v.raw() == data_be
        assert nan_equals(array.to_pylist(), v.to_native())


@pytest.mark.skipif(pa is None, reason="pyarrow not installed")
@pytest.mark.parametrize(("dtype", "value", "data_be_raw"), SPECIAL_VALUES)
def test_from_pyarrow_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    data_be_raw: NormalizableBytes,
) -> None:
    data_be = data_be_raw.raw_bytes()
    array = _get_pyarrow_array(data_be, dtype)
    v = Vector.from_pyarrow(array)
    assert v.dtype == dtype
    assert v.raw() == data_be
    assert nan_equals(array.to_pylist(), v.to_native())


@pytest.mark.skipif(pa is None, reason="pyarrow not installed")
@pytest.mark.parametrize("dtype", ("i8", "i16", "i32", "i64", "f32", "f64"))
@pytest.mark.parametrize(
    "endian",
    (
        None,
        *ENDIAN_LITERALS,
    ),
)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
def test_to_pyarrow_random(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: T_ENDIAN_LITERAL | None,
    repeat: int,
    size: int,
) -> None:
    type_size = _get_type_size(dtype)
    pa_type = _get_pyarrow_dtype(dtype)
    for _ in range(repeat):
        data_be = _random_value_be_bytes(type_size, size)
        data_ne = data_be
        if sys.byteorder == "little":
            data_ne = _swap_endian(type_size, data_be)
        v = _vector_from_data(data_be, dtype, endian)
        array = v.to_pyarrow()
        assert array.type == pa_type
        assert pa_compute.count(array, mode="only_null").as_py() == 0
        buffers = array.buffers()
        assert len(buffers) == 2
        assert buffers[0] is None
        assert buffers[1].to_pybytes() == data_ne
        assert nan_equals(array.tolist(), v.to_native())


@pytest.mark.skipif(pa is None, reason="pyarrow not installed")
@pytest.mark.parametrize(("dtype", "value", "data_be_raw"), SPECIAL_VALUES)
@pytest.mark.parametrize(
    "endian",
    (
        None,
        *ENDIAN_LITERALS,
    ),
)
def test_to_pyarrow_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: T_ENDIAN_LITERAL | None,
    value: object,
    data_be_raw: NormalizableBytes,
) -> None:
    data_be = data_be_raw.raw_bytes()
    type_size = _get_type_size(dtype)
    data_ne = data_be
    if sys.byteorder == "little":
        data_ne = _swap_endian(type_size, data_be)
    pa_type = _get_pyarrow_dtype(dtype)
    v = _vector_from_data(data_be, dtype, endian)
    array = v.to_pyarrow()
    assert array.type == pa_type
    assert pa_compute.count(array, mode="only_null").as_py() == 0
    buffers = array.buffers()
    assert len(buffers) == 2
    assert buffers[0] is None
    assert buffers[1].to_pybytes() == data_ne
    assert nan_equals(array.tolist(), v.to_native())


@pytest.mark.parametrize(
    "vector",
    (
        Vector([], "i8"),
        Vector([], "i16"),
        Vector([], "i32"),
        Vector([], "i64"),
        Vector([], "f32"),
        Vector([], "f64"),
        *(
            Vector([value], dtype)
            for (dtype, value, packed_bytes_be_) in SPECIAL_INT_VALUES
        ),
        *(
            Vector([value], dtype)
            for (dtype, value, packed_bytes_be_) in SPECIAL_FLOAT_VALUES
        ),
    ),
)
def test_vector_repr(vector: Vector) -> None:
    expected = f"Vector({vector.raw()!r}, {vector.dtype.value!r})"
    assert repr(vector) == expected


@pytest.mark.parametrize("dtype", DTYPE_LITERALS)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
def test_vector_repr_random(
    dtype: T_DTYPE_LITERAL,
    repeat: int,
    size: int,
) -> None:
    type_size = _get_type_size(dtype)
    for _ in range(repeat):
        data = _random_value_be_bytes(type_size, size)
        v = Vector(data, dtype)
        if isinstance(dtype, VectorDType):
            expected_dtype = dtype.value
        else:
            expected_dtype = dtype
        expected = f"Vector({data!r}, {expected_dtype!r})"
        assert repr(v) == expected


def _dtype_to_cypher_type(dtype: T_DTYPE_LITERAL) -> str:
    return {
        "i8": "INTEGER8 NOT NULL",
        "i16": "INTEGER16 NOT NULL",
        "i32": "INTEGER32 NOT NULL",
        "i64": "INTEGER NOT NULL",
        "f32": "FLOAT32 NOT NULL",
        "f64": "FLOAT NOT NULL",
    }[dtype]


def _vec_element_cypher_repr(value: t.Any, dtype: T_DTYPE_LITERAL) -> str:
    if isinstance(value, float) and dtype in {"f32", "f64"}:
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
    if dtype == "f32":
        # account for float32 precision loss
        compressed = struct.unpack(">f", struct.pack(">f", value))[0]
        return repr(compressed)
    return repr(value)


@pytest.mark.parametrize(
    ("vector", "expected"),
    (
        (Vector([], "i8"), "vector([], 0, INTEGER8 NOT NULL)"),
        (Vector([], "i16"), "vector([], 0, INTEGER16 NOT NULL)"),
        (Vector([], "i32"), "vector([], 0, INTEGER32 NOT NULL)"),
        (Vector([], "i64"), "vector([], 0, INTEGER NOT NULL)"),
        (Vector([], "f32"), "vector([], 0, FLOAT32 NOT NULL)"),
        (Vector([], "f64"), "vector([], 0, FLOAT NOT NULL)"),
        *(
            (
                Vector([value], dtype),
                (
                    f"vector([{_vec_element_cypher_repr(value, dtype)}], 1, "
                    f"{_dtype_to_cypher_type(dtype)})"
                ),
            )
            for (dtype, value, packed_bytes_be) in SPECIAL_INT_VALUES
        ),
        *(
            (
                Vector([value], dtype),
                (
                    f"vector([{_vec_element_cypher_repr(value, dtype)}], 1, "
                    f"{_dtype_to_cypher_type(dtype)})"
                ),
            )
            for (dtype, value, packed_bytes_be) in SPECIAL_FLOAT_VALUES
        ),
    ),
)
def test_vector_str(vector: Vector, expected: str) -> None:
    assert str(vector) == expected


@pytest.mark.parametrize("dtype", DTYPE_LITERALS)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
def test_vector_str_random(
    dtype: T_DTYPE_LITERAL,
    repeat: int,
    size: int,
) -> None:
    type_size = _get_type_size(dtype)
    cypher_dtype = _dtype_to_cypher_type(dtype)
    for _ in range(repeat):
        data = _random_value_be_bytes(type_size, size)
        v = Vector(data, dtype)
        values_reprs = (
            _vec_element_cypher_repr(value, dtype) for value in v.to_native()
        )
        values_repr = f"[{', '.join(values_reprs)}]"
        expected = f"vector({values_repr}, {size}, {cypher_dtype})"
        assert str(v) == expected
