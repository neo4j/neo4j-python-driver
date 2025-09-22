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

import math
import random
import struct
import sys
import typing as t

import pytest

from neo4j._optional_deps import (
    np,
    pa,
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


def _normalize_float_bytes(dtype: str, data: bytes) -> bytes:
    if dtype not in {"f32", "f64"}:
        raise ValueError(f"Invalid dtype {dtype}")
    type_size = _get_type_size(dtype)
    pack_format = _dtype_to_pack_format(dtype)
    chunks = (data[i : i + type_size] for i in range(0, len(data), type_size))
    return bytes(
        b
        for chunk in chunks
        for b in struct.pack(pack_format, struct.unpack(pack_format, chunk)[0])
    )


def _dtype_to_pack_format(dtype: str) -> str:
    return {
        "i8": ">b",
        "i16": ">h",
        "i32": ">i",
        "i64": ">q",
        "f32": ">f",
        "f64": ">d",
    }[dtype]


def _mock_mask_extensions(mocker, used_ext):
    from neo4j.vector import (
        _swap_endian_unchecked_np,
        _swap_endian_unchecked_py,
        _swap_endian_unchecked_rust,
        _VecF32,
        _VecF64,
        _VecI8,
        _VecI16,
        _VecI32,
        _VecI64,
    )

    vec_types = (_VecF64, _VecF32, _VecI64, _VecI32, _VecI16, _VecI8)
    match used_ext:
        case "numpy":
            if _swap_endian_unchecked_np is None:
                pytest.skip("numpy not installed")
            mocker.patch(
                "neo4j.vector._swap_endian_unchecked",
                new=_swap_endian_unchecked_np,
            )
            for vec_type in vec_types:
                mocker.patch(
                    f"neo4j.vector.{vec_type.__name__}.from_native",
                    new=vec_type._from_native_np,
                )
                mocker.patch(
                    f"neo4j.vector.{vec_type.__name__}.to_native",
                    new=vec_type._to_native_np,
                )
        case "rust":
            if _swap_endian_unchecked_rust is None:
                pytest.skip("rust extensions are not installed")
            mocker.patch(
                "neo4j.vector._swap_endian_unchecked",
                new=_swap_endian_unchecked_rust,
            )
            for vec_type in vec_types:
                mocker.patch(
                    f"neo4j.vector.{vec_type.__name__}.from_native",
                    new=vec_type._from_native_rust,
                )
                mocker.patch(
                    f"neo4j.vector.{vec_type.__name__}.to_native",
                    new=vec_type._to_native_rust,
                )
        case "python":
            mocker.patch(
                "neo4j.vector._swap_endian_unchecked",
                new=_swap_endian_unchecked_py,
            )
            for vec_type in vec_types:
                mocker.patch(
                    f"neo4j.vector.{vec_type.__name__}.from_native",
                    new=vec_type._from_native_py,
                )
                mocker.patch(
                    f"neo4j.vector.{vec_type.__name__}.to_native",
                    new=vec_type._to_native_py,
                )
        case _:
            raise ValueError(f"Invalid ext value {used_ext}")


@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
def test_swap_endian(mocker, ext):
    data = bytes(range(1, 17))
    _mock_mask_extensions(mocker, ext)
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
def test_swap_endian_unhandled_size(mocker, ext, type_size):
    data = bytes(i % 256 for i in range(1, abs(type_size) * 4))
    _mock_mask_extensions(mocker, ext)

    with pytest.raises(ValueError, match=str(type_size)):
        _swap_endian(type_size, data)


@pytest.mark.parametrize(
    ("dtype", "data"),
    (
        ("i8", b""),
        ("i8", b"\x01"),
        ("i8", b"\x01\x02\x03\x04"),
        ("i8", _max_value_be_bytes(1, 4096)),
        ("i16", b""),
        ("i16", b"\x00\x01"),
        ("i16", b"\x00\x01\x00\x02"),
        ("i16", _max_value_be_bytes(2, 4096)),
        ("i32", b""),
        ("i32", b"\x00\x00\x00\x01"),
        ("i32", b"\x00\x00\x00\x01\x00\x00\x00\x02"),
        ("i32", _max_value_be_bytes(4, 4096)),
        ("i64", b""),
        ("i64", b"\x00\x00\x00\x00\x00\x00\x00\x01"),
        (
            "i64",
            (
                b"\x00\x00\x00\x00\x00\x00\x00\x01"
                b"\x00\x00\x00\x00\x00\x00\x00\x02"
            ),
        ),
        ("i64", _max_value_be_bytes(8, 4096)),
        ("f32", b""),
        ("f32", _random_value_be_bytes(4, 4096)),
        ("f64", b""),
        ("f64", _random_value_be_bytes(8, 4096)),
    ),
)
@pytest.mark.parametrize("input_endian", (None, *ENDIAN_LITERALS))
@pytest.mark.parametrize("as_bytearray", (False, True))
def test_raw_data(
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
@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
@pytest.mark.parametrize("use_init", (False, True))
def test_from_native_int_random(
    dtype: T_DTYPE_INT_LITERAL,
    repeat: int,
    size: int,
    ext: str,
    use_init: bool,
    mocker: t.Any,
) -> None:
    _mock_mask_extensions(mocker, ext)
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
        if dtype.startswith("f"):
            expected_raw = _normalize_float_bytes(dtype, data)
        assert v.raw() == expected_raw


@pytest.mark.parametrize("dtype", DTYPE_FLOAT_LITERALS)
@pytest.mark.parametrize(("repeat", "size"), ((10_000, 1), (1, 10_000)))
@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
@pytest.mark.parametrize("use_init", (False, True))
def test_from_native_floatgst_random(
    dtype: T_DTYPE_FLOAT_LITERAL,
    repeat: int,
    size: int,
    ext: str,
    use_init: bool,
    mocker: t.Any,
) -> None:
    _mock_mask_extensions(mocker, ext)
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
        if dtype.startswith("f"):
            expected_raw = _normalize_float_bytes(dtype, data)
        assert v.raw() == expected_raw


SPECIAL_INT_VALUES: tuple[tuple[T_DTYPE_INT_LITERAL, int, bytes], ...] = (
    # (dtype, value, packed_bytes_be)
    # i8
    ("i8", -128, b"\x80"),
    ("i8", 0, b"\x00"),
    ("i8", 127, b"\x7f"),
    # i16
    ("i16", -32768, b"\x80\x00"),
    ("i16", 0, b"\x00\x00"),
    ("i16", 32767, b"\x7f\xff"),
    # i32
    ("i32", -2147483648, b"\x80\x00\x00\x00"),
    ("i32", 0, b"\x00\x00\x00\x00"),
    ("i32", 2147483647, b"\x7f\xff\xff\xff"),
    # i64
    ("i64", -9223372036854775808, b"\x80\x00\x00\x00\x00\x00\x00\x00"),
    ("i64", 0, b"\x00\x00\x00\x00\x00\x00\x00\x00"),
    ("i64", 9223372036854775807, b"\x7f\xff\xff\xff\xff\xff\xff\xff"),
)
SPECIAL_FLOAT_VALUES: tuple[
    tuple[T_DTYPE_FLOAT_LITERAL, float, bytes], ...
] = (
    # (dtype, value, packed_bytes_be)
    # f32
    # NaN
    ("f32", float("nan"), b"\x7f\xc0\x00\x00"),
    ("f32", float("-nan"), b"\xff\xc0\x00\x00"),
    (
        "f32",
        struct.unpack(">f", b"\x7f\xc0\x00\x11")[0],
        b"\x7f\xc0\x00\x11",
    ),
    (
        "f32",
        struct.unpack(">f", b"\x7f\x80\x00\x01")[0],
        # Python < 3.14 does not properly preserver all NaN payload
        # when calling struct.pack
        _normalize_float_bytes("f32", b"\x7f\x80\x00\x01"),
    ),
    # ±inf
    ("f32", float("inf"), b"\x7f\x80\x00\x00"),
    ("f32", float("-inf"), b"\xff\x80\x00\x00"),
    # ±0.0
    ("f32", 0.0, b"\x00\x00\x00\x00"),
    ("f32", -0.0, b"\x80\x00\x00\x00"),
    # smallest normal
    (
        "f32",
        struct.unpack(">f", b"\x00\x80\x00\x00")[0],
        b"\x00\x80\x00\x00",
    ),
    (
        "f32",
        struct.unpack(">f", b"\x80\x80\x00\x00")[0],
        b"\x80\x80\x00\x00",
    ),
    # subnormal
    (
        "f32",
        struct.unpack(">f", b"\x00\x00\x00\x01")[0],
        b"\x00\x00\x00\x01",
    ),
    (
        "f32",
        struct.unpack(">f", b"\x80\x00\x00\x01")[0],
        b"\x80\x00\x00\x01",
    ),
    # largest normal
    (
        "f32",
        struct.unpack(">f", b"\x7f\x7f\xff\xff")[0],
        b"\x7f\x7f\xff\xff",
    ),
    (
        "f32",
        struct.unpack(">f", b"\xff\x7f\xff\xff")[0],
        b"\xff\x7f\xff\xff",
    ),
    # f64
    # NaN
    ("f64", float("nan"), b"\x7f\xf8\x00\x00\x00\x00\x00\x00"),
    ("f64", float("-nan"), b"\xff\xf8\x00\x00\x00\x00\x00\x00"),
    (
        "f64",
        struct.unpack(">d", b"\x7f\xf8\x00\x00\x00\x00\x00\x11")[0],
        b"\x7f\xf8\x00\x00\x00\x00\x00\x11",
    ),
    (
        "f64",
        struct.unpack(">d", b"\x7f\xf0\x00\x01\x00\x00\x00\x01")[0],
        b"\x7f\xf0\x00\x01\x00\x00\x00\x01",
    ),
    # ±inf
    ("f64", float("inf"), b"\x7f\xf0\x00\x00\x00\x00\x00\x00"),
    ("f64", float("-inf"), b"\xff\xf0\x00\x00\x00\x00\x00\x00"),
    # ±0.0
    ("f64", 0.0, b"\x00\x00\x00\x00\x00\x00\x00\x00"),
    ("f64", -0.0, b"\x80\x00\x00\x00\x00\x00\x00\x00"),
    # smallest normal
    (
        "f64",
        struct.unpack(">d", b"\x00\x10\x00\x00\x00\x00\x00\x00")[0],
        b"\x00\x10\x00\x00\x00\x00\x00\x00",
    ),
    (
        "f64",
        struct.unpack(">d", b"\x80\x10\x00\x00\x00\x00\x00\x00")[0],
        b"\x80\x10\x00\x00\x00\x00\x00\x00",
    ),
    # subnormal
    (
        "f64",
        struct.unpack(">d", b"\x00\x00\x00\x00\x00\x00\x00\x01")[0],
        b"\x00\x00\x00\x00\x00\x00\x00\x01",
    ),
    (
        "f64",
        struct.unpack(">d", b"\x80\x00\x00\x00\x00\x00\x00\x01")[0],
        b"\x80\x00\x00\x00\x00\x00\x00\x01",
    ),
    # largest normal
    (
        "f64",
        struct.unpack(">d", b"\x7f\xef\xff\xff\xff\xff\xff\xff")[0],
        b"\x7f\xef\xff\xff\xff\xff\xff\xff",
    ),
    (
        "f64",
        struct.unpack(">d", b"\xff\xef\xff\xff\xff\xff\xff\xff")[0],
        b"\xff\xef\xff\xff\xff\xff\xff\xff",
    ),
)
SPECIAL_VALUES = SPECIAL_INT_VALUES + SPECIAL_FLOAT_VALUES


@pytest.mark.parametrize(("dtype", "value", "data_be"), SPECIAL_VALUES)
@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
def test_from_native_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    data_be: bytes,
    ext: str,
    mocker: t.Any,
) -> None:
    _mock_mask_extensions(mocker, ext)
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
@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
def test_from_native_wrong_type(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    ext: str,
    mocker: t.Any,
) -> None:
    _mock_mask_extensions(mocker, ext)
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
    ),
)
@pytest.mark.parametrize("ext", ("numpy", "rust", "python"))
def test_from_native_overflow(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    ext: str,
    mocker: t.Any,
) -> None:
    _mock_mask_extensions(mocker, ext)
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


@pytest.mark.parametrize(("dtype", "value", "data_be"), SPECIAL_VALUES)
def test_to_native_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    data_be: bytes,
) -> None:
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
@pytest.mark.parametrize(("dtype", "value", "data_be"), SPECIAL_VALUES)
@pytest.mark.parametrize("endian", ("big", "little", "native"))
def test_from_numpy_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    endian: t.Literal["big", "little", "native"],
    value: object,
    data_be: bytes,
) -> None:
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
@pytest.mark.parametrize(("dtype", "value", "data_be"), SPECIAL_VALUES)
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
    data_be: bytes,
) -> None:
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
@pytest.mark.parametrize(("dtype", "value", "data_be"), SPECIAL_VALUES)
def test_from_pyarrow_special_values(
    dtype: t.Literal["i8", "i16", "i32", "i64", "f32", "f64"],
    value: object,
    data_be: bytes,
) -> None:
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
        assert pa.compute.count(array, mode="only_null").as_py() == 0
        buffers = array.buffers()
        assert len(buffers) == 2
        assert buffers[0] is None
        assert buffers[1].to_pybytes() == data_ne
        assert nan_equals(array.tolist(), v.to_native())


@pytest.mark.skipif(pa is None, reason="pyarrow not installed")
@pytest.mark.parametrize(("dtype", "value", "data_be"), SPECIAL_VALUES)
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
    data_be: bytes,
) -> None:
    type_size = _get_type_size(dtype)
    data_ne = data_be
    if sys.byteorder == "little":
        data_ne = _swap_endian(type_size, data_be)
    pa_type = _get_pyarrow_dtype(dtype)
    v = _vector_from_data(data_be, dtype, endian)
    array = v.to_pyarrow()
    assert array.type == pa_type
    assert pa.compute.count(array, mode="only_null").as_py() == 0
    buffers = array.buffers()
    assert len(buffers) == 2
    assert buffers[0] is None
    assert buffers[1].to_pybytes() == data_ne
    assert nan_equals(array.tolist(), v.to_native())


@pytest.mark.parametrize(
    ("vector", "expected"),
    (
        (Vector([], "i8"), "Vector(b'', 'i8')"),
        (Vector([], "i16"), "Vector(b'', 'i16')"),
        (Vector([], "i32"), "Vector(b'', 'i32')"),
        (Vector([], "i64"), "Vector(b'', 'i64')"),
        (Vector([], "f32"), "Vector(b'', 'f32')"),
        (Vector([], "f64"), "Vector(b'', 'f64')"),
        *(
            (
                Vector([value], dtype),
                f"Vector({packed_bytes_be!r}, {dtype!r})",
            )
            for (dtype, value, packed_bytes_be) in SPECIAL_INT_VALUES
        ),
        *(
            (
                Vector([value], dtype),
                f"Vector({packed_bytes_be!r}, {dtype!r})",
            )
            for (dtype, value, packed_bytes_be) in SPECIAL_FLOAT_VALUES
        ),
    ),
)
def test_vector_repr(vector: Vector, expected: str) -> None:
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


def _vec_element_cypher_repr(value: t.Any) -> str:
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
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
                    f"vector([{_vec_element_cypher_repr(value)}], 1, "
                    f"{_dtype_to_cypher_type(dtype)})"
                ),
            )
            for (dtype, value, packed_bytes_be) in SPECIAL_INT_VALUES
        ),
        *(
            (
                Vector([value], dtype),
                (
                    f"vector([{_vec_element_cypher_repr(value)}], 1, "
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
        values_repr = (
            f"[{', '.join(map(_vec_element_cypher_repr, v.to_native()))}]"
        )
        expected = f"vector({values_repr}, {size}, {cypher_dtype})"
        assert str(v) == expected
