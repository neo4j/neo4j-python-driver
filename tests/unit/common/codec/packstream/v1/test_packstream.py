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


import struct
from io import BytesIO
from math import (
    isnan,
    pi,
)
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

from neo4j._codec.packstream import Structure
from neo4j._codec.packstream.v1 import (
    PackableBuffer,
    Packer,
    UnpackableBuffer,
    Unpacker,
)


standard_ascii = [chr(i) for i in range(128)]
not_ascii = "♥O◘♦♥O◘♦"


@pytest.fixture
def packer_with_buffer():
    packable_buffer = Packer.new_packable_buffer()
    return Packer(packable_buffer), packable_buffer


@pytest.fixture
def unpacker_with_buffer():
    unpackable_buffer = Unpacker.new_unpackable_buffer()
    return Unpacker(unpackable_buffer), unpackable_buffer

def test_packable_buffer(packer_with_buffer):
    packer, packable_buffer = packer_with_buffer
    assert isinstance(packable_buffer, PackableBuffer)
    assert packable_buffer is packer.stream

def test_unpackable_buffer(unpacker_with_buffer):
    unpacker, unpackable_buffer = unpacker_with_buffer
    assert isinstance(unpackable_buffer, UnpackableBuffer)
    assert unpackable_buffer is unpacker.unpackable


@pytest.fixture
def pack(packer_with_buffer):
    packer, packable_buffer = packer_with_buffer

    def _pack(*values, dehydration_hooks=None):
        for value in values:
            packer.pack(value, dehydration_hooks=dehydration_hooks)
        data = bytearray(packable_buffer.data)
        packable_buffer.clear()
        return data

    return _pack


_default_out_value = object()


@pytest.fixture
def assert_packable(packer_with_buffer, unpacker_with_buffer):
    def _recursive_nan_equal(a, b):
        if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
            return all(_recursive_nan_equal(x, y) for x, y in zip(a, b))
        elif isinstance(a, dict) and isinstance(b, dict):
            return all(_recursive_nan_equal(a[k], b[k]) for k in a)
        else:
            return a == b or (isnan(a) and isnan(b))

    def _assert(in_value, packed_value, out_value=_default_out_value):
        if out_value is _default_out_value:
            out_value = in_value
        nonlocal packer_with_buffer, unpacker_with_buffer
        packer, packable_buffer = packer_with_buffer
        unpacker, unpackable_buffer = unpacker_with_buffer
        packable_buffer.clear()
        unpackable_buffer.reset()

        packer.pack(in_value)
        packed_data = packable_buffer.data
        assert packed_data == packed_value

        unpackable_buffer.data = bytearray(packed_data)
        unpackable_buffer.used = len(packed_data)
        unpacked_data = unpacker.unpack()
        assert _recursive_nan_equal(unpacked_data, out_value)

    return _assert


@pytest.fixture(params=(True, False))
def np_float_overflow_as_error(request):
    should_raise = request.param
    if should_raise:
        old_err = np.seterr(over="raise")
    else:
        old_err = np.seterr(over="ignore")
    yield
    np.seterr(**old_err)



@pytest.fixture(params=(
    int,
    np.int8, np.int16, np.int32, np.int64, np.longlong,
    np.uint8, np.uint16, np.uint32, np.uint64, np.ulonglong
))
def int_type(request):
    if issubclass(request.param, np.number):
        def _int_type(value):
            # this avoids deprecation warning from NEP50 and forces
            # c-style wrapping of the value
            return np.array(value).astype(request.param).item()

        return _int_type
    else:
        return request.param


@pytest.fixture(params=(float,
                        np.float16, np.float32, np.float64, np.longdouble))
def float_type(request, np_float_overflow_as_error):
    return request.param


@pytest.fixture(params=(bool, np.bool_))
def bool_type(request):
    return request.param


@pytest.fixture(params=(bytes, bytearray, np.bytes_))
def bytes_type(request):
    return request.param


@pytest.fixture(params=(str, np.str_))
def str_type(request):
    return request.param


@pytest.fixture(params=(list, tuple, np.array,
                        pd.Series, pd.array, pd.arrays.SparseArray))
def sequence_type(request):
    if request.param is pd.Series:
        def constructor(value):
            if not value:
                return pd.Series(dtype=object)
            return pd.Series(value)

        return constructor
    return request.param


class TestPackStream:
    @pytest.mark.parametrize("value", (None, pd.NA))
    def test_none(self, value, assert_packable):
        assert_packable(value, b"\xC0", None)

    def test_boolean(self, bool_type, assert_packable):
        assert_packable(bool_type(True), b"\xC3")
        assert_packable(bool_type(False), b"\xC2")

    @pytest.mark.parametrize("dtype", (bool, pd.BooleanDtype()))
    def test_boolean_pandas_series(self, dtype, assert_packable):
        value = [True, False]
        value_series = pd.Series(value, dtype=dtype)
        assert_packable(value_series, b"\x92\xC3\xC2", value)

    def test_negative_tiny_int(self, int_type, assert_packable):
        for z in range(-16, 0):
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            assert_packable(z_typed, bytes(bytearray([z + 0x100])))

    @pytest.mark.parametrize("dtype", (
        int, pd.Int8Dtype(), pd.Int16Dtype(), pd.Int32Dtype(), pd.Int64Dtype(),
        np.int8, np.int16, np.int32, np.int64, np.longlong,
    ))
    def test_negative_tiny_int_pandas_series(self, dtype, assert_packable):
        for z in range(-16, 0):
            z_typed = pd.Series(z, dtype=dtype)
            assert_packable(z_typed, bytes(bytearray([0x91, z + 0x100])), [z])

    def test_positive_tiny_int(self, int_type, assert_packable):
        for z in range(0, 128):
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            assert_packable(z_typed, bytes(bytearray([z])))

    def test_negative_int8(self, int_type, assert_packable):
        for z in range(-128, -16):
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            assert_packable(z_typed, bytes(bytearray([0xC8, z + 0x100])))

    def test_positive_int16(self, int_type, assert_packable):
        for z in range(128, 32768):
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            expected = b"\xC9" + struct.pack(">h", z)
            assert_packable(z_typed, expected)

    def test_negative_int16(self, int_type, assert_packable):
        for z in range(-32768, -128):
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            expected = b"\xC9" + struct.pack(">h", z)
            assert_packable(z_typed, expected)

    def test_positive_int32(self, int_type, assert_packable):
        for e in range(15, 31):
            z = 2 ** e
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            expected = b"\xCA" + struct.pack(">i", z)
            assert_packable(z_typed, expected)

    def test_negative_int32(self, int_type, assert_packable):
        for e in range(15, 31):
            z = -(2 ** e + 1)
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            expected = b"\xCA" + struct.pack(">i", z)
            assert_packable(z_typed, expected)

    def test_positive_int64(self, int_type, assert_packable):
        for e in range(31, 63):
            z = 2 ** e
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            expected = b"\xCB" + struct.pack(">q", z)
            assert_packable(z_typed, expected)

    @pytest.mark.parametrize("dtype", (
        int, pd.Int64Dtype(), pd.UInt64Dtype(),
        np.int64, np.longlong, np.uint64, np.ulonglong,
    ))
    def test_positive_int64_pandas_series(self, dtype, assert_packable):
        for e in range(31, 63):
            z = 2 ** e
            z_typed = pd.Series(z, dtype=dtype)
            expected = b"\x91\xCB" + struct.pack(">q", z)
            assert_packable(z_typed, expected, [z])

    def test_negative_int64(self, int_type, assert_packable):
        for e in range(31, 63):
            z = -(2 ** e + 1)
            z_typed = int_type(z)
            if z != int(z_typed):
                continue  # not representable
            expected = b"\xCB" + struct.pack(">q", z)
            assert_packable(z_typed, expected)

    @pytest.mark.parametrize("dtype", (
        int, pd.Int64Dtype(), np.int64, np.longlong,
    ))
    def test_negative_int64_pandas_series(self, dtype, assert_packable):
        for e in range(31, 63):
            z = -(2 ** e + 1)
            z_typed = pd.Series(z, dtype=dtype)
            expected = b"\x91\xCB" + struct.pack(">q", z)
            assert_packable(z_typed, expected, [z])

    def test_integer_positive_overflow(self, int_type, pack, assert_packable):
        with pytest.raises(OverflowError):
            z = 2 ** 63 + 1
            z_typed = int_type(z)
            if z != int(z_typed):
                pytest.skip("not representable")
            pack(z_typed)

    def test_integer_negative_overflow(self, int_type, pack, assert_packable):
        with pytest.raises(OverflowError):
            z = -(2 ** 63) - 1
            z_typed = int_type(z)
            if z != int(z_typed):
                pytest.skip("not representable")
            pack(z_typed)

    def test_float(self, float_type, assert_packable):
        for z in (
            0.0, -0.0, pi, 2 * pi, float("inf"), float("-inf"), float("nan"),
            *(float(2 ** e) + 0.5 for e in range(100)),
            *(-float(2 ** e) + 0.5 for e in range(100)),
        ):
            print(z)
            try:
                z_typed = float_type(z)
            except FloatingPointError:
                continue  # not representable
            expected = b"\xC1" + struct.pack(">d", float(z_typed))
            assert_packable(z_typed, expected)

    @pytest.mark.parametrize("dtype", (
        float, pd.Float32Dtype(),  pd.Float64Dtype(),
        np.float16, np.float32, np.float64, np.longdouble,
    ))
    def test_float_pandas_series(self, dtype, np_float_overflow_as_error,
                                 assert_packable):
        for z in (
            0.0, -0.0, pi, 2 * pi, float("inf"), float("-inf"), float("nan"),
            *(float(2 ** e) + 0.5 for e in range(100)),
            *(-float(2 ** e) + 0.5 for e in range(100)),
        ):
            try:
                z_typed = pd.Series(z, dtype=dtype)
            except FloatingPointError:
                continue  # not representable
            if z_typed[0] is pd.NA:
                expected_bytes = b"\x91\xC0"  # encoded as NULL
                expected_value = [None]
            else:
                expected_bytes = (b"\x91\xC1"
                                  + struct.pack(">d", float(z_typed[0])))
                expected_value = [float(z_typed[0])]
            assert_packable(z_typed, expected_bytes, expected_value)

    def test_empty_bytes(self, bytes_type, assert_packable):
        b = bytes_type(b"")
        assert_packable(b, b"\xCC\x00")

    def test_bytes_8(self, bytes_type, assert_packable):
        b = bytes_type(b"hello")
        assert_packable(b, b"\xCC\x05hello")

    def test_bytes_16(self, bytes_type, assert_packable):
        b = bytearray(40000)
        b_typed = bytes_type(b)
        assert_packable(b_typed, b"\xCD\x9C\x40" + b)

    def test_bytes_32(self, bytes_type, assert_packable):
        b = bytearray(80000)
        b_typed = bytes_type(b)
        assert_packable(b_typed, b"\xCE\x00\x01\x38\x80" + b)

    def test_bytes_pandas_series(self, assert_packable):
        for b, header in (
            (b"", b"\xCC\x00"),
            (b"hello", b"\xCC\x05"),
            (bytearray(40000), b"\xCD\x9C\x40"),
            (bytearray(80000), b"\xCE\x00\x01\x38\x80"),
        ):
            b_typed = pd.Series([b])
            assert_packable(b_typed, b"\x91" + header + b, [b])

    def test_bytearray_size_overflow(self, bytes_type, assert_packable):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with pytest.raises(OverflowError):
            packer._pack_bytes_header(2 ** 32)

    def test_empty_string(self, str_type, assert_packable):
        assert_packable(str_type(""), b"\x80")

    def test_tiny_strings(self, str_type, assert_packable):
        for size in range(0x10):
            s = str_type("A" * size)
            assert_packable(s, bytes(bytearray([0x80 + size]) + (b"A" * size)))

    def test_string_8(self, str_type, assert_packable):
        t = "A" * 40
        b = t.encode("utf-8")
        t_typed = str_type(t)
        assert_packable(t_typed, b"\xD0\x28" + b)

    def test_string_16(self, str_type, assert_packable):
        t = "A" * 40000
        b = t.encode("utf-8")
        t_typed = str_type(t)
        assert_packable(t_typed, b"\xD1\x9C\x40" + b)

    def test_string_32(self, str_type, assert_packable):
        t = "A" * 80000
        b = t.encode("utf-8")
        t_typed = str_type(t)
        assert_packable(t_typed, b"\xD2\x00\x01\x38\x80" + b)

    def test_unicode_string(self, str_type, assert_packable):
        t = "héllö"
        b = t.encode("utf-8")
        t_typed = str_type(t)
        assert_packable(t_typed, bytes(bytearray([0x80 + len(b)])) + b)

    @pytest.mark.parametrize("dtype", (
        str, np.str_, pd.StringDtype("python"), pd.StringDtype("pyarrow"),
    ))
    def test_string_pandas_series(self, dtype, assert_packable):
        values = (
            ("", b"\x80"),
            ("A" * 40, b"\xD0\x28"),
            ("A" * 40000, b"\xD1\x9C\x40"),
            ("A" * 80000, b"\xD2\x00\x01\x38\x80"),
        )
        for t, header in values:
            t_typed = pd.Series([t], dtype=dtype)
            assert_packable(t_typed, b"\x91" + header + t.encode("utf-8"), [t])

        t_typed = pd.Series([t for t, _ in values], dtype=dtype)
        expected = (
            bytes([0x90 + len(values)])
            + b"".join(header + t.encode("utf-8") for t, header in values)
        )
        assert_packable(t_typed, expected, [t for t, _ in values])

    def test_string_size_overflow(self):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with pytest.raises(OverflowError):
            packer._pack_string_header(2 ** 32)

    def test_empty_list(self, sequence_type, assert_packable):
        l = []
        l_typed = sequence_type(l)
        assert_packable(l_typed, b"\x90", l)

    def test_tiny_lists(self, sequence_type, assert_packable):
        for size in range(0x10):
            l = [1] * size
            l_typed = sequence_type(l)
            data_out = bytearray([0x90 + size]) + bytearray([1] * size)
            assert_packable(l_typed, bytes(data_out), l)

    def test_list_8(self, sequence_type, assert_packable):
        l = [1] * 40
        l_typed = sequence_type(l)
        assert_packable(l_typed, b"\xD4\x28" + (b"\x01" * 40), l)

    def test_list_16(self, sequence_type, assert_packable):
        l = [1] * 40000
        l_typed = sequence_type(l)
        assert_packable(l_typed, b"\xD5\x9C\x40" + (b"\x01" * 40000), l)

    def test_list_32(self, sequence_type, assert_packable):
        l = [1] * 80000
        l_typed = sequence_type(l)
        assert_packable(l_typed, b"\xD6\x00\x01\x38\x80" + (b"\x01" * 80000), l)

    def test_nested_lists(self, sequence_type, assert_packable):
        l = [[[]]]
        l_typed = sequence_type([sequence_type([sequence_type([])])])
        assert_packable(l_typed, b"\x91\x91\x90", l)

    @pytest.mark.parametrize("as_series", (True, False))
    def test_list_pandas_categorical(self, as_series, pack, assert_packable):
        l = ["cat", "dog", "cat", "cat", "dog", "horse"]
        l_typed = pd.Categorical(l)
        if as_series:
            l_typed = pd.Series(l_typed)
        b = b"".join([
            b"\x96",
            *(pack(e) for e in l)
        ])
        assert_packable(l_typed, b, l)

    def test_list_size_overflow(self):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with pytest.raises(OverflowError):
            packer._pack_list_header(2 ** 32)

    def test_empty_map(self, assert_packable):
        assert_packable({}, b"\xA0")

    @pytest.mark.parametrize("size", range(0x10))
    def test_tiny_maps(self, assert_packable, size):
        data_in = dict()
        data_out = bytearray([0xA0 + size])
        for el in range(1, size + 1):
            data_in[chr(64 + el)] = el
            data_out += bytearray([0x81, 64 + el, el])
        assert_packable(data_in, bytes(data_out))

    def test_map_8(self, pack, assert_packable):
        d = dict([(u"A%s" % i, 1) for i in range(40)])
        b = b"".join(pack(u"A%s" % i, 1) for i in range(40))
        assert_packable(d, b"\xD8\x28" + b)

    def test_map_16(self, pack, assert_packable):
        d = dict([(u"A%s" % i, 1) for i in range(40000)])
        b = b"".join(pack(u"A%s" % i, 1) for i in range(40000))
        assert_packable(d, b"\xD9\x9C\x40" + b)

    def test_map_32(self, pack, assert_packable):
        d = dict([(u"A%s" % i, 1) for i in range(80000)])
        b = b"".join(pack(u"A%s" % i, 1) for i in range(80000))
        assert_packable(d, b"\xDA\x00\x01\x38\x80" + b)

    def test_empty_dataframe_maps(self, assert_packable):
        df = pd.DataFrame()
        assert_packable(df, b"\xA0", {})

    @pytest.mark.parametrize("size", range(0x10))
    def test_tiny_dataframes_maps(self, assert_packable, size):
        data_in = dict()
        data_out = bytearray([0xA0 + size])
        for el in range(1, size + 1):
            data_in[chr(64 + el)] = [el]
            data_out += bytearray([0x81, 64 + el, 0x91, el])
        data_in_typed = pd.DataFrame(data_in)
        assert_packable(data_in_typed, bytes(data_out), data_in)

    def test_map_size_overflow(self):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with pytest.raises(OverflowError):
            packer._pack_map_header(2 ** 32)

    @pytest.mark.parametrize(("map_", "exc_type"), (
        ({1: "1"}, TypeError),
        (pd.DataFrame({1: ["1"]}), TypeError),
        (pd.DataFrame({(1, 2): ["1"]}), TypeError),
        ({"x": {1: 'eins', 2: 'zwei', 3: 'drei'}}, TypeError),
        ({"x": {(1, 2): '1+2i', (2, 0): '2'}}, TypeError),
    ))
    def test_map_key_type(self, packer_with_buffer, map_, exc_type):
        # maps must have string keys
        packer, packable_buffer = packer_with_buffer
        with pytest.raises(exc_type, match="strings"):
            packer._pack(map_)

    def test_illegal_signature(self, assert_packable):
        with pytest.raises(ValueError):
            assert_packable(Structure(b"XXX"), b"\xB0XXX")

    def test_empty_struct(self, assert_packable):
        assert_packable(Structure(b"X"), b"\xB0X")

    def test_tiny_structs(self, assert_packable):
        for size in range(0x10):
            fields = [1] * size
            data_in = Structure(b"A", *fields)
            data_out = bytearray([0xB0 + size, 0x41] + fields)
            assert_packable(data_in, bytes(data_out))

    def test_struct_size_overflow(self, pack):
        with pytest.raises(OverflowError):
            fields = [1] * 16
            pack(Structure(b"X", *fields))

    def test_illegal_uuid(self, assert_packable):
        with pytest.raises(ValueError):
            assert_packable(uuid4(), b"\xB0XXX")
