#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import struct
from collections import OrderedDict
from io import BytesIO
from math import pi
from unittest import TestCase
from uuid import uuid4

from pytest import raises

from neo4j.packstream import Packer, UnpackableBuffer, Unpacker, Structure


class PackStreamTestCase(TestCase):

    @classmethod
    def packb(cls, *values):
        stream = BytesIO()
        packer = Packer(stream)
        for value in values:
            packer.pack(value)
        return stream.getvalue()

    @classmethod
    def assert_packable(cls, value, packed_value):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        packer.pack(value)
        packed = stream_out.getvalue()
        try:
            assert packed == packed_value
        except AssertionError:
            raise AssertionError("Packed value %r is %r instead of expected %r" %
                                 (value, packed, packed_value))
        unpacked = Unpacker(UnpackableBuffer(packed)).unpack()
        try:
            assert unpacked == value
        except AssertionError:
            raise AssertionError("Unpacked value %r is not equal to original %r" % (unpacked, value))

    def test_none(self):
        self.assert_packable(None, b"\xC0")

    def test_boolean(self):
        self.assert_packable(True, b"\xC3")
        self.assert_packable(False, b"\xC2")

    def test_negative_tiny_int(self):
        for z in range(-16, 0):
            self.assert_packable(z, bytes(bytearray([z + 0x100])))

    def test_positive_tiny_int(self):
        for z in range(0, 128):
            self.assert_packable(z, bytes(bytearray([z])))

    def test_negative_int8(self):
        for z in range(-128, -16):
            self.assert_packable(z, bytes(bytearray([0xC8, z + 0x100])))

    def test_positive_int16(self):
        for z in range(128, 32768):
            expected = b"\xC9" + struct.pack(">h", z)
            self.assert_packable(z, expected)

    def test_negative_int16(self):
        for z in range(-32768, -128):
            expected = b"\xC9" + struct.pack(">h", z)
            self.assert_packable(z, expected)

    def test_positive_int32(self):
        for e in range(15, 31):
            z = 2 ** e
            expected = b"\xCA" + struct.pack(">i", z)
            self.assert_packable(z, expected)

    def test_negative_int32(self):
        for e in range(15, 31):
            z = -(2 ** e + 1)
            expected = b"\xCA" + struct.pack(">i", z)
            self.assert_packable(z, expected)

    def test_positive_int64(self):
        for e in range(31, 63):
            z = 2 ** e
            expected = b"\xCB" + struct.pack(">q", z)
            self.assert_packable(z, expected)

    def test_negative_int64(self):
        for e in range(31, 63):
            z = -(2 ** e + 1)
            expected = b"\xCB" + struct.pack(">q", z)
            self.assert_packable(z, expected)

    def test_integer_positive_overflow(self):
        with raises(OverflowError):
            self.packb(2 ** 63 + 1)

    def test_integer_negative_overflow(self):
        with raises(OverflowError):
            self.packb(-(2 ** 63) - 1)

    def test_zero_float64(self):
        zero = 0.0
        expected = b"\xC1" + struct.pack(">d", zero)
        self.assert_packable(zero, expected)

    def test_tau_float64(self):
        tau = 2 * pi
        expected = b"\xC1" + struct.pack(">d", tau)
        self.assert_packable(tau, expected)

    def test_positive_float64(self):
        for e in range(0, 100):
            r = float(2 ** e) + 0.5
            expected = b"\xC1" + struct.pack(">d", r)
            self.assert_packable(r, expected)

    def test_negative_float64(self):
        for e in range(0, 100):
            r = -(float(2 ** e) + 0.5)
            expected = b"\xC1" + struct.pack(">d", r)
            self.assert_packable(r, expected)

    def test_empty_bytes(self):
        self.assert_packable(b"", b"\xCC\x00")

    def test_empty_bytearray(self):
        self.assert_packable(bytearray(), b"\xCC\x00")

    def test_bytes_8(self):
        self.assert_packable(bytearray(b"hello"), b"\xCC\x05hello")

    def test_bytes_16(self):
        b = bytearray(40000)
        self.assert_packable(b, b"\xCD\x9C\x40" + b)

    def test_bytes_32(self):
        b = bytearray(80000)
        self.assert_packable(b, b"\xCE\x00\x01\x38\x80" + b)

    def test_bytearray_size_overflow(self):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with raises(OverflowError):
            packer.pack_bytes_header(2 ** 32)

    def test_empty_string(self):
        self.assert_packable(u"", b"\x80")

    def test_tiny_strings(self):
        for size in range(0x10):
            self.assert_packable(u"A" * size, bytes(bytearray([0x80 + size]) + (b"A" * size)))

    def test_string_8(self):
        t = u"A" * 40
        b = t.encode("utf-8")
        self.assert_packable(t, b"\xD0\x28" + b)

    def test_string_16(self):
        t = u"A" * 40000
        b = t.encode("utf-8")
        self.assert_packable(t, b"\xD1\x9C\x40" + b)

    def test_string_32(self):
        t = u"A" * 80000
        b = t.encode("utf-8")
        self.assert_packable(t, b"\xD2\x00\x01\x38\x80" + b)

    def test_unicode_string(self):
        t = u"héllö"
        b = t.encode("utf-8")
        self.assert_packable(t, bytes(bytearray([0x80 + len(b)])) + b)

    def test_string_size_overflow(self):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with raises(OverflowError):
            packer.pack_string_header(2 ** 32)

    def test_empty_list(self):
        self.assert_packable([], b"\x90")

    def test_tiny_lists(self):
        for size in range(0x10):
            data_out = bytearray([0x90 + size]) + bytearray([1] * size)
            self.assert_packable([1] * size, bytes(data_out))

    def test_list_8(self):
        l = [1] * 40
        self.assert_packable(l, b"\xD4\x28" + (b"\x01" * 40))

    def test_list_16(self):
        l = [1] * 40000
        self.assert_packable(l, b"\xD5\x9C\x40" + (b"\x01" * 40000))

    def test_list_32(self):
        l = [1] * 80000
        self.assert_packable(l, b"\xD6\x00\x01\x38\x80" + (b"\x01" * 80000))

    def test_nested_lists(self):
        self.assert_packable([[[]]], b"\x91\x91\x90")

    def test_list_stream(self):
        packed_value = b"\xD7\x01\x02\x03\xDF"
        unpacked_value = [1, 2, 3]
        stream_out = BytesIO()
        packer = Packer(stream_out)
        packer.pack_list_stream_header()
        packer.pack(1)
        packer.pack(2)
        packer.pack(3)
        packer.pack_end_of_stream()
        packed = stream_out.getvalue()
        try:
            assert packed == packed_value
        except AssertionError:
            raise AssertionError("Packed value is %r instead of expected %r" %
                                 (packed, packed_value))
        unpacked = Unpacker(UnpackableBuffer(packed)).unpack()
        try:
            assert unpacked == unpacked_value
        except AssertionError:
            raise AssertionError("Unpacked value %r is not equal to expected %r" %
                                 (unpacked, unpacked_value))

    def test_list_size_overflow(self):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with raises(OverflowError):
            packer.pack_list_header(2 ** 32)

    def test_empty_map(self):
        self.assert_packable({}, b"\xA0")

    def test_tiny_maps(self):
        for size in range(0x10):
            data_in = OrderedDict()
            data_out = bytearray([0xA0 + size])
            for el in range(1, size + 1):
                data_in[chr(64 + el)] = el
                data_out += bytearray([0x81, 64 + el, el])
            self.assert_packable(data_in, bytes(data_out))

    def test_map_8(self):
        d = OrderedDict([(u"A%s" % i, 1) for i in range(40)])
        b = b"".join(self.packb(u"A%s" % i, 1) for i in range(40))
        self.assert_packable(d, b"\xD8\x28" + b)

    def test_map_16(self):
        d = OrderedDict([(u"A%s" % i, 1) for i in range(40000)])
        b = b"".join(self.packb(u"A%s" % i, 1) for i in range(40000))
        self.assert_packable(d, b"\xD9\x9C\x40" + b)

    def test_map_32(self):
        d = OrderedDict([(u"A%s" % i, 1) for i in range(80000)])
        b = b"".join(self.packb(u"A%s" % i, 1) for i in range(80000))
        self.assert_packable(d, b"\xDA\x00\x01\x38\x80" + b)

    def test_map_stream(self):
        packed_value = b"\xDB\x81A\x01\x81B\x02\xDF"
        unpacked_value = {u"A": 1, u"B": 2}
        stream_out = BytesIO()
        packer = Packer(stream_out)
        packer.pack_map_stream_header()
        packer.pack(u"A")
        packer.pack(1)
        packer.pack(u"B")
        packer.pack(2)
        packer.pack_end_of_stream()
        packed = stream_out.getvalue()
        try:
            assert packed == packed_value
        except AssertionError:
            raise AssertionError("Packed value is %r instead of expected %r" %
                                 (packed, packed_value))
        unpacked = Unpacker(UnpackableBuffer(packed)).unpack()
        try:
            assert unpacked == unpacked_value
        except AssertionError:
            raise AssertionError("Unpacked value %r is not equal to expected %r" %
                                 (unpacked, unpacked_value))

    def test_map_size_overflow(self):
        stream_out = BytesIO()
        packer = Packer(stream_out)
        with raises(OverflowError):
            packer.pack_map_header(2 ** 32)

    def test_illegal_signature(self):
        with self.assertRaises(ValueError):
            self.assert_packable(Structure(b"XXX"), b"\xB0XXX")

    def test_empty_struct(self):
        self.assert_packable(Structure(b"X"), b"\xB0X")

    def test_tiny_structs(self):
        for size in range(0x10):
            fields = [1] * size
            data_in = Structure(b"A", *fields)
            data_out = bytearray([0xB0 + size, 0x41] + fields)
            self.assert_packable(data_in, bytes(data_out))

    def test_struct_size_overflow(self):
        with raises(OverflowError):
            fields = [1] * 16
            self.packb(Structure(b"X", *fields))

    def test_illegal_uuid(self):
        with self.assertRaises(ValueError):
            self.assert_packable(uuid4(), b"\xB0XXX")
