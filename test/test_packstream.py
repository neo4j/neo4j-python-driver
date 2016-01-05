#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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
from neo4j.v1.packstream import Packer, Unpacker, packb


def assert_packable(value, packed_value):
    stream_out = BytesIO()
    packer = Packer(stream_out)
    packer.pack(value)
    packed = stream_out.getvalue()
    try:
        assert packed == packed_value
    except AssertionError:
        raise AssertionError("Packed value %r is %r instead of expected %r" %
                             (value, packed, packed_value))
    stream_in = BytesIO(packed)
    unpacker = Unpacker(stream_in)
    unpacked = next(unpacker.unpack())
    try:
        assert unpacked == value
    except AssertionError:
        raise AssertionError("Unpacked value %r is not equal to original %r" % (unpacked, value))


class PackStreamTestCase(TestCase):

    def test_none(self):
        assert_packable(None, b"\xC0")

    def test_boolean(self):
        assert_packable(True, b"\xC3")
        assert_packable(False, b"\xC2")

    def test_negative_tiny_int(self):
        for z in range(-16, 0):
            assert_packable(z, bytes(bytearray([z + 0x100])))

    def test_positive_tiny_int(self):
        for z in range(0, 128):
            assert_packable(z, bytes(bytearray([z])))

    def test_negative_int8(self):
        for z in range(-128, -16):
            assert_packable(z, bytes(bytearray([0xC8, z + 0x100])))

    def test_positive_int16(self):
        for z in range(128, 32768):
            expected = b"\xC9" + struct.pack(">h", z)
            assert_packable(z, expected)

    def test_negative_int16(self):
        for z in range(-32768, -128):
            expected = b"\xC9" + struct.pack(">h", z)
            assert_packable(z, expected)

    def test_positive_int32(self):
        for e in range(15, 31):
            z = 2 ** e
            expected = b"\xCA" + struct.pack(">i", z)
            assert_packable(z, expected)

    def test_negative_int32(self):
        for e in range(15, 31):
            z = -(2 ** e + 1)
            expected = b"\xCA" + struct.pack(">i", z)
            assert_packable(z, expected)

    def test_positive_int64(self):
        for e in range(31, 63):
            z = 2 ** e
            expected = b"\xCB" + struct.pack(">q", z)
            assert_packable(z, expected)

    def test_negative_int64(self):
        for e in range(31, 63):
            z = -(2 ** e + 1)
            expected = b"\xCB" + struct.pack(">q", z)
            assert_packable(z, expected)

    def test_zero_float64(self):
        zero = 0.0
        expected = b"\xC1" + struct.pack(">d", zero)
        assert_packable(zero, expected)

    def test_tau_float64(self):
        tau = 2 * pi
        expected = b"\xC1" + struct.pack(">d", tau)
        assert_packable(tau, expected)

    def test_positive_float64(self):
        for e in range(0, 100):
            r = float(2 ** e) + 0.5
            expected = b"\xC1" + struct.pack(">d", r)
            assert_packable(r, expected)

    def test_negative_float64(self):
        for e in range(0, 100):
            r = -(float(2 ** e) + 0.5)
            expected = b"\xC1" + struct.pack(">d", r)
            assert_packable(r, expected)

    def test_empty_bytes(self):
        assert_packable(bytearray(), b"\xCC\x00")

    def test_bytes_8(self):
        assert_packable(bytearray(b"hello"), b"\xCC\x05hello")

    def test_bytes_16(self):
        b = bytearray(40000)
        assert_packable(b, b"\xCD\x9C\x40" + b)

    def test_bytes_32(self):
        b = bytearray(80000)
        assert_packable(b, b"\xCE\x00\x01\x38\x80" + b)

    def test_empty_string(self):
        assert_packable(u"", b"\x80")

    def test_tiny_string(self):
        assert_packable(u"hello", b"\x85hello")

    def test_string_8(self):
        t = u"A" * 40
        b = t.encode("utf-8")
        assert_packable(t, b"\xD0\x28" + b)

    def test_string_16(self):
        t = u"A" * 40000
        b = t.encode("utf-8")
        assert_packable(t, b"\xD1\x9C\x40" + b)

    def test_string_32(self):
        t = u"A" * 80000
        b = t.encode("utf-8")
        assert_packable(t, b"\xD2\x00\x01\x38\x80" + b)

    def test_unicode_string(self):
        t = u"héllö"
        b = t.encode("utf-8")
        assert_packable(t, bytes(bytearray([0x80 + len(b)])) + b)

    def test_empty_list(self):
        assert_packable([], b"\x90")

    def test_tiny_list(self):
        assert_packable([1, 2, 3], b"\x93\x01\x02\x03")

    def test_list_8(self):
        l = [1] * 40
        assert_packable(l, b"\xD4\x28" + (b"\x01" * 40))

    def test_list_16(self):
        l = [1] * 40000
        assert_packable(l, b"\xD5\x9C\x40" + (b"\x01" * 40000))

    def test_list_32(self):
        l = [1] * 80000
        assert_packable(l, b"\xD6\x00\x01\x38\x80" + (b"\x01" * 80000))

    def test_nested_lists(self):
        assert_packable([[[]]], b"\x91\x91\x90")

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
        stream_in = BytesIO(packed)
        unpacker = Unpacker(stream_in)
        unpacked = next(unpacker.unpack())
        try:
            assert unpacked == unpacked_value
        except AssertionError:
            raise AssertionError("Unpacked value %r is not equal to expected %r" %
                                 (unpacked, unpacked_value))

    def test_empty_map(self):
        assert_packable({}, b"\xA0")

    def test_tiny_map(self):
        d = OrderedDict([(u"A", 1), (u"B", 2)])
        assert_packable(d, b"\xA2\x81A\x01\x81B\x02")

    def test_map_8(self):
        d = OrderedDict([(u"A%s" % i, 1) for i in range(40)])
        b = b"".join(packb(u"A%s" % i, 1) for i in range(40))
        assert_packable(d, b"\xD8\x28" + b)

    def test_map_16(self):
        d = OrderedDict([(u"A%s" % i, 1) for i in range(40000)])
        b = b"".join(packb(u"A%s" % i, 1) for i in range(40000))
        assert_packable(d, b"\xD9\x9C\x40" + b)

    def test_map_32(self):
        d = OrderedDict([(u"A%s" % i, 1) for i in range(80000)])
        b = b"".join(packb(u"A%s" % i, 1) for i in range(80000))
        assert_packable(d, b"\xDA\x00\x01\x38\x80" + b)

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
        stream_in = BytesIO(packed)
        unpacker = Unpacker(stream_in)
        unpacked = next(unpacker.unpack())
        try:
            assert unpacked == unpacked_value
        except AssertionError:
            raise AssertionError("Unpacked value %r is not equal to expected %r" %
                                 (unpacked, unpacked_value))

    def test_illegal_signature(self):
        try:
            assert_packable((b"XXX", ()), b"\xB0XXX")
        except ValueError:
            assert True
        else:
            assert False

    def test_empty_struct(self):
        assert_packable((b"X", ()), b"\xB0X")

    def test_tiny_struct(self):
        assert_packable((b"Z", (u"A", 1)), b"\xB2Z\x81A\x01")
