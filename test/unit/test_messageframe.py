#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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


from unittest import TestCase

from neo4j.util import import_best as _import_best

MessageFrame = _import_best("neo4j.bolt._io", "neo4j.bolt.io").MessageFrame


class MessageFrameTestCase(TestCase):

    def test_should_be_able_to_read_int(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x00"), [(2, 5)])

        # When
        values = [frame.read_int() for _ in range(4)]

        # Then
        assert values == [65, 66, 67, -1]

    def test_should_be_able_to_read_int_across_chunks(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x03DEF\x00\x00"), [(2, 5), (7, 10)])

        # When
        values = [frame.read_int() for _ in range(7)]

        # Then
        assert values == [65, 66, 67, 68, 69, 70, -1]

    def test_should_be_able_to_read_one(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x00"), [(2, 5)])

        # When
        value = frame.read(1)

        # Then
        assert bytearray(value) == bytearray(b"A")
        assert isinstance(value, memoryview)

    def test_should_be_able_to_read_some(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x00"), [(2, 5)])

        # When
        value = frame.read(2)

        # Then
        assert bytearray(value) == bytearray(b"AB")
        assert isinstance(value, memoryview)

    def test_should_be_able_to_read_all(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x00"), [(2, 5)])

        # When
        value = frame.read(3)

        # Then
        assert bytearray(value) == bytearray(b"ABC")
        assert isinstance(value, memoryview)

    def test_should_read_empty_if_exhausted(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x00"), [(2, 5)])
        frame.read(3)

        # When
        value = frame.read(3)

        # Then
        assert bytearray(value) == bytearray(b"")

    def test_should_be_able_to_read_beyond(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x00"), [(2, 5)])

        # When
        value = frame.read(4)

        # Then
        assert bytearray(value) == bytearray(b"ABC")

    def test_should_be_able_to_read_across_chunks(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x03DEF\x00\x00"), [(2, 5), (7, 10)])

        # When
        value = frame.read(4)

        # Then
        assert bytearray(value) == bytearray(b"ABCD")

    def test_should_be_able_to_read_all_across_chunks(self):
        # Given
        frame = MessageFrame(memoryview(b"\x00\x03ABC\x00\x03DEF\x00\x00"), [(2, 5), (7, 10)])

        # When
        value = frame.read(6)

        # Then
        assert bytearray(value) == bytearray(b"ABCDEF")
