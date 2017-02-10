#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
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


from unittest import TestCase

from neo4j.bolt.io import ChunkedOutputBuffer as PyChunkedOutputBuffer


class ChunkedOutputBufferTestCase(TestCase):
    ChunkedOutputBuffer = PyChunkedOutputBuffer

    def test_should_start_empty(self):
        # Given
        buffer = self.ChunkedOutputBuffer()

        # Then
        assert buffer.view().tobytes() == b""

    def test_should_be_able_to_set_max_chunk_size(self):
        # Given
        buffer = self.ChunkedOutputBuffer(max_chunk_size=4)

        # Then
        assert buffer.max_chunk_size() == 4

    def test_small_data_should_be_directly_appended(self):
        # Given
        buffer = self.ChunkedOutputBuffer()

        # When
        buffer.write(b"hello")

        # Then
        assert buffer.view().tobytes() == b"\x00\x05hello"

    def test_overflow_data_should_use_a_new_chunk(self):
        # Given
        buffer = self.ChunkedOutputBuffer(max_chunk_size=6)

        # When
        buffer.write(b"over")
        buffer.write(b"flow")

        # Then
        assert buffer.view().tobytes() == b"\x00\x04over\x00\x04flow"

    def test_big_data_should_be_split_across_chunks(self):
        # Given
        buffer = self.ChunkedOutputBuffer(max_chunk_size=2)

        # When
        buffer.write(b"octopus")

        # Then
        assert buffer.view().tobytes() == b"\x00\x02oc\x00\x02to\x00\x02pu\x00\x01s"

    def test_clear_should_clear_everything(self):
        # Given
        buffer = self.ChunkedOutputBuffer()

        # When
        buffer.write(b"redacted")
        buffer.clear()

        # Then
        assert buffer.view().tobytes() == b""

    def test_cleared_buffer_should_be_reusable(self):
        # Given
        buffer = self.ChunkedOutputBuffer()

        # When
        buffer.write(b"Windows")
        buffer.clear()
        buffer.write(b"Linux")

        # Then
        assert buffer.view().tobytes() == b"\x00\x05Linux"

    def test_should_be_able_to_force_chunks(self):
        # Given
        buffer = self.ChunkedOutputBuffer()

        # When
        buffer.write(b"hello")
        buffer.chunk()
        buffer.write(b"world")
        buffer.chunk()

        # Then
        assert buffer.view().tobytes() == b"\x00\x05hello\x00\x05world"

    def test_should_be_able_to_force_empty_chunk(self):
        # Given
        buffer = self.ChunkedOutputBuffer()

        # When
        buffer.write(b"hello")
        buffer.chunk()
        buffer.chunk()

        # Then
        assert buffer.view().tobytes() == b"\x00\x05hello\x00\x00"


try:
    from neo4j.bolt._io import ChunkedOutputBuffer as CChunkedOutputBuffer
except ImportError:
    pass
else:
    class CChunkedOutputBufferTestCase(ChunkedOutputBufferTestCase):
        ChunkedOutputBuffer = CChunkedOutputBuffer
