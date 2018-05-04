#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

from neo4j.bolt.io import ChunkedInputBuffer as PyChunkedInputBuffer


class ChunkedInputBufferTestCase(TestCase):
    ChunkedInputBuffer = PyChunkedInputBuffer

    def test_should_start_empty(self):
        # Given
        buffer = self.ChunkedInputBuffer()

        # Then
        assert buffer.view().tobytes() == b""

    def test_should_be_able_to_set_capacity(self):
        # Given
        buffer = self.ChunkedInputBuffer(capacity=10)

        # Then
        assert buffer.capacity() == 10

    def test_should_be_able_to_load_data(self):
        # Given
        buffer = self.ChunkedInputBuffer()

        # When
        buffer.load(b"\x00\x05hello")

        # Then
        assert buffer.view().tobytes() == b"\x00\x05hello"

    def test_should_be_able_to_load_multiple_times(self):
        # Given
        buffer = self.ChunkedInputBuffer()

        # When
        buffer.load(b"\x00\x05hello")
        buffer.load(b"\x00\x05world")

        # Then
        assert buffer.view().tobytes() == b"\x00\x05hello\x00\x05world"

    def test_should_be_able_to_load_after_discard(self):
        # Given
        buffer = self.ChunkedInputBuffer()

        # When
        buffer.load(b"\x00\x05hello\x00\x00")
        buffer.frame_message()
        frame = buffer.frame()

        # Then
        assert frame.panes() == [(2, 7)]

        # When
        buffer.discard_message()
        buffer.load(b"\x00\x07bonjour\x00\x00")
        buffer.frame_message()
        frame = buffer.frame()

        # Then
        assert frame.panes() == [(2, 9)]

    def test_should_auto_extend_on_load(self):
        # Given
        buffer = self.ChunkedInputBuffer(capacity=10)
        buffer.load(b"\x00\x05hello")

        # When
        buffer.load(b"\x00\x07bonjour")

        # Then
        assert buffer.capacity() == 16

    def test_should_start_with_no_frame(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00")
        buffer.load(b"\x00\x07bonjour\x00\x00")

        # Then
        assert buffer.frame() is None

    def test_should_be_able_to_frame_message(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00")
        buffer.load(b"\x00\x07bonjour\x00\x00")

        # When
        framed = buffer.frame_message()

        # Then
        assert framed
        assert buffer.frame().panes() == [(2, 7)]

    def test_should_be_able_to_frame_empty_message(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x00")

        # When
        framed = buffer.frame_message()

        # Then
        assert framed
        assert buffer.frame().panes() == []

    def test_should_not_be_able_to_frame_empty_buffer(self):
        # Given
        buffer = self.ChunkedInputBuffer()

        # When
        framed = buffer.frame_message()

        # Then
        assert not framed

    def test_should_not_be_able_to_frame_partial_message(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello")

        # When
        framed = buffer.frame_message()

        # Then
        assert not framed

    def test_should_be_able_to_discard_message(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00")
        buffer.load(b"\x00\x07bonjour\x00\x00")
        buffer.frame_message()

        # When
        buffer.discard_message()

        # Then
        assert buffer.frame() is None

    def test_should_be_able_to_discard_empty_message(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x00")
        buffer.frame_message()

        # When
        buffer.discard_message()

        # Then
        assert buffer.frame() is None

    def test_discarding_message_should_move_read_pointer(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00")
        buffer.load(b"\x00\x07bonjour\x00\x00")
        buffer.frame_message()
        buffer.discard_message()

        # When
        framed = buffer.frame_message()

        # Then
        assert framed
        assert buffer.frame().panes() == [(2, 9)]

    def test_should_be_able_to_frame_successive_messages_without_discarding(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00")
        buffer.load(b"\x00\x07bonjour\x00\x00")
        buffer.frame_message()

        # When
        framed = buffer.frame_message()

        # Then
        assert framed
        assert buffer.frame().panes() == [(2, 9)]

    def test_discarding_message_should_not_recycle_buffer(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00")
        buffer.load(b"\x00\x07bonjour\x00\x00")
        buffer.frame_message()

        # When
        buffer.discard_message()

        # Then
        assert buffer.view().tobytes() == b"\x00\x05hello\x00\x00\x00\x07bonjour\x00\x00"

    def test_should_not_be_able_to_frame_message_if_empty(self):
        # Given
        buffer = self.ChunkedInputBuffer()

        # Then
        assert not buffer.frame_message()

    def test_should_not_be_able_to_frame_message_if_incomplete(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello")

        # Then
        assert not buffer.frame_message()

    def test_should_be_able_to_frame_message_if_complete(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00")

        # Then
        assert buffer.frame_message()

    def test_should_be_able_to_frame_message_if_complete_with_more(self):
        # Given
        buffer = self.ChunkedInputBuffer()
        buffer.load(b"\x00\x05hello\x00\x00\x00\x05world\x00\x00")

        # Then
        assert buffer.frame_message()


try:
    from neo4j.bolt._io import ChunkedInputBuffer as CChunkedInputBuffer
except ImportError:
    pass
else:
    class CChunkedInputBufferTestCase(ChunkedInputBufferTestCase):
        ChunkedInputBuffer = CChunkedInputBuffer
