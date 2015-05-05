#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from io import BytesIO
from struct import pack


class ChunkedIO(BytesIO):
    max_chunk_size = 65535

    def __init__(self, *args, **kwargs):
        super(ChunkedIO, self).__init__(*args, **kwargs)
        self.input_buffer = []
        self.input_size = 0
        self.output_buffer = []
        self.output_size = 0

    def write(self, b):
        max_chunk_size = self.max_chunk_size
        output_buffer = self.output_buffer
        while b:
            size = len(b)
            future_size = self.output_size + size
            if future_size >= max_chunk_size:
                end = max_chunk_size - self.output_size
                output_buffer.append(b[:end])
                self.output_size = max_chunk_size
                b = b[end:]
                self.flush()
            else:
                output_buffer.append(b)
                self.output_size = future_size
                b = b""

    def flush(self, zero_chunk=False):
        output_buffer = self.output_buffer
        if output_buffer:
            lines = [pack(">H", self.output_size)] + output_buffer
        else:
            lines = []
        if zero_chunk:
            lines.append(b"\x00\x00")
        if lines:
            BytesIO.writelines(self, lines)
            BytesIO.flush(self)
            del output_buffer[:]
            self.output_size = 0

    def close(self, zero_chunk=False):
        self.flush(zero_chunk=zero_chunk)
        BytesIO.close(self)


if __name__ == "__main__":
    chunked_io = ChunkedIO()
    chunked_io.write(b"hello world")
    chunked_io.flush()
    chunked_io.flush(zero_chunk=True)
    print(chunked_io.getvalue())
