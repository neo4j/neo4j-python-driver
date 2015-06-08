#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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
