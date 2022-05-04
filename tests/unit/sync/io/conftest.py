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


from io import BytesIO
from struct import (
    pack as struct_pack,
    unpack as struct_unpack,
)

import pytest

from neo4j._sync.io._common import MessageInbox
from neo4j.packstream import (
    Packer,
    UnpackableBuffer,
    Unpacker,
)


class FakeSocket:

    def __init__(self, address):
        self.address = address
        self.captured = b""
        self.messages = MessageInbox(self, on_error=print)

    def getsockname(self):
        return "127.0.0.1", 0xFFFF

    def getpeername(self):
        return self.address

    def recv_into(self, buffer, nbytes):
        data = self.captured[:nbytes]
        actual = len(data)
        buffer[:actual] = data
        self.captured = self.captured[actual:]
        return actual

    def sendall(self, data):
        self.captured += data

    def close(self):
        return

    def pop_message(self):
        return self.messages.pop()


class FakeSocket2:

    def __init__(self, address=None, on_send=None):
        self.address = address
        self.recv_buffer = bytearray()
        self._messages = MessageInbox(self, on_error=print)
        self.on_send = on_send

    def getsockname(self):
        return "127.0.0.1", 0xFFFF

    def getpeername(self):
        return self.address

    def recv_into(self, buffer, nbytes):
        data = self.recv_buffer[:nbytes]
        actual = len(data)
        buffer[:actual] = data
        self.recv_buffer = self.recv_buffer[actual:]
        return actual

    def sendall(self, data):
        if callable(self.on_send):
            self.on_send(data)

    def close(self):
        return

    def inject(self, data):
        self.recv_buffer += data

    def _pop_chunk(self):
        chunk_size, = struct_unpack(">H", self.recv_buffer[:2])
        print("CHUNK SIZE %r" % chunk_size)
        end = 2 + chunk_size
        chunk_data, self.recv_buffer = self.recv_buffer[2:end], self.recv_buffer[end:]
        return chunk_data

    def pop_message(self):
        data = bytearray()
        while True:
            chunk = self._pop_chunk()
            print("CHUNK %r" % chunk)
            if chunk:
                data.extend(chunk)
            elif data:
                break       # end of message
            else:
                continue    # NOOP
        header = data[0]
        n_fields = header % 0x10
        tag = data[1]
        buffer = UnpackableBuffer(data[2:])
        unpacker = Unpacker(buffer)
        fields = [unpacker.unpack() for _ in range(n_fields)]
        return tag, fields

    def send_message(self, tag, *fields):
        data = self.encode_message(tag, *fields)
        self.sendall(struct_pack(">H", len(data)) + data + b"\x00\x00")

    @classmethod
    def encode_message(cls, tag, *fields):
        b = BytesIO()
        packer = Packer(b)
        for field in fields:
            packer.pack(field)
        return bytearray([0xB0 + len(fields), tag]) + b.getvalue()


class FakeSocketPair:

    def __init__(self, address):
        self.client = FakeSocket2(address)
        self.server = FakeSocket2()
        self.client.on_send = self.server.inject
        self.server.on_send = self.client.inject


@pytest.fixture
def fake_socket():
    return FakeSocket


@pytest.fixture
def fake_socket_2():
    return FakeSocket2


@pytest.fixture
def fake_socket_pair():
    return FakeSocketPair
