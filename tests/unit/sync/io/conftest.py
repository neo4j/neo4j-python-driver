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

from neo4j._sync.io._common import (
    Inbox,
    Outbox,
)
from neo4j.auth_management import AuthManagers


class FakeSocket:

    def __init__(self, address, unpacker_cls=None):
        self.address = address
        self.captured = b""
        self.messages = None
        if unpacker_cls is not None:
            self.messages = Inbox(
                self, on_error=print, unpacker_cls=unpacker_cls
            )

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
        assert self.messages
        return self.messages.pop(None)


class FakeSocket2:

    def __init__(self, address=None, on_send=None,
                 packer_cls=None, unpacker_cls=None):
        self.address = address
        self.recv_buffer = bytearray()
        # self.messages = AsyncMessageInbox(self, on_error=print)
        self.on_send = on_send
        self._outbox = self._messages = None
        if packer_cls:
            self._outbox = Outbox(
                self, on_error=print, packer_cls=packer_cls
            )
        if unpacker_cls:
            self._messages = Inbox(
                self, on_error=print, unpacker_cls=unpacker_cls
            )

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

    def kill(self):
        return

    def inject(self, data):
        self.recv_buffer += data

    def pop_message(self):
        assert self._messages
        return self._messages.pop(None)

    def send_message(self, tag, *fields):
        assert self._outbox
        self._outbox.append_message(tag, fields, None)
        self._outbox.flush()


class FakeSocketPair:

    def __init__(self, address, packer_cls=None, unpacker_cls=None):
        self.client = FakeSocket2(
            address, packer_cls=packer_cls, unpacker_cls=unpacker_cls
        )
        self.server = FakeSocket2(
            packer_cls=packer_cls, unpacker_cls=unpacker_cls
        )
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


@pytest.fixture
def static_auth():
    return AuthManagers.static


@pytest.fixture
def none_auth(static_auth):
    return static_auth(None)
