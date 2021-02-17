#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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


import pytest

from neo4j.io._courier import MessageInbox


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


@pytest.fixture
def fake_socket():
    return FakeSocket
