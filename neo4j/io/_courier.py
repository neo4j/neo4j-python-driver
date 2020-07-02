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


from neo4j.packstream import (
    UnpackableBuffer,
    Unpacker,
)

import logging
log = logging.getLogger("neo4j")


class MessageInbox:

    def __init__(self, s, on_error):
        self.on_error = on_error
        self._messages = self._yield_messages(s)

    def _yield_messages(self, sock):
        try:
            buffer = UnpackableBuffer()
            unpacker = Unpacker(buffer)
            chunk_size = 0
            while True:

                while chunk_size == 0:
                    # Determine the chunk size and skip noop
                    buffer.receive(sock, 2)
                    chunk_size = buffer.pop_u16()
                    if chunk_size == 0:
                        log.debug("[#%04X]  S: <NOOP>", sock.getsockname()[1])

                buffer.receive(sock, chunk_size + 2)
                chunk_size = buffer.pop_u16()

                if chunk_size == 0:
                    # chunk_size was the end marker for the message
                    size, tag = unpacker.unpack_structure_header()
                    fields = [unpacker.unpack() for _ in range(size)]
                    yield tag, fields
                    # Reset for new message
                    unpacker.reset()

        except OSError as error:
            self.on_error(error)

    def pop(self):
        return next(self._messages)
