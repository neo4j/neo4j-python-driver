#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from select import select
from struct import pack as struct_pack
from collections import deque
from time import perf_counter

from neo4j.exceptions import (
    ProtocolError,
    CypherError,
    AuthError,
    ServiceUnavailable,
)

from neo4j.packstream import (
    UnpackableBuffer,
    Unpacker,
)

from neo4j.io import Bolt

from neo4j.api import (
    ServerInfo,
    Version,
)

from neo4j.conf import PoolConfig

from neo4j.addressing import Address

from neo4j.packstream import (
    Packer,
    Unpacker,
)

from neo4j.meta import get_user_agent

from neo4j import Auth

class Bolt3(Bolt):

    protocol_version = Version(3, 0)

    def __init__(self, unresolved_address, sock, *, auth=None, protocol_version=None, **config):
        self.config = PoolConfig.consume(config)
        self.protocol_version = protocol_version
        self.unresolved_address = unresolved_address
        self.socket = sock
        self.server = ServerInfo(Address(sock.getpeername()), protocol_version)
        self.outbox = Outbox()
        self.inbox = Inbox(BufferedSocket(self.socket, 32768), on_error=self._set_defunct)
        self.packer = Packer(self.outbox)
        self.unpacker = Unpacker(self.inbox)
        self.responses = deque()
        self._max_connection_lifetime = self.config.max_age
        self._creation_timestamp = perf_counter()

        # Determine the user agent
        user_agent = self.config.user_agent
        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = get_user_agent()

        # Determine auth details
        if not auth:
            self.auth_dict = {}
        elif isinstance(auth, tuple) and 2 <= len(auth) <= 3:
            self.auth_dict = vars(Auth("basic", *auth))
        else:
            try:
                self.auth_dict = vars(auth)
            except (KeyError, TypeError):
                raise AuthError("Cannot determine auth details from %r" % auth)

        # Check for missing password
        try:
            credentials = self.auth_dict["credentials"]
        except KeyError:
            pass
        else:
            if credentials is None:
                raise AuthError("Password cannot be None")


class Outbox:

    def __init__(self, capacity=8192, max_chunk_size=16384):
        self._max_chunk_size = max_chunk_size
        self._header = 0
        self._start = 2
        self._end = 2
        self._data = bytearray(capacity)

    def max_chunk_size(self):
        return self._max_chunk_size

    def clear(self):
        self._header = 0
        self._start = 2
        self._end = 2
        self._data[0:2] = b"\x00\x00"

    def write(self, b):
        to_write = len(b)
        max_chunk_size = self._max_chunk_size
        pos = 0
        while to_write > 0:
            chunk_size = self._end - self._start
            remaining = max_chunk_size - chunk_size
            if remaining == 0 or remaining < to_write <= max_chunk_size:
                self.chunk()
            else:
                wrote = min(to_write, remaining)
                new_end = self._end + wrote
                self._data[self._end:new_end] = b[pos:pos+wrote]
                self._end = new_end
                pos += wrote
                new_chunk_size = self._end - self._start
                self._data[self._header:(self._header + 2)] = struct_pack(">H", new_chunk_size)
                to_write -= wrote

    def chunk(self):
        self._header = self._end
        self._start = self._header + 2
        self._end = self._start
        self._data[self._header:self._start] = b"\x00\x00"

    def view(self):
        end = self._end
        chunk_size = end - self._start
        if chunk_size == 0:
            return memoryview(self._data[:self._header])
        else:
            return memoryview(self._data[:end])


class BufferedSocket:
    """ Wrapper for a regular socket, with an added a dynamically-resizing
    receive buffer to reduce the number of calls to recv.

    NOTE: not all socket methods are implemented yet
    """

    def __init__(self, socket_, initial_capacity=0):
        self.socket = socket_
        self.buffer = bytearray(initial_capacity)
        self.r_pos = 0
        self.w_pos = 0

    def _fill_buffer(self, min_bytes):
        """ Fill the buffer with at least `min_bytes` bytes, requesting more if
        the buffer has space. Internally, this method attempts to do as little
        allocation as possible and make as few calls to socket.recv as
        possible.
        """
        # First, we need to calculate how much spare space exists between the
        # write cursor and the end of the buffer.
        space_at_end = len(self.buffer) - self.w_pos
        if min_bytes <= space_at_end:
            # If there's at least enough here for the minimum number of bytes
            # we need, then do nothing
            #
            pass
        elif min_bytes <= space_at_end + self.r_pos:
            # If the buffer contains enough space, but it's split between the
            # end of the buffer and recyclable space at the start of the
            # buffer, then recycle that space by pushing the remaining data
            # towards the front.
            #
            # print("Recycling {} bytes".format(self.r_pos))
            size = self.w_pos - self.r_pos
            view = memoryview(self.buffer)
            self.buffer[0:size] = view[self.r_pos:self.w_pos]
            self.r_pos = 0
            self.w_pos = size
        else:
            # Otherwise, there's just not enough space whichever way you shake
            # it. So, rebuild the buffer from scratch, taking the unread data
            # and appending empty space big enough to hold the minimum number
            # of bytes we're looking for.
            #
            # print("Rebuilding buffer from {} bytes ({} used) to "
            #       "{} bytes".format(len(self.buffer),
            #                         self.w_pos - self.r_pos,
            #                         self.w_pos - self.r_pos + min_bytes))
            self.buffer = (self.buffer[self.r_pos:self.w_pos] +
                           bytearray(min_bytes))
            self.w_pos -= self.r_pos
            self.r_pos = 0
        min_end = self.w_pos + min_bytes
        end = len(self.buffer)
        view = memoryview(self.buffer)
        self.socket.setblocking(0)
        while self.w_pos < min_end:
            ready_to_read, _, _ = select([self.socket], [], [])
            subview = view[self.w_pos:end]
            n = self.socket.recv_into(subview, end - self.w_pos)
            if n == 0:
                raise OSError("No data")
            self.w_pos += n

    def recv_into(self, buffer, n_bytes=0, flags=0):
        """ Intercepts a regular socket.recv_into call, taking data from the
        internal buffer, if available. If not enough data exists in the buffer,
        more will be retrieved first.

        Unlike the lower-level call, this method will never return 0, instead
        raising an OSError if no data is returned on the underlying socket.

        :param buffer:
        :param n_bytes:
        :param flags:
        :raises OSError:
        :return:
        """
        available = self.w_pos - self.r_pos
        required = n_bytes - available
        if required > 0:
            self._fill_buffer(required)
        view = memoryview(self.buffer)
        end = self.r_pos + n_bytes
        buffer[:] = view[self.r_pos:end]
        self.r_pos = end
        return n_bytes


class Inbox:

    def __init__(self, s, on_error):
        super(Inbox, self).__init__()
        self.on_error = on_error
        self._messages = self._yield_messages(s)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._messages)

    @classmethod
    def _load_chunks(cls, sock, buffer):
        chunk_size = 0
        while True:
            if chunk_size == 0:
                buffer.receive(sock, 2)
            chunk_size = buffer.pop_u16()
            if chunk_size > 0:
                buffer.receive(sock, chunk_size + 2)
            yield chunk_size

    def _yield_messages(self, sock):
        try:
            buffer = UnpackableBuffer()
            chunk_loader = self._load_chunks(sock, buffer)
            unpacker = Unpacker(buffer)
            details = []
            while True:
                unpacker.reset()
                details[:] = ()
                chunk_size = -1
                while chunk_size != 0:
                    chunk_size = next(chunk_loader)
                summary_signature = None
                summary_metadata = None
                size, signature = unpacker.unpack_structure_header()
                if size > 1:
                    raise ProtocolError("Expected one field")
                if signature == b"\x71":
                    data = unpacker.unpack()
                    details.append(data)
                else:
                    summary_signature = signature
                    summary_metadata = unpacker.unpack_map()
                yield details, summary_signature, summary_metadata
        except OSError as error:
            self.on_error(error)


class Response:
    """ Subscriber object for a full response (zero or
    more detail messages followed by one summary message).
    """

    def __init__(self, connection, **handlers):
        self.connection = connection
        self.handlers = handlers
        self.complete = False

    def on_records(self, records):
        """ Called when one or more RECORD messages have been received.
        """
        handler = self.handlers.get("on_records")
        if callable(handler):
            handler(records)

    def on_success(self, metadata):
        """ Called when a SUCCESS message has been received.
        """
        handler = self.handlers.get("on_success")
        if callable(handler):
            handler(metadata)
        handler = self.handlers.get("on_summary")
        if callable(handler):
            handler()

    def on_failure(self, metadata):
        """ Called when a FAILURE message has been received.
        """
        self.connection.reset()
        handler = self.handlers.get("on_failure")
        if callable(handler):
            handler(metadata)
        handler = self.handlers.get("on_summary")
        if callable(handler):
            handler()
        raise CypherError.hydrate(**metadata)

    def on_ignored(self, metadata=None):
        """ Called when an IGNORED message has been received.
        """
        handler = self.handlers.get("on_ignored")
        if callable(handler):
            handler(metadata)
        handler = self.handlers.get("on_summary")
        if callable(handler):
            handler()


class InitResponse(Response):

    def on_failure(self, metadata):
        code = metadata.get("code")
        message = metadata.get("message", "Connection initialisation failed")
        if code == "Neo.ClientError.Security.Unauthorized":
            raise AuthError(message)
        else:
            raise ServiceUnavailable(message)


class CommitResponse(Response):

    pass
