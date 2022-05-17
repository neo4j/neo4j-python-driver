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


from contextlib import contextmanager
import socket
from struct import pack as struct_pack

from neo4j.exceptions import (
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
)
from neo4j.packstream import (
    UnpackableBuffer,
    Unpacker,
)

import logging
log = logging.getLogger("neo4j")


class MessageInbox:

    def __init__(self, s, on_error):
        self.on_error = on_error
        self._local_port = s.getsockname()[1]
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
                        log.debug("[#%04X]  S: <NOOP>", self._local_port)

                buffer.receive(sock, chunk_size + 2)
                chunk_size = buffer.pop_u16()

                if chunk_size == 0:
                    # chunk_size was the end marker for the message
                    size, tag = unpacker.unpack_structure_header()
                    fields = [unpacker.unpack() for _ in range(size)]
                    yield tag, fields
                    # Reset for new message
                    unpacker.reset()

        except (OSError, socket.timeout) as error:
            self.on_error(error)

    def pop(self):
        return next(self._messages)


class Inbox(MessageInbox):

    def __next__(self):
        tag, fields = self.pop()
        if tag == b"\x71":
            return fields, None, None
        elif fields:
            return [], tag, fields[0]
        else:
            return [], tag, None


class Outbox:

    def __init__(self, max_chunk_size=16384):
        self._max_chunk_size = max_chunk_size
        self._chunked_data = bytearray()
        self._raw_data = bytearray()
        self.write = self._raw_data.extend
        self._tmp_buffering = 0

    def max_chunk_size(self):
        return self._max_chunk_size

    def clear(self):
        if self._tmp_buffering:
            raise RuntimeError("Cannot clear while buffering")
        self._chunked_data = bytearray()
        self._raw_data.clear()

    def _chunk_data(self):
        data_len = len(self._raw_data)
        num_full_chunks, chunk_rest = divmod(
            data_len, self._max_chunk_size
        )
        num_chunks = num_full_chunks + bool(chunk_rest)

        data_view = memoryview(self._raw_data)
        header_start = len(self._chunked_data)
        data_start = header_start + 2
        raw_data_start = 0
        for i in range(num_chunks):
            chunk_size = min(data_len - raw_data_start,
                             self._max_chunk_size)
            self._chunked_data[header_start:data_start] = struct_pack(
                ">H", chunk_size
            )
            self._chunked_data[data_start:(data_start + chunk_size)] = \
                data_view[raw_data_start:(raw_data_start + chunk_size)]
            header_start += chunk_size + 2
            data_start = header_start + 2
            raw_data_start += chunk_size
        del data_view
        self._raw_data.clear()

    def wrap_message(self):
        if self._tmp_buffering:
            raise RuntimeError("Cannot wrap message while buffering")
        self._chunk_data()
        self._chunked_data += b"\x00\x00"

    def view(self):
        if self._tmp_buffering:
            raise RuntimeError("Cannot view while buffering")
        self._chunk_data()
        return memoryview(self._chunked_data)

    @contextmanager
    def tmp_buffer(self):
        self._tmp_buffering += 1
        old_len = len(self._raw_data)
        try:
            yield
        except Exception:
            del self._raw_data[old_len:]
            raise
        finally:
            self._tmp_buffering -= 1


class ConnectionErrorHandler:
    """
    Wrapper class for handling connection errors.

    The class will wrap each method to invoke a callback if the method raises
    Neo4jError, SessionExpired, or ServiceUnavailable.
    The error will be re-raised after the callback.
    """

    def __init__(self, connection, on_error):
        """
        :param connection the connection object to warp
        :type connection Bolt
        :param on_error the function to be called when a method of
            connection raises of of the caught errors.
        :type on_error callable
        """
        self.__connection = connection
        self.__on_error = on_error

    def __getattr__(self, name):
        connection_attr = getattr(self.__connection, name)
        if not callable(connection_attr):
            return connection_attr

        def outer(func):
            def inner(*args, **kwargs):
                try:
                    func(*args, **kwargs)
                except (Neo4jError, ServiceUnavailable, SessionExpired) as exc:
                    self.__on_error(exc)
                    raise
            return inner

        return outer(connection_attr)

    def __setattr__(self, name, value):
        if name.startswith("_" + self.__class__.__name__ + "__"):
            super().__setattr__(name, value)
        else:
            setattr(self.__connection, name, value)


class Response:
    """ Subscriber object for a full response (zero or
    more detail messages followed by one summary message).
    """

    def __init__(self, connection, message, **handlers):
        self.connection = connection
        self.handlers = handlers
        self.message = message
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

        if not metadata.get("has_more"):
            handler = self.handlers.get("on_summary")
            if callable(handler):
                handler()

    def on_failure(self, metadata):
        """ Called when a FAILURE message has been received.
        """
        try:
            self.connection.reset()
        except (SessionExpired, ServiceUnavailable):
            pass
        handler = self.handlers.get("on_failure")
        if callable(handler):
            handler(metadata)
        handler = self.handlers.get("on_summary")
        if callable(handler):
            handler()
        raise Neo4jError.hydrate(**metadata)

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
        if code == "Neo.ClientError.Security.Unauthorized":
            raise Neo4jError.hydrate(**metadata)
        else:
            raise ServiceUnavailable(
                metadata.get("message", "Connection initialisation failed")
            )


class CommitResponse(Response):

    pass
