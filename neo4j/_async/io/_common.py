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


import asyncio
import logging
import socket
from struct import pack as struct_pack

from ..._async_compat.util import AsyncUtil
from ...exceptions import (
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
    UnsupportedServerProduct,
)
from ...packstream import (
    UnpackableBuffer,
    Unpacker,
)


log = logging.getLogger("neo4j")


class AsyncMessageInbox:

    def __init__(self, s, on_error):
        self.on_error = on_error
        self._messages = self._yield_messages(s)

    async def _yield_messages(self, sock):
        try:
            buffer = UnpackableBuffer()
            unpacker = Unpacker(buffer)
            chunk_size = 0
            while True:

                while chunk_size == 0:
                    # Determine the chunk size and skip noop
                    await receive_into_buffer(sock, buffer, 2)
                    chunk_size = buffer.pop_u16()
                    if chunk_size == 0:
                        log.debug("[#%04X]  S: <NOOP>", sock.getsockname()[1])

                await receive_into_buffer(sock, buffer, chunk_size + 2)
                chunk_size = buffer.pop_u16()

                if chunk_size == 0:
                    # chunk_size was the end marker for the message
                    size, tag = unpacker.unpack_structure_header()
                    fields = [unpacker.unpack() for _ in range(size)]
                    yield tag, fields
                    # Reset for new message
                    unpacker.reset()

        except (OSError, socket.timeout) as error:
            await AsyncUtil.callback(self.on_error, error)

    async def pop(self):
        return await AsyncUtil.next(self._messages)


class AsyncInbox(AsyncMessageInbox):

    async def __anext__(self):
        tag, fields = await self.pop()
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

    def max_chunk_size(self):
        return self._max_chunk_size

    def clear(self):
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
        self._chunk_data()
        self._chunked_data += b"\x00\x00"

    def view(self):
        self._chunk_data()
        return memoryview(self._chunked_data)


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
                    assert not asyncio.iscoroutinefunction(self.__on_error)
                    self.__on_error(exc)
                    raise
            return inner

        def outer_async(coroutine_func):
            async def inner(*args, **kwargs):
                try:
                    await coroutine_func(*args, **kwargs)
                except (Neo4jError, ServiceUnavailable, SessionExpired) as exc:
                    await AsyncUtil.callback(self.__on_error, exc)
                    raise
            return inner

        if asyncio.iscoroutinefunction(connection_attr):
            return outer_async(connection_attr)
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

    async def on_records(self, records):
        """ Called when one or more RECORD messages have been received.
        """
        handler = self.handlers.get("on_records")
        await AsyncUtil.callback(handler, records)

    async def on_success(self, metadata):
        """ Called when a SUCCESS message has been received.
        """
        handler = self.handlers.get("on_success")
        await AsyncUtil.callback(handler, metadata)

        if not metadata.get("has_more"):
            handler = self.handlers.get("on_summary")
            await AsyncUtil.callback(handler)

    async def on_failure(self, metadata):
        """ Called when a FAILURE message has been received.
        """
        try:
            self.connection.reset()
        except (SessionExpired, ServiceUnavailable):
            pass
        handler = self.handlers.get("on_failure")
        await AsyncUtil.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        await AsyncUtil.callback(handler)
        raise Neo4jError.hydrate(**metadata)

    async def on_ignored(self, metadata=None):
        """ Called when an IGNORED message has been received.
        """
        handler = self.handlers.get("on_ignored")
        await AsyncUtil.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        await AsyncUtil.callback(handler)


class InitResponse(Response):

    async def on_failure(self, metadata):
        code = metadata.get("code")
        if code == "Neo.ClientError.Security.Unauthorized":
            raise Neo4jError.hydrate(**metadata)
        else:
            raise ServiceUnavailable(
                metadata.get("message", "Connection initialisation failed")
            )


class CommitResponse(Response):

    pass


def check_supported_server_product(agent):
    """ Checks that a server product is supported by the driver by
    looking at the server agent string.

    :param agent: server agent string to check for validity
    :raises UnsupportedServerProduct: if the product is not supported
    """
    if not agent.startswith("Neo4j/"):
        raise UnsupportedServerProduct(agent)


async def receive_into_buffer(sock, buffer, n_bytes):
    end = buffer.used + n_bytes
    if end > len(buffer.data):
        buffer.data += bytearray(end - len(buffer.data))
    view = memoryview(buffer.data)
    while buffer.used < end:
        n = await sock.recv_into(view[buffer.used:end], end - buffer.used)
        if n == 0:
            raise OSError("No data")
        buffer.used += n
