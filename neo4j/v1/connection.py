#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from __future__ import division

from collections import deque
from io import BytesIO
import logging
from os import environ
from select import select
from socket import create_connection, SHUT_RDWR
from struct import pack as struct_pack, unpack as struct_unpack, unpack_from as struct_unpack_from

from ..meta import version
from .compat import hex2, secure_socket
from .exceptions import ProtocolError
from .packstream import Packer, Unpacker


DEFAULT_PORT = 7687
DEFAULT_USER_AGENT = "neo4j-python/%s" % version

MAGIC_PREAMBLE = 0x6060B017

# Signature bytes for each message type
INIT = b"\x01"             # 0000 0001 // INIT <user_agent>
ACK_FAILURE = b"\x0F"      # 0000 1111 // ACK_FAILURE
RUN = b"\x10"              # 0001 0000 // RUN <statement> <parameters>
DISCARD_ALL = b"\x2F"      # 0010 1111 // DISCARD *
PULL_ALL = b"\x3F"         # 0011 1111 // PULL *
SUCCESS = b"\x70"          # 0111 0000 // SUCCESS <metadata>
RECORD = b"\x71"           # 0111 0001 // RECORD <value>
IGNORED = b"\x7E"          # 0111 1110 // IGNORED <metadata>
FAILURE = b"\x7F"          # 0111 1111 // FAILURE <metadata>

DETAIL = {RECORD}
SUMMARY = {SUCCESS, IGNORED, FAILURE}

message_names = {
    INIT: "INIT",
    ACK_FAILURE: "ACK_FAILURE",
    RUN: "RUN",
    DISCARD_ALL: "DISCARD_ALL",
    PULL_ALL: "PULL_ALL",
    SUCCESS: "SUCCESS",
    RECORD: "RECORD",
    IGNORED: "IGNORED",
    FAILURE: "FAILURE",
}

# Set up logger
log = logging.getLogger("neo4j")
log_debug = log.debug
log_info = log.info
log_warning = log.warning
log_error = log.error


class ChunkChannel(object):
    """ Reader/writer for chunked data.

    .. note:: logs at DEBUG level
    """

    max_chunk_size = 65535

    def __init__(self, sock):
        self.socket = sock
        self.raw = BytesIO()
        self.output_buffer = []
        self.output_size = 0
        self._recv_buffer = b""

    def write(self, b):
        """ Write some bytes, splitting into chunks if necessary.
        """
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

    def flush(self, end_of_message=False):
        """ Flush everything written since the last chunk to the
        stream, followed by a zero-chunk if required.
        """
        output_buffer = self.output_buffer
        if output_buffer:
            lines = [struct_pack(">H", self.output_size)] + output_buffer
        else:
            lines = []
        if end_of_message:
            lines.append(b"\x00\x00")
        if lines:
            self.raw.writelines(lines)
            self.raw.flush()
            del output_buffer[:]
            self.output_size = 0

    def send(self):
        """ Send all queued messages to the server.
        """
        data = self.raw.getvalue()
        if __debug__:
            log_debug("C: %s", ":".join(map(hex2, data)))
        self.socket.sendall(data)

        self.raw.seek(self.raw.truncate(0))

    def _recv(self, size):
        # If data is needed, keep reading until all bytes have been received
        remaining = size - len(self._recv_buffer)
        ready_to_read = None
        while remaining > 0:
            # Read up to the required amount remaining
            b = self.socket.recv(8192)
            if b:
                if __debug__: log_debug("S: %s", ":".join(map(hex2, b)))
            else:
                if ready_to_read is not None:
                    raise ProtocolError("Server closed connection")
            remaining -= len(b)
            self._recv_buffer += b

            # If more is required, wait for available network data
            if remaining > 0:
                ready_to_read, _, _ = select((self.socket,), (), (), 0)
                while not ready_to_read:
                    ready_to_read, _, _ = select((self.socket,), (), (), 0)

        # Split off the amount of data required and keep the rest in the buffer
        data, self._recv_buffer = self._recv_buffer[:size], self._recv_buffer[size:]
        return data

    def chunk_reader(self):
        chunk_size = -1
        while chunk_size != 0:
            chunk_header = self._recv(2)
            chunk_size, = struct_unpack_from(">H", chunk_header)
            if chunk_size > 0:
                data = self._recv(chunk_size)
                yield data

    def close(self):
        """ Shut down and close the connection.
        """
        if __debug__: log_info("~~ [CLOSE]")
        socket = self.socket
        socket.shutdown(SHUT_RDWR)
        socket.close()


class Response(object):
    """ Subscriber object for a full response (zero or
    more detail messages followed by one summary message).
    """

    def __init__(self, connection):
        self.connection = connection
        self.complete = False

    def on_record(self, values):
        pass

    def on_success(self, metadata):
        pass

    def on_failure(self, metadata):
        pass

    def on_ignored(self, metadata=None):
        pass


class AckFailureResponse(Response):

    def on_failure(self, metadata):
        raise ProtocolError("Could not acknowledge failure")


class Connection(object):
    """ Server connection through which all protocol messages
    are sent and received. This class is designed for protocol
    version 1.

    .. note:: logs at INFO level
    """

    def __init__(self, sock, **config):
        self.channel = ChunkChannel(sock)
        self.packer = Packer(self.channel)
        self.responses = deque()

        # Determine the user agent and ensure it is a Unicode value
        user_agent = config.get("user_agent", DEFAULT_USER_AGENT)
        if isinstance(user_agent, bytes):
            user_agent = user_agent.decode("UTF-8")

        def on_failure(metadata):
            raise ProtocolError("Initialisation failed")

        response = Response(self)
        response.on_failure = on_failure

        self.append(INIT, (user_agent,), response=response)
        self.send()
        while not response.complete:
            self.fetch_next()

    def append(self, signature, fields=(), response=None):
        """ Add a message to the outgoing queue.
        """
        if __debug__:
            log_info("C: %s %s", message_names[signature], " ".join(map(repr, fields)))

        self.packer.pack_struct_header(len(fields), signature)
        for field in fields:
            self.packer.pack(field)
        self.channel.flush(end_of_message=True)
        self.responses.append(response)

    def send(self):
        """ Send all queued messages to the server.
        """
        self.channel.send()

    def fetch_next(self):
        """ Receive exactly one message from the server.
        """
        raw = BytesIO()
        unpack = Unpacker(raw).unpack
        raw.writelines(self.channel.chunk_reader())

        # Unpack from the raw byte stream and call the relevant message handler(s)
        raw.seek(0)
        response = self.responses[0]
        for signature, fields in unpack():
            if __debug__:
                log_info("S: %s %s", message_names[signature], " ".join(map(repr, fields)))
            handler_name = "on_%s" % message_names[signature].lower()
            try:
                handler = getattr(response, handler_name)
            except AttributeError:
                pass
            else:
                handler(*fields)
            if signature in SUMMARY:
                response.complete = True
                self.responses.popleft()
            if signature == FAILURE:
                self.append(ACK_FAILURE, response=AckFailureResponse(self))
        raw.close()

    def close(self):
        """ Shut down and close the connection.
        """
        self.channel.close()


def connect(host, port=None, **config):
    """ Connect and perform a handshake and return a valid Connection object, assuming
    a protocol version can be agreed.
    """

    # Establish a connection to the host and port specified
    port = port or DEFAULT_PORT
    if __debug__: log_info("~~ [CONNECT] %s %d", host, port)
    s = create_connection((host, port))

    # Secure the connection if so requested
    try:
        secure = environ["NEO4J_SECURE"]
    except KeyError:
        secure = config.get("secure", False)
    if secure:
        if __debug__: log_info("~~ [SECURE] %s", host)
        s = secure_socket(s, host)

    # Send details of the protocol versions supported
    supported_versions = [1, 0, 0, 0]
    handshake = [MAGIC_PREAMBLE] + supported_versions
    if __debug__: log_info("C: [HANDSHAKE] 0x%X %r", MAGIC_PREAMBLE, supported_versions)
    data = b"".join(struct_pack(">I", num) for num in handshake)
    if __debug__: log_debug("C: %s", ":".join(map(hex2, data)))
    s.sendall(data)

    # Handle the handshake response
    ready_to_read, _, _ = select((s,), (), (), 0)
    while not ready_to_read:
        ready_to_read, _, _ = select((s,), (), (), 0)
    data = s.recv(4)
    data_size = len(data)
    if data_size == 0:
        # If no data is returned after a successful select
        # response, the server has closed the connection
        log_error("S: [CLOSE]")
        raise ProtocolError("Server closed connection without responding to handshake")
    if data_size == 4:
        if __debug__: log_debug("S: %s", ":".join(map(hex2, data)))
    else:
        # Some other garbled data has been received
        log_error("S: @*#!")
        raise ProtocolError("Expected four byte handshake response, received %r instead" % data)
    agreed_version, = struct_unpack(">I", data)
    if __debug__: log_info("S: [HANDSHAKE] %d", agreed_version)
    if agreed_version == 0:
        if __debug__: log_info("~~ [CLOSE]")
        s.shutdown(SHUT_RDWR)
        s.close()
    else:
        return Connection(s, **config)
