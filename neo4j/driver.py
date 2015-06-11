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

"""
This module contains the main NDP driver components as well as several
helper and exception classes. The main entry point is the `connect`
function which returns an instance of the ConnectionV1 class that can
be used for running Cypher statements.
"""


from io import BytesIO
import logging
from select import select
from socket import create_connection, SHUT_RDWR
import struct
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

# Serialisation and deserialisation routines
from neo4j.packstream import Packer, Unpacker
# Hydration function for turning structures into their actual types
from neo4j.typesystem import hydrated


DEFAULT_PORT = 7687

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

# Set up logger
log = logging.getLogger("neo4j")
log_debug = log.debug
log_info = log.info
log_warning = log.warning


class ProtocolError(Exception):

    pass


class CypherError(Exception):

    code = None
    message = None

    def __init__(self, data):
        super(CypherError, self).__init__(data.get("message"))
        for key, value in data.items():
            if not key.startswith("_"):
                setattr(self, key, value)


class ChunkWriter(object):
    """ Writer for chunked data.
    """

    max_chunk_size = 65535

    def __init__(self):
        self.raw = BytesIO()
        self.output_buffer = []
        self.output_size = 0

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

    def flush(self, zero_chunk=False):
        """ Flush everything written since the last chunk to the
        stream, followed by a zero-chunk if required.
        """
        output_buffer = self.output_buffer
        if output_buffer:
            lines = [struct.pack(">H", self.output_size)] + output_buffer
        else:
            lines = []
        if zero_chunk:
            lines.append(b"\x00\x00")
        if lines:
            self.raw.writelines(lines)
            self.raw.flush()
            del output_buffer[:]
            self.output_size = 0

    def to_bytes(self):
        """ Extract the written data as bytes.
        """
        return self.raw.getvalue()

    def close(self, zero_chunk=False):
        """ Close the stream.
        """
        self.flush(zero_chunk=zero_chunk)
        self.raw.close()


class SessionV1(object):
    """ Server connection through which all protocol messages
    are sent and received. This class is designed for protocol
    version 1.
    """

    def __init__(self, s):
        self.socket = s
        if __debug__: log_info("NDPv1 connection established!")
        self.init("ExampleDriver/1.0")

    def _send_messages(self, *messages):
        """ Send one or more messages to the server.
        """
        raw = ChunkWriter()
        packer = Packer(raw)
        pack = packer.pack
        flush = raw.flush

        for message in messages:
            pack(message)
            flush(zero_chunk=True)

        data = raw.to_bytes()
        if __debug__: log_debug("Sending request data: %r", data)
        self.socket.sendall(data)

        raw.close()

    def _recv(self, size):
        """ Receive a required number of bytes from the network.
        """
        socket = self.socket
        recv = socket.recv

        # Try to read the required amount of data
        data = recv(size)
        size -= len(data)

        # If more is needed, keep reading until all data has been received
        while size:
            # Check for available network data
            ready_to_read, _, _ = select((socket,), (), (), 0)
            while not ready_to_read:
                ready_to_read, _, _ = select((socket,), (), (), 0)

            # Read up to the required amount remaining
            b = recv(size)
            size -= len(b)
            data += b

        return data

    def _recv_message(self):
        """ Receive exactly one message from the server.
        """
        raw = BytesIO()
        unpack = Unpacker(raw).unpack

        recv = self._recv
        write = raw.write

        # Receive chunks of data until chunk_size == 0
        more = True
        while more:
            # Receive chunk header to establish size of chunk that follows
            chunk_header = recv(2)
            if __debug__: log_debug("Received chunk header data: %r", chunk_header)
            chunk_size, = struct.unpack_from(">H", chunk_header)

            # Receive chunk data
            if chunk_size > 0:
                chunk_data = recv(chunk_size)
                if __debug__: log_debug("Received chunk data: %r", chunk_data)
                write(chunk_data)
            else:
                more = False

        # Unpack the message structure from the raw byte stream
        # (there should be only one)
        raw.seek(0)
        signature, fields = next(unpack())
        raw.close()

        # Acknowledge any failures immediately
        if signature == FAILURE:
            self.ack_failure()

        return signature, fields

    def init(self, user_agent):
        """ Initialise a connection with a user agent string.
        """

        # Ensure the user agent is a Unicode value
        if isinstance(user_agent, bytes):
            user_agent = user_agent.decode("UTF-8")

        if __debug__: log_info("Initialising connection")
        self._send_messages((INIT, (user_agent,)))

        signature, (data,) = self._recv_message()
        if signature == SUCCESS:
            if __debug__: log_info("Initialisation successful")
        else:
            raise ProtocolError("INIT was unsuccessful: %r" % data)

    def run(self, statement, parameters=None):
        """ Run a parameterised Cypher statement.
        """
        recv_message = self._recv_message

        # Ensure the statement is a Unicode value
        if isinstance(statement, bytes):
            statement = statement.decode("UTF-8")

        parameters = dict(parameters or {})
        if __debug__: log_info("Running statement %r with parameters %r", statement, parameters)
        self._send_messages((RUN, (statement, parameters)),
                            (PULL_ALL, ()))

        signature, (data,) = recv_message()
        if signature == SUCCESS:
            fields = tuple(data["fields"])
            if __debug__: log_info("Statement ran successfully with field list %r", fields)
        else:
            raise CypherError(data)

        records = []
        append = records.append
        more = True
        while more:
            signature, (data,) = recv_message()
            if signature == RECORD:
                if __debug__: log_info("Record received with value list %r", data)
                append(tuple(map(hydrated, data)))
            elif signature == SUCCESS:
                if __debug__: log_info("All records successfully received: %r", data)
                more = False
            else:
                raise CypherError(data)

        return fields, records

    def ack_failure(self):
        """ Send an acknowledgement for a previous failure.
        """
        if __debug__: log_info("Acknowledging failure")
        self._send_messages((ACK_FAILURE, ()))

        # Skip any ignored responses
        signature, _ = self._recv_message()
        while signature == IGNORED:
            signature, _ = self._recv_message()

        # Check the acknowledgement was successful
        if signature == SUCCESS:
            if __debug__: log_info("Acknowledgement successful")
        else:
            raise ProtocolError("ACK_FAILURE was unsuccessful")

    def close(self):
        """ Shut down and close the connection.
        """
        if __debug__: log_info("Shutting down and closing connection")
        socket = self.socket
        socket.shutdown(SHUT_RDWR)
        socket.close()


class Driver(object):

    def __init__(self, url, **config):
        parsed = urlparse(url)
        if parsed.scheme == "graph":
            self.host = parsed.hostname
            self.port = parsed.port
        else:
            raise ProtocolError("Unsupported URL scheme: %s" % parsed.scheme)
        self.config = config

    def session(self):
        """ Connect and perform a handshake in order to return a valid
        Connection object, assuming a protocol version can be agreed.
        """

        # Establish a connection to the host and port specified
        host = self.host
        port = self.port or DEFAULT_PORT
        if __debug__: log_info("Creating connection to %s on port %d" % (host, port))
        s = create_connection((host, port))

        # Send details of the protocol versions supported
        supported_versions = [1, 0, 0, 0]
        if __debug__: log_info("Supported protocol versions are: %r", supported_versions)
        data = b"".join(struct.pack(">I", version) for version in supported_versions)
        if __debug__: log_debug("Sending handshake data: %r", data)
        s.sendall(data)

        # Handle the handshake response
        data = s.recv(4)
        if __debug__: log_debug("Received handshake data: %r", data)
        agreed_version, = struct.unpack(">I", data)
        if agreed_version == 0:
            if __debug__: log_warning("Closing connection as no protocol version could be agreed")
            s.shutdown(SHUT_RDWR)
            s.close()
        else:
            if __debug__: log_info("Protocol version %d agreed", agreed_version)
            return SessionV1(s)


def driver(url, **config):
    return Driver(url, **config)
