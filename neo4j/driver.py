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
This module contains the main GAP driver components as well as several
helper and exception classes. The main entry point is the `GraphDatabase`
class which can be used to obtain `Driver` instances that are used for
managing sessions.
"""


from __future__ import division

from collections import namedtuple
from io import BytesIO
import logging
from os import environ
from select import select
from socket import create_connection, SHUT_RDWR
from struct import pack as struct_pack, unpack as struct_unpack, unpack_from as struct_unpack_from

from .compat import integer, perf_counter, secure_socket, string, urlparse

# Serialisation and deserialisation routines plus the structure data type
from .packstream import Packer, Unpacker, Structure

# Hydration function for turning structures into their actual types
from .typesystem import hydrated


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
log_error = log.error


Latency = namedtuple("Latency", ["overall", "network", "wait"])


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


class Record(object):
    """ Record object for storing result values along with field names.
    """

    def __init__(self, fields, values):
        self.__fields__ = fields
        self.__values__ = values

    def __repr__(self):
        values = self.__values__
        s = []
        for i, field in enumerate(self.__fields__):
            value = values[i]
            if isinstance(value, tuple):
                signature, _ = value
                if signature == b"N":
                    s.append("%s=<Node>" % (field,))
                elif signature == b"R":
                    s.append("%s=<Relationship>" % (field,))
                else:
                    s.append("%s=<?>" % (field,))
            else:
                s.append("%s=%r" % (field, value))
        return "<Record %s>" % " ".join(s)

    def __eq__(self, other):
        try:
            return vars(self) == vars(other)
        except TypeError:
            return tuple(self) == tuple(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return self.__fields__.__len__()

    def __getitem__(self, item):
        if isinstance(item, string):
            return getattr(self, item)
        elif isinstance(item, integer):
            return getattr(self, self.__fields__[item])
        else:
            raise LookupError(item)

    def __getattr__(self, item):
        try:
            i = self.__fields__.index(item)
        except ValueError:
            raise AttributeError("No field %r" % item)
        else:
            value = self.__values__[i]
            if isinstance(value, tuple):
                value = self.__values__[i] = hydrated(value)
            return value


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
            lines = [struct_pack(">H", self.output_size)] + output_buffer
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

    def __init__(self, s, **config):
        self.socket = s
        self._recv_buffer = b""
        self.init("neo4j-python/0.0")
        if config.get("bench"):
            self._bench = []
        else:
            self._bench = None

    def _send_messages(self, *messages):
        """ Send one or more messages to the server.
        """
        raw = ChunkWriter()
        packer = Packer(raw)
        pack_struct_header = packer.pack_struct_header
        pack = packer.pack
        flush = raw.flush

        for signature, fields in messages:
            pack_struct_header(len(fields), signature)
            for field in fields:
                pack(field)
            flush(zero_chunk=True)

        data = raw.to_bytes()
        if __debug__: log_debug("C: %r", data)
        t1 = perf_counter()
        self.socket.sendall(data)
        t2 = perf_counter()

        raw.close()

        return t1, t2

    def _recv_message(self, times=None):
        """ Receive exactly one message from the server.
        """
        raw = BytesIO()
        unpack = Unpacker(raw).unpack

        socket = self.socket
        recv = socket.recv
        write = raw.write

        start_recv = None

        # Receive chunks of data until chunk_size == 0
        chunk_size = None
        while chunk_size != 0:
            # Determine how much to read depending on header or data
            size = 2 if chunk_size is None else chunk_size

            # If data is needed, keep reading until all bytes have been received
            remaining = size - len(self._recv_buffer)
            ready_to_read = None
            while remaining > 0:
                # Read up to the required amount remaining
                b = recv(8192)
                if b:
                    if start_recv is None:
                        start_recv = perf_counter()
                    if __debug__: log_debug("S: %r", b)
                else:
                    if ready_to_read is not None:
                        raise ProtocolError("Server closed connection")
                remaining -= len(b)
                self._recv_buffer += b

                # If more is required, wait for available network data
                if remaining > 0:
                    ready_to_read, _, _ = select((socket,), (), (), 0)
                    while not ready_to_read:
                        ready_to_read, _, _ = select((socket,), (), (), 0)

            # Split off the amount of data required and keep the rest in the buffer
            data, self._recv_buffer = self._recv_buffer[:size], self._recv_buffer[size:]

            if chunk_size is None:
                # Interpret data as chunk header
                chunk_size, = struct_unpack_from(">H", data)
            elif chunk_size > 0:
                # Interpret data as chunk
                write(data)
                chunk_size = None

        if times:
            times[3] = start_recv
            times[4] = perf_counter()

        # Unpack the message structure from the raw byte stream
        # (there should be only one)
        raw.seek(0)
        message = next(unpack())
        if not isinstance(message, Structure):
            # Something other than a message has been received
            log_error("S: @*#!")
            raise ProtocolError("Non-message data received from server")
        signature, fields = message
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

        if __debug__: log_info("C: INIT %r", user_agent)
        self._send_messages((INIT, (user_agent,)))

        signature, (data,) = self._recv_message()
        if signature == SUCCESS:
            if __debug__: log_info("S: SUCCESS %r", data)
        else:
            if __debug__: log_info("S: FAILURE %r", data)
            raise ProtocolError("Initialisation failed")

    def run(self, statement, parameters=None):
        """ Run a parameterised Cypher statement.
        """
        # Collect six checkpoint times for each run
        # INIT | START_SEND | END_SEND | START_RECV | END_RECV | RETURN
        t = [perf_counter(), None, None, None, None, None]

        recv_message = self._recv_message

        # Ensure the statement is a Unicode value
        if isinstance(statement, bytes):
            statement = statement.decode("UTF-8")

        parameters = dict(parameters or {})
        if __debug__:
            log_info("C: RUN %r %r", statement, parameters)
            log_info("C: PULL_ALL")
        t[1], t[2] = self._send_messages((RUN, (statement, parameters)),
                                         (PULL_ALL, ()))

        signature, (data,) = recv_message(t)
        if signature == SUCCESS:
            fields = data["fields"]
            if __debug__: log_info("S: SUCCESS %r", data)
        else:
            if __debug__: log_info("S: FAILURE %r", data)
            raise CypherError(data)

        records = []
        append = records.append
        more = True
        while more:
            signature, (data,) = recv_message()
            if signature == RECORD:
                if __debug__: log_info("S: RECORD %r", data)
                append(Record(fields, tuple(map(hydrated, data))))
            elif signature == SUCCESS:
                if __debug__: log_info("S: SUCCESS %r", data)
                more = False
            else:
                if __debug__: log_info("S: FAILURE %r", data)
                raise CypherError(data)

        bench = self._bench
        if bench is not None:
            t[5] = perf_counter()
            bench.append(t)

        return records

    def ack_failure(self):
        """ Send an acknowledgement for a previous failure.
        """
        if __debug__: log_info("C: ACK_FAILURE")
        self._send_messages((ACK_FAILURE, ()))

        # Skip any ignored responses
        signature, fields = self._recv_message()
        while signature == IGNORED:
            if __debug__: log_info("S: IGNORED")
            signature, fields = self._recv_message()

        # Check the acknowledgement was successful
        data, = fields
        if signature == SUCCESS:
            if __debug__: log_info("S: SUCCESS %r", data)
        else:
            if __debug__: log_info("S: FAILURE %r", data)
            raise ProtocolError("Could not acknowledge failure")

    def close(self):
        """ Shut down and close the connection.
        """
        if __debug__: log_info("~~ [CLOSE]")
        socket = self.socket
        socket.shutdown(SHUT_RDWR)
        socket.close()

    @property
    def bench(self):
        if self._bench:
            return [Latency(return_ - init, end_recv - start_send, start_recv - end_send)
                    for init, start_send, end_send, start_recv, end_recv, return_ in self._bench]


class Driver(object):
    """ Accessor for a specific graph database resource.
    """

    def __init__(self, url, **config):
        self.url = url
        parsed = urlparse(self.url)
        if parsed.scheme == "gap":
            self.host = parsed.hostname
            self.port = parsed.port
        else:
            raise ProtocolError("Unsupported URL scheme: %s" % parsed.scheme)
        self.config = config

    def session(self, **config):
        """ Connect and perform a handshake in order to return a valid
        Connection object, assuming a protocol version can be agreed.
        """
        config = dict(self.config, **config)

        # Establish a connection to the host and port specified
        host = self.host
        port = self.port or DEFAULT_PORT
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
        if __debug__: log_info("C: [HANDSHAKE] %r", supported_versions)
        data = b"".join(struct_pack(">I", version) for version in supported_versions)
        if __debug__: log_debug("C: %r", data)
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
            if __debug__: log_debug("S: %r", data)
        else:
            # Some other garbled data has been received
            log_error("S: @*#!")
            raise ProtocolError("Expected four byte handshake response, received %r instead" % data)
        agreed_version, = struct_unpack(">I", data)
        if __debug__: log_info("S: [HANDSHAKE] %d", agreed_version)
        if agreed_version == 0:
            if __debug__: log_debug("~~ [CLOSE]")
            s.shutdown(SHUT_RDWR)
            s.close()
        else:
            return SessionV1(s, **config)


class GraphDatabase(object):
    """ Top level accessor for all graph database functionality.
    """

    @classmethod
    def driver(cls, url, **config):
        """ Acquire a driver instance for the given URL and configuration.
        """
        return Driver(url, **config)
