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


from io import DEFAULT_BUFFER_SIZE
import socket
import struct
from select import select


__all__ = ["SocketSession"]


class SocketSession(object):

    default_port = 7687
    subclasses = [None, None, None, None]

    @classmethod
    def create(cls, host, port):
        """ Create a session for raw socket transport.
        """

        # Establish a socket connection
        s = socket.create_connection((host, port or cls.default_port))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Negotiate protocol version
        versions = [0 if c is None else c.version for c in cls.subclasses]
        s.send(b"".join(struct.pack(">I", version) for version in versions))

        # Attempt to create session instance for agreed protocol version
        chosen_version, = struct.unpack(">I", s.recv(4))
        if chosen_version == 0:
            # No protocol version agreed
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            raise RuntimeError("Unable to negotiate protocol version")
        else:
            # Create an instance for the agreed protocol version
            return cls.subclasses[versions.index(chosen_version)](s)

    def __init__(self, s):
        self._socket = s
        self._buffer = b""

    def _read(self, size, closed_exception=IOError):
        """ Read a fixed number of bytes from the network.

        :param size: the number of bytes to read
        :return: the bytes read
        """
        s = self._socket
        if not s:
            raise closed_exception("Session closed")
        recv = s.recv
        required = size - len(self._buffer)
        while required > 0:
            ready_to_read, _, _ = select((s,), (), (), 0)
            while not ready_to_read:
                ready_to_read, _, _ = select((s,), (), (), 0)
            data = recv(required if required > DEFAULT_BUFFER_SIZE else DEFAULT_BUFFER_SIZE)
            data_size = len(data)
            if data_size == 0:
                raise closed_exception("Peer has closed connection")
            self._buffer += data
            required -= data_size
        buffer = self._buffer
        data, self._buffer = buffer[:size], buffer[size:]
        return data

    def _write(self, b):
        """ Write byte data to the network.

        :param b: bytes to send
        """
        s = self._socket
        if not s:
            raise RuntimeError("Session closed")
        s.sendall(b)

    def close(self):
        if self._socket:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            self._socket = None
