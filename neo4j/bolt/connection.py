#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
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
This module contains the low-level functionality required for speaking
Bolt. It is not intended to be used directly by driver users. Instead,
the `session` module provides the main user-facing abstractions.
"""

from __future__ import division

import logging
from collections import deque
from select import select
from socket import socket, SOL_SOCKET, SO_KEEPALIVE, SHUT_RDWR, error as SocketError, timeout as SocketTimeout, AF_INET, AF_INET6
from struct import pack as struct_pack, unpack as struct_unpack
from threading import RLock

from neo4j.addressing import SocketAddress, is_ip_address
from neo4j.bolt.cert import KNOWN_HOSTS
from neo4j.bolt.response import InitResponse, AckFailureResponse, ResetResponse
from neo4j.compat.ssl import SSL_AVAILABLE, HAS_SNI, SSLError
from neo4j.exceptions import ProtocolError, SecurityError, ServiceUnavailable
from neo4j.meta import version
from neo4j.packstream import Packer, Unpacker
from neo4j.util import import_best as _import_best

ChunkedInputBuffer = _import_best("neo4j.bolt._io", "neo4j.bolt.io").ChunkedInputBuffer
ChunkedOutputBuffer = _import_best("neo4j.bolt._io", "neo4j.bolt.io").ChunkedOutputBuffer


DEFAULT_CONNECTION_TIMEOUT = 5.0
DEFAULT_PORT = 7687
DEFAULT_USER_AGENT = "neo4j-python/%s" % version

MAGIC_PREAMBLE = 0x6060B017


# Signature bytes for each message type
INIT = b"\x01"             # 0000 0001 // INIT <user_agent> <auth>
ACK_FAILURE = b"\x0E"      # 0000 1110 // ACK_FAILURE
RESET = b"\x0F"            # 0000 1111 // RESET
RUN = b"\x10"              # 0001 0000 // RUN <statement> <parameters>
DISCARD_ALL = b"\x2F"      # 0010 1111 // DISCARD *
PULL_ALL = b"\x3F"         # 0011 1111 // PULL *
SUCCESS = b"\x70"          # 0111 0000 // SUCCESS <metadata>
RECORD = b"\x71"           # 0111 0001 // RECORD <value>
IGNORED = b"\x7E"          # 0111 1110 // IGNORED <metadata>
FAILURE = b"\x7F"          # 0111 1111 // FAILURE <metadata>

DETAIL = {RECORD}
SUMMARY = {SUCCESS, IGNORED, FAILURE}

# Set up logger
log = logging.getLogger("neo4j.bolt")
log_debug = log.debug
log_info = log.info
log_warning = log.warning
log_error = log.error


class ServerInfo(object):

    address = None

    version = None

    def __init__(self, address):
        self.address = address

    def product(self):
        if not self.version:
            return None
        value, _, _ = self.version.partition("/")
        return value

    def version_info(self):
        if not self.version:
            return None
        _, _, value = self.version.partition("/")
        value = value.replace("-", ".").split(".")
        for i, v in enumerate(value):
            try:
                value[i] = int(v)
            except ValueError:
                pass
        return tuple(value)

    def supports_statement_reuse(self):
        if not self.version:
            return False
        if self.product() != "Neo4j":
            return False
        return self.version_info() >= (3, 2)

    def supports_bytes(self):
        if not self.version:
            return False
        if self.product() != "Neo4j":
            return False
        return self.version_info() >= (3, 2)


class Connection(object):
    """ Server connection for Bolt protocol v1.

    A :class:`.Connection` should be constructed following a
    successful Bolt handshake and takes the socket over which
    the handshake was carried out.

    .. note:: logs at INFO level
    """

    #: Server details for this connection
    server = None

    in_use = False

    _closed = False

    _defunct = False

    #: The pool of which this connection is a member
    pool = None

    #: Error class used for raising connection errors
    Error = ServiceUnavailable

    _supports_statement_reuse = False

    _last_run_statement = None

    def __init__(self, sock, **config):
        self.socket = sock
        self.server = ServerInfo(SocketAddress.from_socket(sock))
        self.input_buffer = ChunkedInputBuffer()
        self.output_buffer = ChunkedOutputBuffer()
        self.packer = Packer(self.output_buffer)
        self.unpacker = Unpacker()
        self.responses = deque()

        # Determine the user agent and ensure it is a Unicode value
        user_agent = config.get("user_agent", DEFAULT_USER_AGENT)
        if isinstance(user_agent, bytes):
            user_agent = user_agent.decode("UTF-8")
        self.user_agent = user_agent

        # Determine auth details
        auth = config.get("auth")
        if not auth:
            self.auth_dict = {}
        elif isinstance(auth, tuple) and 2 <= len(auth) <= 3:
            from neo4j.v1 import basic_auth
            self.auth_dict = vars(basic_auth(*auth))
        else:
            try:
                self.auth_dict = vars(auth)
            except (KeyError, TypeError):
                raise TypeError("Cannot determine auth details from %r" % auth)

        # Pick up the server certificate, if any
        self.der_encoded_server_certificate = config.get("der_encoded_server_certificate")

        response = InitResponse(self)
        self.append(INIT, (self.user_agent, self.auth_dict), response=response)
        self.sync()

        self._supports_statement_reuse = self.server.supports_statement_reuse()
        self.packer.supports_bytes = self.server.supports_bytes()

    def __del__(self):
        self.close()

    def append(self, signature, fields=(), response=None):
        """ Add a message to the outgoing queue.

        :arg signature: the signature of the message
        :arg fields: the fields of the message as a tuple
        :arg response: a response object to handle callbacks
        """
        if signature == RUN:
            if self._supports_statement_reuse:
                statement = fields[0]
                if statement.upper() not in ("BEGIN", "COMMIT", "ROLLBACK"):
                    if statement == self._last_run_statement:
                        fields = ("",) + fields[1:]
                    else:
                        self._last_run_statement = statement
            log_info("C: RUN %r", fields)
        elif signature == PULL_ALL:
            log_info("C: PULL_ALL %r", fields)
        elif signature == DISCARD_ALL:
            log_info("C: DISCARD_ALL %r", fields)
        elif signature == RESET:
            log_info("C: RESET %r", fields)
        elif signature == ACK_FAILURE:
            log_info("C: ACK_FAILURE %r", fields)
        elif signature == INIT:
            log_info("C: INIT (%r, {...})", fields[0])
        else:
            raise ValueError("Unknown message signature")
        self.packer.pack_struct(signature, fields)
        self.output_buffer.chunk()
        self.output_buffer.chunk()
        self.responses.append(response)

    def acknowledge_failure(self):
        """ Add an ACK_FAILURE message to the outgoing queue, send
        it and consume all remaining messages.
        """
        self.append(ACK_FAILURE, response=AckFailureResponse(self))
        self.sync()

    def reset(self):
        """ Add a RESET message to the outgoing queue, send
        it and consume all remaining messages.
        """
        self.append(RESET, response=ResetResponse(self))
        self.sync()

    def send(self):
        """ Send all queued messages to the server.
        """
        data = self.output_buffer.view()
        if not data:
            return
        if self.closed():
            raise self.Error("Failed to write to closed connection {!r}".format(self.server.address))
        if self.defunct():
            raise self.Error("Failed to write to defunct connection {!r}".format(self.server.address))
        self.socket.sendall(data)
        self.output_buffer.clear()

    def fetch(self):
        """ Receive at least one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary messages fetched
        """
        if self.closed():
            raise self.Error("Failed to read from closed connection {!r}".format(self.server.address))
        if self.defunct():
            raise self.Error("Failed to read from defunct connection {!r}".format(self.server.address))
        if not self.responses:
            return 0, 0

        self._receive()

        details, summary_signature, summary_metadata = self._unpack()

        if details:
            log_info("S: RECORD * %d", len(details))  # TODO
            self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == SUCCESS:
            log_info("S: SUCCESS (%r)", summary_metadata)
            response.on_success(summary_metadata or {})
        elif summary_signature == IGNORED:
            self._last_run_statement = None
            log_info("S: IGNORED (%r)", summary_metadata)
            response.on_ignored(summary_metadata or {})
        elif summary_signature == FAILURE:
            self._last_run_statement = None
            log_info("S: FAILURE (%r)", summary_metadata)
            response.on_failure(summary_metadata or {})
        else:
            self._last_run_statement = None
            raise ProtocolError("Unexpected response message with signature %02X" % summary_signature)

        return len(details), 1

    def _receive(self):
        try:
            received = self.input_buffer.receive_message(self.socket, 8192)
        except SocketError:
            received = False
        if not received:
            self._defunct = True
            self.close()
            raise self.Error("Failed to read from defunct connection {!r}".format(self.server.address))

    def _unpack(self):
        unpacker = self.unpacker
        input_buffer = self.input_buffer

        details = []
        summary_signature = None
        summary_metadata = None
        more = True
        while more:
            unpacker.attach(input_buffer.frame())
            size, signature = unpacker.unpack_structure_header()
            if size > 1:
                raise ProtocolError("Expected one field")
            if signature == RECORD:
                data = unpacker.unpack_list()
                details.append(data)
                more = input_buffer.frame_message()
            else:
                summary_signature = signature
                summary_metadata = unpacker.unpack_map()
                more = False
        return details, summary_signature, summary_metadata

    def sync(self):
        """ Send and fetch all outstanding messages.

        :return: 2-tuple of number of detail messages and number of summary messages fetched
        """
        self.send()
        detail_count = summary_count = 0
        while self.responses:
            response = self.responses[0]
            while not response.complete:
                detail_delta, summary_delta = self.fetch()
                detail_count += detail_delta
                summary_count += summary_delta
        return detail_count, summary_count

    def close(self):
        """ Close the connection.
        """
        if not self.closed():
            log_info("~~ [CLOSE]")
            self.socket.close()
            self._closed = True

    def closed(self):
        return self._closed

    def defunct(self):
        return self._defunct


class ConnectionPool(object):
    """ A collection of connections to one or more server addresses.
    """

    _closed = False

    def __init__(self, connector):
        self.connector = connector
        self.connections = {}
        self.lock = RLock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def acquire_direct(self, address):
        """ Acquire a connection to a given address from the pool.
        The address supplied should always be an IP address, not
        a host name.

        This method is thread safe.
        """
        if self.closed():
            raise ServiceUnavailable("Connection pool closed")
        if not is_ip_address(address[0]):
            raise ValueError("Invalid IP address {!r}".format(address[0]))
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:
                connections = self.connections[address] = deque()
            for connection in list(connections):
                if connection.closed() or connection.defunct():
                    connections.remove(connection)
                    continue
                if not connection.in_use:
                    connection.in_use = True
                    return connection
            try:
                connection = self.connector(address)
            except ServiceUnavailable:
                self.remove(address)
                raise
            else:
                connection.pool = self
                connection.in_use = True
                connections.append(connection)
                return connection

    def acquire(self, access_mode=None):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param access_mode:
        """

    def release(self, connection):
        """ Release a connection back into the pool.
        This method is thread safe.
        """
        with self.lock:
            connection.in_use = False

    def remove(self, address):
        """ Remove an address from the connection pool, if present, closing
        all connections to that address.
        """
        with self.lock:
            for connection in self.connections.pop(address, ()):
                try:
                    connection.close()
                except IOError:
                    pass

    def close(self):
        """ Close all connections and empty the pool.
        This method is thread safe.
        """
        with self.lock:
            self._closed = True
            for address in list(self.connections):
                self.remove(address)

    def closed(self):
        """ Return :const:`True` if this pool is closed, :const:`False`
        otherwise.
        """
        with self.lock:
            return self._closed


def connect(address, ssl_context=None, **config):
    """ Connect and perform a handshake and return a valid Connection object, assuming
    a protocol version can be agreed.
    """

    # Establish a connection to the host and port specified
    # Catches refused connections see:
    # https://docs.python.org/2/library/errno.html
    log_info("~~ [CONNECT] %s", address)
    s = None
    try:
        if len(address) == 2:
            s = socket(AF_INET)
        elif len(address) == 4:
            s = socket(AF_INET6)
        else:
            raise ValueError("Unsupported address {!r}".format(address))
        t = s.gettimeout()
        s.settimeout(config.get("connection_timeout", DEFAULT_CONNECTION_TIMEOUT))
        s.connect(address)
        s.settimeout(t)
        s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1 if config.get("keep_alive", True) else 0)
    except SocketTimeout:
        if s:
            try:
                s.close()
            except:
                pass
        raise ServiceUnavailable("Timed out trying to establish connection to {!r}".format(address))
    except SocketError as error:
        if s:
            try:
                s.close()
            except:
                pass
        if error.errno in (61, 111, 10061):
            raise ServiceUnavailable("Failed to establish connection to {!r}".format(address))
        else:
            raise
    except ConnectionResetError:
        raise ServiceUnavailable("Failed to establish connection to {!r}".format(address))

    # Secure the connection if an SSL context has been provided
    if ssl_context and SSL_AVAILABLE:
        host = address[0]
        log_info("~~ [SECURE] %s", host)
        try:
            s = ssl_context.wrap_socket(s, server_hostname=host if HAS_SNI else None)
        except SSLError as cause:
            s.close()
            error = SecurityError("Failed to establish secure connection to {!r}".format(cause.args[1]))
            error.__cause__ = cause
            raise error
        else:
            from neo4j.v1 import TRUST_DEFAULT, TRUST_ON_FIRST_USE
            # Check that the server provides a certificate
            der_encoded_server_certificate = s.getpeercert(binary_form=True)
            if der_encoded_server_certificate is None:
                s.close()
                raise ProtocolError("When using a secure socket, the server should always "
                                    "provide a certificate")
            trust = config.get("trust", TRUST_DEFAULT)
            if trust == TRUST_ON_FIRST_USE:
                from neo4j.bolt.cert import PersonalCertificateStore
                store = PersonalCertificateStore()
                if not store.match_or_trust(host, der_encoded_server_certificate):
                    s.close()
                    raise ProtocolError("Server certificate does not match known certificate "
                                        "for %r; check details in file %r" % (host, KNOWN_HOSTS))
    else:
        der_encoded_server_certificate = None

    # Send details of the protocol versions supported
    supported_versions = [1, 0, 0, 0]
    handshake = [MAGIC_PREAMBLE] + supported_versions
    log_info("C: [HANDSHAKE] 0x%X %r", MAGIC_PREAMBLE, supported_versions)
    data = b"".join(struct_pack(">I", num) for num in handshake)
    s.sendall(data)

    # Handle the handshake response
    ready_to_read, _, _ = select((s,), (), (), 0)
    while not ready_to_read:
        ready_to_read, _, _ = select((s,), (), (), 0)
    try:
        data = s.recv(4)
    except ConnectionResetError:
        raise ServiceUnavailable("Failed to read any data from server {!r} after connected".format(address))
    data_size = len(data)
    if data_size == 0:
        # If no data is returned after a successful select
        # response, the server has closed the connection
        log_error("S: [CLOSE]")
        s.close()
        raise ProtocolError("Connection to %r closed without handshake response" % (address,))
    if data_size != 4:
        # Some garbled data has been received
        log_error("S: @*#!")
        s.close()
        raise ProtocolError("Expected four byte handshake response, received %r instead" % data)
    agreed_version, = struct_unpack(">I", data)
    log_info("S: [HANDSHAKE] %d", agreed_version)
    if agreed_version == 0:
        log_info("~~ [CLOSE]")
        s.shutdown(SHUT_RDWR)
        s.close()
    elif agreed_version == 1:
        return Connection(s, der_encoded_server_certificate=der_encoded_server_certificate, **config)
    elif agreed_version == 0x48545450:
        log_error("S: [CLOSE]")
        s.close()
        raise ServiceUnavailable("Cannot to connect to Bolt service on {!r} "
                                 "(looks like HTTP)".format(address))
    else:
        log_error("S: [CLOSE]")
        s.close()
        raise ProtocolError("Unknown Bolt protocol version: {}".format(agreed_version))
