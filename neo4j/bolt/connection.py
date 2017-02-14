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
from base64 import b64encode
from collections import deque, namedtuple
from os import makedirs, open as os_open, write as os_write, close as os_close, O_CREAT, O_APPEND, O_WRONLY
from os.path import dirname, isfile, join as path_join, expanduser
from select import select
from socket import create_connection, SOL_SOCKET, SO_KEEPALIVE, SHUT_RDWR, error as SocketError
from struct import pack as struct_pack, unpack as struct_unpack
from threading import RLock

from neo4j.compat.ssl import SSL_AVAILABLE, HAS_SNI, SSLError
from neo4j.meta import version
from neo4j.packstream import Packer, Unpacker
from neo4j.util import import_best as _import_best

from .exceptions import AuthError, SecurityError, ProtocolError, ServiceUnavailable

ChunkedInputBuffer = _import_best("neo4j.bolt._io", "neo4j.bolt.io").ChunkedInputBuffer
ChunkedOutputBuffer = _import_best("neo4j.bolt._io", "neo4j.bolt.io").ChunkedOutputBuffer


DEFAULT_PORT = 7687
DEFAULT_USER_AGENT = "neo4j-python/%s" % version

KNOWN_HOSTS = path_join(expanduser("~"), ".neo4j", "known_hosts")

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

message_names = {
    INIT: "INIT",
    ACK_FAILURE: "ACK_FAILURE",
    RESET: "RESET",
    RUN: "RUN",
    DISCARD_ALL: "DISCARD_ALL",
    PULL_ALL: "PULL_ALL",
    SUCCESS: "SUCCESS",
    RECORD: "RECORD",
    IGNORED: "IGNORED",
    FAILURE: "FAILURE",
}

# Set up logger
log = logging.getLogger("neo4j.bolt")
log_debug = log.debug
log_info = log.info
log_warning = log.warning
log_error = log.error


Address4 = namedtuple("Address", ["host", "port"])
Address6 = namedtuple("Address", ["host", "port", "flow_info", "scope_id"])
ServerInfo = namedtuple("ServerInfo", ["address", "version"])


class Address(object):

    def __new__(cls, host, port, flow_info=None, scope_id=None):
        if flow_info is None:
            return Address4(host, port)
        else:
            return Address6(host, port, flow_info, scope_id)


class Response(object):
    """ Subscriber object for a full response (zero or
    more detail messages followed by one summary message).
    """

    def __init__(self, connection):
        self.connection = connection
        self.complete = False

    def on_records(self, records):
        pass

    def on_success(self, metadata):
        pass

    def on_failure(self, metadata):
        pass

    def on_ignored(self, metadata=None):
        pass


class InitResponse(Response):

    def on_success(self, metadata):
        super(InitResponse, self).on_success(metadata)
        connection = self.connection
        address = Address(*connection.socket.getpeername())
        version = metadata.get("server")
        connection.server = ServerInfo(address, version)

    def on_failure(self, metadata):
        raise ServiceUnavailable(metadata.get("message", "INIT failed"), metadata.get("code"))


class Connection(object):
    """ Server connection for Bolt protocol v1.

    A :class:`.Connection` should be constructed following a
    successful Bolt handshake and takes the socket over which
    the handshake was carried out.

    .. note:: logs at INFO level
    """

    in_use = False

    closed = False

    defunct = False

    #: The pool of which this connection is a member
    pool = None

    #: Error class used for raising connection errors
    Error = ServiceUnavailable

    def __init__(self, sock, **config):
        self.socket = sock
        self.address = sock.getpeername()
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
                self.auth_dict = vars(config["auth"])
            except (KeyError, TypeError):
                raise TypeError("Cannot determine auth details from %r" % auth)

        # Pick up the server certificate, if any
        self.der_encoded_server_certificate = config.get("der_encoded_server_certificate")

        response = InitResponse(self)
        self.append(INIT, (self.user_agent, self.auth_dict), response=response)
        self.sync()

    def __del__(self):
        self.close()

    def append(self, signature, fields=(), response=None):
        """ Add a message to the outgoing queue.

        :arg signature: the signature of the message
        :arg fields: the fields of the message as a tuple
        :arg response: a response object to handle callbacks
        """
        #log_info("C: %s %r", message_names[signature], fields)
        self.packer.pack_struct(signature, fields)
        self.output_buffer.chunk()
        self.output_buffer.chunk()
        self.responses.append(response)

    def acknowledge_failure(self):
        """ Add an ACK_FAILURE message to the outgoing queue, send
        it and consume all remaining messages.
        """
        response = Response(self)

        def on_failure(metadata):
            raise ProtocolError("ACK_FAILURE failed")

        response.on_failure = on_failure

        self.append(ACK_FAILURE, response=response)
        self.sync()

    def reset(self):
        """ Add a RESET message to the outgoing queue, send
        it and consume all remaining messages.
        """
        response = Response(self)

        def on_failure(metadata):
            raise ProtocolError("Reset failed")

        response.on_failure = on_failure

        self.append(RESET, response=response)
        self.send()
        fetch = self.fetch
        while not response.complete:
            fetch()

    def send(self):
        """ Send all queued messages to the server.
        """
        data = self.output_buffer.view()
        if not data:
            return
        #log_debug("C: %r" % data.tobytes())
        if self.closed:
            raise self.Error("Failed to write to closed connection %r" % (self.address,))
        if self.defunct:
            raise self.Error("Failed to write to defunct connection %r" % (self.address,))
        self.socket.sendall(data)
        self.output_buffer.clear()

    def fetch(self):
        """ Receive at least one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary messages fetched
        """
        if self.closed:
            raise self.Error("Failed to read from closed connection %r" % (self.address,))
        if self.defunct:
            raise self.Error("Failed to read from defunct connection %r" % (self.address,))
        if not self.responses:
            return 0, 0

        self._receive()

        details, summary_signature, summary_metadata = self._unpack()

        if details:
            #log_info("S: RECORD (%r)", data)  # TODO
            self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == SUCCESS:
            #log_info("S: SUCCESS (%r)", metadata)
            response.on_success(summary_metadata or {})
        elif summary_signature == IGNORED:
            #log_info("S: IGNORED (%r)", metadata)
            response.on_ignored(summary_metadata or {})
        elif summary_signature == FAILURE:
            #log_info("S: FAILURE (%r)", metadata)
            response.on_failure(summary_metadata or {})
        else:
            raise ProtocolError("Unexpected response message with signature %02X" % summary_signature)

        return len(details), 1

    def _receive(self):
        try:
            received = self.input_buffer.receive_message(self.socket, 8192)
        except SocketError:
            received = False
        if not received:
            self.defunct = True
            self.close()
            raise self.Error("Failed to read from defunct connection %r" % (self.address,))

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
        if not self.closed:
            #log_info("~~ [CLOSE]")
            self.socket.close()
            self.closed = True


class ConnectionPool(object):
    """ A collection of connections to one or more server addresses.
    """

    closed = False

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
        This method is thread safe.
        """
        if self.closed:
            raise ServiceUnavailable("This connection pool is closed so no new "
                                     "connections may be acquired")
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:
                connections = self.connections[address] = deque()
            for connection in list(connections):
                if connection.closed or connection.defunct:
                    connections.remove(connection)
                    continue
                if not connection.in_use:
                    connection.in_use = True
                    return connection
            try:
                connection = self.connector(address)
            except ServiceUnavailable as error:
                if error.code == "Neo.ClientError.Security.Unauthorized":
                    raise AuthError(error.args[0])
                self.remove(address)
                raise
            else:
                connection.pool = self
                connection.in_use = True
                connections.append(connection)
                return connection

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
            self.closed = True
            for address in list(self.connections):
                self.remove(address)


class CertificateStore(object):

    def match_or_trust(self, host, der_encoded_certificate):
        """ Check whether the supplied certificate matches that stored for the
        specified host. If it does, return ``True``, if it doesn't, return
        ``False``. If no entry for that host is found, add it to the store
        and return ``True``.

        :arg host:
        :arg der_encoded_certificate:
        :return:
        """
        raise NotImplementedError()


class PersonalCertificateStore(CertificateStore):

    def __init__(self, path=None):
        self.path = path or KNOWN_HOSTS

    def match_or_trust(self, host, der_encoded_certificate):
        base64_encoded_certificate = b64encode(der_encoded_certificate)
        if isfile(self.path):
            with open(self.path) as f_in:
                for line in f_in:
                    known_host, _, known_cert = line.strip().partition(":")
                    known_cert = known_cert.encode("utf-8")
                    if host == known_host:
                        return base64_encoded_certificate == known_cert
        # First use (no hosts match)
        try:
            makedirs(dirname(self.path))
        except OSError:
            pass
        f_out = os_open(self.path, O_CREAT | O_APPEND | O_WRONLY, 0o600)  # TODO: Windows
        if isinstance(host, bytes):
            os_write(f_out, host)
        else:
            os_write(f_out, host.encode("utf-8"))
        os_write(f_out, b":")
        os_write(f_out, base64_encoded_certificate)
        os_write(f_out, b"\n")
        os_close(f_out)
        return True


def connect(address, ssl_context=None, **config):
    """ Connect and perform a handshake and return a valid Connection object, assuming
    a protocol version can be agreed.
    """

    # Establish a connection to the host and port specified
    # Catches refused connections see:
    # https://docs.python.org/2/library/errno.html
    log_info("~~ [CONNECT] %s", address)
    try:
        s = create_connection(address)
        s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1 if config.get("keep_alive", True) else 0)
    except SocketError as error:
        if error.errno in (61, 111, 10061):
            raise ServiceUnavailable("Failed to establish connection to %r" % (address,))
        else:
            raise

    # Secure the connection if an SSL context has been provided
    if ssl_context and SSL_AVAILABLE:
        host, port = address
        log_info("~~ [SECURE] %s", host)
        try:
            s = ssl_context.wrap_socket(s, server_hostname=host if HAS_SNI else None)
        except SSLError as cause:
            error = SecurityError("Failed to establish secure connection to %r" % cause.args[1])
            error.__cause__ = cause
            raise error
        else:
            from neo4j.v1 import TRUST_DEFAULT, TRUST_ON_FIRST_USE
            # Check that the server provides a certificate
            der_encoded_server_certificate = s.getpeercert(binary_form=True)
            if der_encoded_server_certificate is None:
                raise ProtocolError("When using a secure socket, the server should always "
                                    "provide a certificate")
            trust = config.get("trust", TRUST_DEFAULT)
            if trust == TRUST_ON_FIRST_USE:
                store = PersonalCertificateStore()
                if not store.match_or_trust(host, der_encoded_server_certificate):
                    raise ProtocolError("Server certificate does not match known certificate "
                                        "for %r; check details in file %r" % (host, KNOWN_HOSTS))
    else:
        der_encoded_server_certificate = None

    # Send details of the protocol versions supported
    supported_versions = [1, 0, 0, 0]
    handshake = [MAGIC_PREAMBLE] + supported_versions
    log_info("C: [HANDSHAKE] 0x%X %r", MAGIC_PREAMBLE, supported_versions)
    data = b"".join(struct_pack(">I", num) for num in handshake)
    log_debug("C: b%r", data)
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
        raise ProtocolError("Connection to %r closed without handshake response" % (address,))
    if data_size == 4:
        log_debug("S: b%r", data)
    else:
        # Some other garbled data has been received
        log_error("S: @*#!")
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
        raise ServiceUnavailable("Cannot to connect to Bolt service on %r "
                                 "(looks like HTTP)" % (address,))
    else:
        log_error("S: [CLOSE]")
        raise ProtocolError("Unknown Bolt protocol version: %d", agreed_version)
