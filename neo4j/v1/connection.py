#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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

from base64 import b64encode
from collections import deque
from io import BytesIO
import logging
from os import makedirs, open as os_open, write as os_write, close as os_close, O_CREAT, O_APPEND, O_WRONLY
from os.path import dirname, isfile
from select import select
from socket import create_connection, SHUT_RDWR, error as SocketError
from struct import pack as struct_pack, unpack as struct_unpack, unpack_from as struct_unpack_from

from .constants import DEFAULT_PORT, DEFAULT_USER_AGENT, KNOWN_HOSTS, MAGIC_PREAMBLE, \
    TRUST_DEFAULT, TRUST_ON_FIRST_USE
from .compat import hex2
from .exceptions import ProtocolError
from .packstream import Packer, Unpacker
from .ssl_compat import SSL_AVAILABLE, HAS_SNI, SSLError


# Signature bytes for each message type
INIT = b"\x01"             # 0000 0001 // INIT <user_agent>
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


class Connection(object):
    """ Server connection through which all protocol messages
    are sent and received. This class is designed for protocol
    version 1.

    .. note:: logs at INFO level
    """

    def __init__(self, sock, **config):
        self.defunct = False
        self.channel = ChunkChannel(sock)
        self.packer = Packer(self.channel)
        self.responses = deque()
        self.closed = False

        # Determine the user agent and ensure it is a Unicode value
        user_agent = config.get("user_agent", DEFAULT_USER_AGENT)
        if isinstance(user_agent, bytes):
            user_agent = user_agent.decode("UTF-8")
        self.user_agent = user_agent

        # Determine auth details
        try:
            self.auth_dict = vars(config["auth"])
        except (KeyError, TypeError):
            self.auth_dict = {}

        # Pick up the server certificate, if any
        self.der_encoded_server_certificate = config.get("der_encoded_server_certificate")

        def on_failure(metadata):
            raise ProtocolError(metadata.get("message", "Inititalisation failed"))

        response = Response(self)
        response.on_failure = on_failure

        self.append(INIT, (self.user_agent, self.auth_dict), response=response)
        self.send()
        while not response.complete:
            self.fetch()

    def __del__(self):
        self.close()

    def append(self, signature, fields=(), response=None):
        """ Add a message to the outgoing queue.

        :arg signature: the signature of the message
        :arg fields: the fields of the message as a tuple
        :arg response: a response object to handle callbacks
        """
        if __debug__:
            log_info("C: %s %s", message_names[signature], " ".join(map(repr, fields)))

        self.packer.pack_struct_header(len(fields), signature)
        for field in fields:
            self.packer.pack(field)
        self.channel.flush(end_of_message=True)
        self.responses.append(response)

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
        if self.closed:
            raise ProtocolError("Cannot write to a closed connection")
        if self.defunct:
            raise ProtocolError("Cannot write to a defunct connection")
        self.channel.send()

    def fetch(self):
        """ Receive exactly one message from the server.
        """
        if self.closed:
            raise ProtocolError("Cannot read from a closed connection")
        if self.defunct:
            raise ProtocolError("Cannot read from a defunct connection")
        raw = BytesIO()
        unpack = Unpacker(raw).unpack
        try:
            raw.writelines(self.channel.chunk_reader())
        except ProtocolError:
            self.defunct = True
            self.close()
            raise
        # Unpack from the raw byte stream and call the relevant message handler(s)
        raw.seek(0)
        response = self.responses[0]
        for signature, fields in unpack():
            if __debug__:
                log_info("S: %s %s", message_names[signature], " ".join(map(repr, fields)))
            if signature in SUMMARY:
                response.complete = True
                self.responses.popleft()
            if signature == FAILURE:
                self.reset()
            handler_name = "on_%s" % message_names[signature].lower()
            try:
                handler = getattr(response, handler_name)
            except AttributeError:
                pass
            else:
                handler(*fields)
        raw.close()

    def close(self):
        """ Close the connection.
        """
        if not self.closed:
            if __debug__:
                log_info("~~ [CLOSE]")
            self.channel.socket.close()
            self.closed = True


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


def connect(host, port=None, ssl_context=None, **config):
    """ Connect and perform a handshake and return a valid Connection object, assuming
    a protocol version can be agreed.
    """

    # Establish a connection to the host and port specified
    port = port or DEFAULT_PORT
    if __debug__: log_info("~~ [CONNECT] %s %d", host, port)
    try:
        s = create_connection((host, port))
    except SocketError as error:
        if error.errno == 111:
            raise ProtocolError("Unable to connect to %s on port %d - is the server running?" % (host, port))
        else:
            raise

    # Secure the connection if an SSL context has been provided
    if ssl_context and SSL_AVAILABLE:
        if __debug__: log_info("~~ [SECURE] %s", host)
        try:
            s = ssl_context.wrap_socket(s, server_hostname=host if HAS_SNI else None)
        except SSLError as cause:
            error = ProtocolError("Cannot establish secure connection; %s" % cause.args[1])
            error.__cause__ = cause
            raise error
        else:
            # Check that the server provides a certificate
            der_encoded_server_certificate = s.getpeercert(binary_form=True)
            if der_encoded_server_certificate is None:
                raise ProtocolError("When using a secure socket, the server should always provide a certificate")
            trust = config.get("trust", TRUST_DEFAULT)
            if trust == TRUST_ON_FIRST_USE:
                store = PersonalCertificateStore()
                if not store.match_or_trust(host, der_encoded_server_certificate):
                    raise ProtocolError("Server certificate does not match known certificate for %r; check "
                                        "details in file %r" % (host, KNOWN_HOSTS))
    else:
        der_encoded_server_certificate = None

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
        return Connection(s, der_encoded_server_certificate=der_encoded_server_certificate, **config)
