# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import selectors
import socket
from socket import (
    AF_INET,
    AF_INET6,
    SHUT_RDWR,
    SO_KEEPALIVE,
    socket,
    SOL_SOCKET,
    timeout as SocketTimeout,
)
from ssl import (
    CertificateError,
    HAS_SNI,
    SSLError,
)
import struct

from .._exceptions import (
    BoltError,
    BoltProtocolError,
    BoltSecurityError,
    SocketDeadlineExceeded,
)
from .._deadline import Deadline
from ..addressing import Address
from ..exceptions import (
    DriverError,
    ServiceUnavailable,
)


log = logging.getLogger("neo4j")


def _sanitize_deadline(deadline):
    if deadline is None:
        return None
    deadline = Deadline.from_timeout_or_deadline(deadline)
    if deadline.to_timeout() is None:
        return None
    return deadline


class BoltSocket:
    Bolt = None

    def __init__(self, socket_: socket):
        self._socket = socket_
        self._deadline = None

    @property
    def _socket(self):
        return self.__socket

    @_socket.setter
    def _socket(self, socket_: socket):
        self.__socket = socket_
        self.getsockname = socket_.getsockname
        self.getpeername = socket_.getpeername
        if hasattr(socket, "getpeercert"):
            self.getpeercert = socket_.getpeercert
        elif hasattr(self, "getpeercert"):
            del self.getpeercert
        self.gettimeout = socket_.gettimeout
        self.settimeout = socket_.settimeout

    def _wait_for_io(self, func, *args, **kwargs):
        if self._deadline is None:
            return func(*args, **kwargs)
        timeout = self._socket.gettimeout()
        deadline_timeout = self._deadline.to_timeout()
        if deadline_timeout <= 0:
            raise SocketDeadlineExceeded("timed out")
        if timeout is None or deadline_timeout <= timeout:
            self._socket.settimeout(deadline_timeout)
            try:
                return func(*args, **kwargs)
            except SocketTimeout as e:
                raise SocketDeadlineExceeded("timed out") from e
            finally:
                self._socket.settimeout(timeout)
        return func(*args, **kwargs)

    def get_deadline(self):
        return self._deadline

    def set_deadline(self, deadline):
        self._deadline = _sanitize_deadline(deadline)

    def recv(self, n):
        return self._wait_for_io(self._socket.recv, n)

    def recv_into(self, buffer, nbytes):
        return self._wait_for_io(self._socket.recv_into, buffer, nbytes)

    def sendall(self, data):
        return self._wait_for_io(self._socket.sendall, data)

    def close(self):
        self._socket.shutdown(SHUT_RDWR)
        self._socket.close()

    @classmethod
    def _connect(cls, resolved_address, timeout, keep_alive):
        """

        :param resolved_address:
        :param timeout: seconds
        :param keep_alive: True or False
        :return: socket object
        """

        s = None  # The socket

        try:
            if len(resolved_address) == 2:
                s = socket(AF_INET)
            elif len(resolved_address) == 4:
                s = socket(AF_INET6)
            else:
                raise ValueError(
                    "Unsupported address {!r}".format(resolved_address))
            t = s.gettimeout()
            if timeout:
                s.settimeout(timeout)
            log.debug("[#0000]  C: <OPEN> %s", resolved_address)
            s.connect(resolved_address)
            s.settimeout(t)
            keep_alive = 1 if keep_alive else 0
            s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, keep_alive)
            return s
        except SocketTimeout:
            log.debug("[#0000]  C: <TIMEOUT> %s", resolved_address)
            log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
            cls.close_socket(s)
            raise ServiceUnavailable(
                "Timed out trying to establish connection to {!r}".format(
                    resolved_address))
        except OSError as error:
            log.debug("[#0000]  C: <ERROR> %s %s", type(error).__name__,
                      " ".join(map(repr, error.args)))
            log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
            s.close()
            raise ServiceUnavailable(
                "Failed to establish connection to {!r} (reason {})".format(
                    resolved_address, error))

    @classmethod
    def _secure(cls, s, host, ssl_context):
        local_port = s.getsockname()[1]
        # Secure the connection if an SSL context has been provided
        if ssl_context:
            log.debug("[#%04X]  C: <SECURE> %s", local_port, host)
            try:
                sni_host = host if HAS_SNI and host else None
                s = ssl_context.wrap_socket(s, server_hostname=sni_host)
            except (OSError, SSLError, CertificateError) as cause:
                raise BoltSecurityError(
                    message="Failed to establish encrypted connection.",
                    address=(host, local_port)
                ) from cause
            # Check that the server provides a certificate
            der_encoded_server_certificate = s.getpeercert(binary_form=True)
            if der_encoded_server_certificate is None:
                raise BoltProtocolError(
                    "When using an encrypted socket, the server should always "
                    "provide a certificate", address=(host, local_port)
                )
            return s
        return s

    @classmethod
    def _handshake(cls, s, resolved_address):
        """

        :param s: Socket
        :param resolved_address:

        :return: (socket, version, client_handshake, server_response_data)
        """
        local_port = s.getsockname()[1]

        # TODO: Optimize logging code
        handshake = cls.Bolt.get_handshake()
        handshake = struct.unpack(">16B", handshake)
        handshake = [handshake[i:i + 4] for i in range(0, len(handshake), 4)]

        supported_versions = [
            ("0x%02X%02X%02X%02X" % (vx[0], vx[1], vx[2], vx[3])) for vx in
            handshake]

        log.debug("[#%04X]  C: <MAGIC> 0x%08X", local_port,
                  int.from_bytes(cls.Bolt.MAGIC_PREAMBLE, byteorder="big"))
        log.debug("[#%04X]  C: <HANDSHAKE> %s %s %s %s", local_port,
                  *supported_versions)

        data = cls.Bolt.MAGIC_PREAMBLE + cls.Bolt.get_handshake()
        s.sendall(data)

        # Handle the handshake response
        ready_to_read = False
        with selectors.DefaultSelector() as selector:
            selector.register(s, selectors.EVENT_READ)
            selector.select(1)
        try:
            data = s.recv(4)
        except OSError:
            raise ServiceUnavailable(
                "Failed to read any data from server {!r} "
                "after connected".format(resolved_address))
        data_size = len(data)
        if data_size == 0:
            # If no data is returned after a successful select
            # response, the server has closed the connection
            log.debug("[#%04X]  S: <CLOSE>", local_port)
            BoltSocket.close_socket(s)
            raise ServiceUnavailable(
                "Connection to {address} closed without handshake response".format(
                    address=resolved_address))
        if data_size != 4:
            # Some garbled data has been received
            log.debug("[#%04X]  S: @*#!", local_port)
            s.close()
            raise BoltProtocolError(
                "Expected four byte Bolt handshake response from %r, received %r instead; check for incorrect port number" % (
                resolved_address, data), address=resolved_address)
        elif data == b"HTTP":
            log.debug("[#%04X]  S: <CLOSE>", local_port)
            BoltSocket.close_socket(s)
            raise ServiceUnavailable(
                "Cannot to connect to Bolt service on {!r} "
                "(looks like HTTP)".format(resolved_address))
        agreed_version = data[-1], data[-2]
        log.debug("[#%04X]  S: <HANDSHAKE> 0x%06X%02X", local_port,
                  agreed_version[1], agreed_version[0])
        return cls(s), agreed_version, handshake, data

    @classmethod
    def close_socket(cls, socket_):
        try:
            if isinstance(socket_, BoltSocket):
                socket.close()
            else:
                socket_.shutdown(SHUT_RDWR)
                socket_.close()
        except OSError:
            pass

    @classmethod
    def connect(cls, address, *, timeout, custom_resolver, ssl_context,
                keep_alive):
        """ Connect and perform a handshake and return a valid Connection object,
        assuming a protocol version can be agreed.
        """
        errors = []
        # Establish a connection to the host and port specified
        # Catches refused connections see:
        # https://docs.python.org/2/library/errno.html

        resolved_addresses = Address(address).resolve(resolver=custom_resolver)
        for resolved_address in resolved_addresses:
            s = None
            try:
                s = BoltSocket._connect(resolved_address, timeout, keep_alive)
                s = BoltSocket._secure(s, resolved_address.host_name,
                                       ssl_context)
                return BoltSocket._handshake(s, resolved_address)
            except (BoltError, DriverError, OSError) as error:
                try:
                    local_port = s.getsockname()[1]
                except (OSError, AttributeError):
                    local_port = 0
                err_str = error.__class__.__name__
                if str(error):
                    err_str += ": " + str(error)
                log.debug("[#%04X]  C: <CONNECTION FAILED> %s", local_port,
                          err_str)
                if s:
                    BoltSocket.close_socket(s)
                errors.append(error)
            except Exception:
                if s:
                    BoltSocket.close_socket(s)
                raise
        if not errors:
            raise ServiceUnavailable(
                "Couldn't connect to %s (resolved to %s)" % (
                    str(address), tuple(map(str, resolved_addresses)))
            )
        else:
            raise ServiceUnavailable(
                "Couldn't connect to %s (resolved to %s):\n%s" % (
                    str(address), tuple(map(str, resolved_addresses)),
                    "\n".join(map(str, errors))
                )
            ) from errors[0]
