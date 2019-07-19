#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


"""
This module contains the low-level functionality required for speaking
Bolt. It is not intended to be used directly by driver users. Instead,
the `session` module provides the main user-facing abstractions.
"""


__all__ = [
    "DEFAULT_PORT",
    "AbstractConnectionPool",
    "Connection",
    "ConnectionPool",
    "ServerInfo",
    "connect",
]


from collections import deque
from logging import getLogger
from select import select
from socket import socket, SOL_SOCKET, SO_KEEPALIVE, SHUT_RDWR, \
    timeout as SocketTimeout, AF_INET, AF_INET6
from ssl import HAS_SNI, SSLSocket, SSLError
from struct import pack as struct_pack, unpack as struct_unpack
from threading import RLock, Condition
from time import perf_counter

from neo4j.bolt.addressing import SocketAddress, Resolver
from neo4j.bolt.exceptions import ClientError, ProtocolError, SecurityError, \
    ServiceUnavailable, AuthError, CypherError, IncompleteCommitError, \
    ConnectionExpired, DatabaseUnavailableError, NotALeaderError, \
    ForbiddenOnReadOnlyDatabaseError
from neo4j.bolt.meta import get_user_agent
from neo4j.bolt.security import make_ssl_context
from neo4j.data.packing import Packer, UnpackableBuffer, Unpacker


DEFAULT_PORT = 7687
MAGIC_PREAMBLE = 0x6060B017

# Connection Pool Management
DEFAULT_MAX_CONNECTION_LIFETIME = 3600  # 1h
DEFAULT_MAX_CONNECTION_POOL_SIZE = 100
DEFAULT_CONNECTION_TIMEOUT = 5.0  # 5s

DEFAULT_KEEP_ALIVE = True

# Connection Settings
DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = 60  # 1m


# Set up logger
log = getLogger("neobolt")


class AuthToken(object):
    """ Container for auth information
    """

    #: By default we should not send any realm
    realm = None

    def __init__(self, scheme, principal, credentials, realm=None, **parameters):
        self.scheme = scheme
        self.principal = principal
        self.credentials = credentials
        if realm:
            self.realm = realm
        if parameters:
            self.parameters = parameters


class ServerInfo(object):

    address = None

    def __init__(self, address, protocol_version):
        self.address = address
        self.protocol_version = protocol_version
        self.metadata = {}

    @property
    def agent(self):
        return self.metadata.get("server")

    def version_info(self):
        if not self.agent:
            return None
        _, _, value = self.agent.partition("/")
        value = value.replace("-", ".").split(".")
        for i, v in enumerate(value):
            try:
                value[i] = int(v)
            except ValueError:
                pass
        return tuple(value)


class Outbox(object):

    def __init__(self, capacity=8192, max_chunk_size=16384):
        self._max_chunk_size = max_chunk_size
        self._header = 0
        self._start = 2
        self._end = 2
        self._data = bytearray(capacity)

    def max_chunk_size(self):
        return self._max_chunk_size

    def clear(self):
        self._header = 0
        self._start = 2
        self._end = 2
        self._data[0:2] = b"\x00\x00"

    def write(self, b):
        to_write = len(b)
        max_chunk_size = self._max_chunk_size
        pos = 0
        while to_write > 0:
            chunk_size = self._end - self._start
            remaining = max_chunk_size - chunk_size
            if remaining == 0 or remaining < to_write <= max_chunk_size:
                self.chunk()
            else:
                wrote = min(to_write, remaining)
                new_end = self._end + wrote
                self._data[self._end:new_end] = b[pos:pos+wrote]
                self._end = new_end
                pos += wrote
                new_chunk_size = self._end - self._start
                self._data[self._header:(self._header + 2)] = struct_pack(">H", new_chunk_size)
                to_write -= wrote

    def chunk(self):
        self._header = self._end
        self._start = self._header + 2
        self._end = self._start
        self._data[self._header:self._start] = b"\x00\x00"

    def view(self):
        end = self._end
        chunk_size = end - self._start
        if chunk_size == 0:
            return memoryview(self._data[:self._header])
        else:
            return memoryview(self._data[:end])


class BufferedSocket(object):
    """ Wrapper for a regular socket, with an added a dynamically-resizing
    receive buffer to reduce the number of calls to recv.

    NOTE: not all socket methods are implemented yet
    """

    def __init__(self, socket_, initial_capacity=0):
        self.socket = socket_
        self.buffer = bytearray(initial_capacity)
        self.r_pos = 0
        self.w_pos = 0

    def _fill_buffer(self, min_bytes):
        """ Fill the buffer with at least `min_bytes` bytes, requesting more if
        the buffer has space. Internally, this method attempts to do as little
        allocation as possible and make as few calls to socket.recv as
        possible.
        """
        # First, we need to calculate how much spare space exists between the
        # write cursor and the end of the buffer.
        space_at_end = len(self.buffer) - self.w_pos
        if min_bytes <= space_at_end:
            # If there's at least enough here for the minimum number of bytes
            # we need, then do nothing
            #
            pass
        elif min_bytes <= space_at_end + self.r_pos:
            # If the buffer contains enough space, but it's split between the
            # end of the buffer and recyclable space at the start of the
            # buffer, then recycle that space by pushing the remaining data
            # towards the front.
            #
            # print("Recycling {} bytes".format(self.r_pos))
            size = self.w_pos - self.r_pos
            view = memoryview(self.buffer)
            self.buffer[0:size] = view[self.r_pos:self.w_pos]
            self.r_pos = 0
            self.w_pos = size
        else:
            # Otherwise, there's just not enough space whichever way you shake
            # it. So, rebuild the buffer from scratch, taking the unread data
            # and appending empty space big enough to hold the minimum number
            # of bytes we're looking for.
            #
            # print("Rebuilding buffer from {} bytes ({} used) to "
            #       "{} bytes".format(len(self.buffer),
            #                         self.w_pos - self.r_pos,
            #                         self.w_pos - self.r_pos + min_bytes))
            self.buffer = (self.buffer[self.r_pos:self.w_pos] +
                           bytearray(min_bytes))
            self.w_pos -= self.r_pos
            self.r_pos = 0
        min_end = self.w_pos + min_bytes
        end = len(self.buffer)
        view = memoryview(self.buffer)
        self.socket.setblocking(0)
        while self.w_pos < min_end:
            ready_to_read, _, _ = select([self.socket], [], [])
            subview = view[self.w_pos:end]
            n = self.socket.recv_into(subview, end - self.w_pos)
            if n == 0:
                raise OSError("No data")
            self.w_pos += n

    def recv_into(self, buffer, n_bytes=0, flags=0):
        """ Intercepts a regular socket.recv_into call, taking data from the
        internal buffer, if available. If not enough data exists in the buffer,
        more will be retrieved first.

        Unlike the lower-level call, this method will never return 0, instead
        raising an OSError if no data is returned on the underlying socket.

        :param buffer:
        :param n_bytes:
        :param flags:
        :raises OSError:
        :return:
        """
        available = self.w_pos - self.r_pos
        required = n_bytes - available
        if required > 0:
            self._fill_buffer(required)
        view = memoryview(self.buffer)
        end = self.r_pos + n_bytes
        buffer[:] = view[self.r_pos:end]
        self.r_pos = end
        return n_bytes


class Inbox(object):

    def __init__(self, s, on_error):
        super(Inbox, self).__init__()
        self.on_error = on_error
        self._messages = self._yield_messages(s)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._messages)

    @classmethod
    def _load_chunks(cls, sock, buffer):
        chunk_size = 0
        while True:
            if chunk_size == 0:
                buffer.receive(sock, 2)
            chunk_size = buffer.pop_u16()
            if chunk_size > 0:
                buffer.receive(sock, chunk_size + 2)
            yield chunk_size

    def _yield_messages(self, sock):
        try:
            buffer = UnpackableBuffer()
            chunk_loader = self._load_chunks(sock, buffer)
            unpacker = Unpacker(buffer)
            details = []
            while True:
                unpacker.reset()
                details[:] = ()
                chunk_size = -1
                while chunk_size != 0:
                    chunk_size = next(chunk_loader)
                summary_signature = None
                summary_metadata = None
                size, signature = unpacker.unpack_structure_header()
                if size > 1:
                    raise ProtocolError("Expected one field")
                if signature == b"\x71":
                    data = unpacker.unpack()
                    details.append(data)
                else:
                    summary_signature = signature
                    summary_metadata = unpacker.unpack_map()
                yield details, summary_signature, summary_metadata
        except OSError as error:
            self.on_error(error)


class Connection(object):
    """ Server connection for Bolt protocol v1.

    A :class:`.Connection` should be constructed following a
    successful Bolt handshake and takes the socket over which
    the handshake was carried out.

    .. note:: logs at INFO level
    """

    #: The protocol version in use on this connection
    protocol_version = 0

    #: Server details for this connection
    server = None

    in_use = False

    _closed = False

    _defunct = False

    #: The pool of which this connection is a member
    pool = None

    #: Error class used for raising connection errors
    # TODO: separate errors for connector API
    Error = ServiceUnavailable

    def __init__(self, protocol_version, unresolved_address, sock, **config):
        self.protocol_version = protocol_version
        self.unresolved_address = unresolved_address
        self.socket = sock
        self.server = ServerInfo(SocketAddress.from_socket(sock), protocol_version)
        self.outbox = Outbox()
        self.inbox = Inbox(BufferedSocket(self.socket, 32768), on_error=self._set_defunct)
        self.packer = Packer(self.outbox)
        self.unpacker = Unpacker(self.inbox)
        self.responses = deque()
        self._max_connection_lifetime = config.get("max_connection_lifetime", DEFAULT_MAX_CONNECTION_LIFETIME)
        self._creation_timestamp = perf_counter()

        # Determine the user agent and ensure it is a Unicode value
        user_agent = config.get("user_agent", get_user_agent())
        if isinstance(user_agent, bytes):
            user_agent = user_agent.decode("UTF-8")
        self.user_agent = user_agent

        # Determine auth details
        auth = config.get("auth")
        if not auth:
            self.auth_dict = {}
        elif isinstance(auth, tuple) and 2 <= len(auth) <= 3:
            self.auth_dict = vars(AuthToken("basic", *auth))
        else:
            try:
                self.auth_dict = vars(auth)
            except (KeyError, TypeError):
                raise AuthError("Cannot determine auth details from %r" % auth)

        # Check for missing password
        try:
            credentials = self.auth_dict["credentials"]
        except KeyError:
            pass
        else:
            if credentials is None:
                raise AuthError("Password cannot be None")

        # Pick up the server certificate, if any
        self.der_encoded_server_certificate = config.get("der_encoded_server_certificate")

    @property
    def secure(self):
        return isinstance(self.socket, SSLSocket)

    @property
    def local_port(self):
        try:
            return self.socket.getsockname()[1]
        except IOError:
            return 0

    def hello(self):
        headers = {"user_agent": self.user_agent}
        headers.update(self.auth_dict)
        logged_headers = dict(headers)
        if "credentials" in logged_headers:
            logged_headers["credentials"] = "*******"
        log.debug("[#%04X]  C: HELLO %r", self.local_port, logged_headers)
        self._append(b"\x01", (headers,),
                     response=InitResponse(self, on_success=self.server.metadata.update))
        self.send_all()
        self.fetch_all()

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, statement, parameters=None, mode=None, bookmarks=None, metadata=None, timeout=None, **handlers):
        if not parameters:
            parameters = {}
        extra = {}
        if mode:
            extra["mode"] = mode
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided within an iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout:
            try:
                extra["tx_timeout"] = int(1000 * timeout)
            except TypeError:
                raise TypeError("Timeout must be specified as a number of seconds")
        fields = (statement, parameters, extra)
        log.debug("[#%04X]  C: RUN %s", self.local_port, " ".join(map(repr, fields)))
        if statement.upper() == u"COMMIT":
            self._append(b"\x10", fields, CommitResponse(self, **handlers))
        else:
            self._append(b"\x10", fields, Response(self, **handlers))

    def discard_all(self, **handlers):
        log.debug("[#%04X]  C: DISCARD_ALL", self.local_port)
        self._append(b"\x2F", (), Response(self, **handlers))

    def pull_all(self, **handlers):
        log.debug("[#%04X]  C: PULL_ALL", self.local_port)
        self._append(b"\x3F", (), Response(self, **handlers))

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None, **handlers):
        extra = {}
        if mode:
            extra["mode"] = mode
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided within an iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout:
            try:
                extra["tx_timeout"] = int(1000 * timeout)
            except TypeError:
                raise TypeError("Timeout must be specified as a number of seconds")
        log.debug("[#%04X]  C: BEGIN %r", self.local_port, extra)
        self._append(b"\x11", (extra,), Response(self, **handlers))

    def commit(self, **handlers):
        log.debug("[#%04X]  C: COMMIT", self.local_port)
        self._append(b"\x12", (), CommitResponse(self, **handlers))

    def rollback(self, **handlers):
        log.debug("[#%04X]  C: ROLLBACK", self.local_port)
        self._append(b"\x13", (), Response(self, **handlers))

    def _append(self, signature, fields=(), response=None):
        """ Add a message to the outgoing queue.

        :arg signature: the signature of the message
        :arg fields: the fields of the message as a tuple
        :arg response: a response object to handle callbacks
        """
        self.packer.pack_struct(signature, fields)
        self.outbox.chunk()
        self.outbox.chunk()
        self.responses.append(response)

    def reset(self):
        """ Add a RESET message to the outgoing queue, send
        it and consume all remaining messages.
        """

        def fail(metadata):
            raise ProtocolError("RESET failed %r" % metadata)

        log.debug("[#%04X]  C: RESET", self.local_port)
        self._append(b"\x0F", response=Response(self, on_failure=fail))
        self.send_all()
        self.fetch_all()

    def _send_all(self):
        data = self.outbox.view()
        if data:
            self.socket.sendall(data)
            self.outbox.clear()

    def send_all(self):
        """ Send all queued messages to the server.
        """
        if self.closed():
            raise self.Error("Failed to write to closed connection "
                             "{!r} ({!r})".format(self.unresolved_address,
                                                  self.server.address))
        if self.defunct():
            raise self.Error("Failed to write to defunct connection "
                             "{!r} ({!r})".format(self.unresolved_address,
                                                  self.server.address))
        try:
            self._send_all()
        except (IOError, OSError) as error:
            log.error("Failed to write data to connection "
                      "{!r} ({!r}); ({!r})".
                      format(self.unresolved_address,
                             self.server.address,
                             "; ".join(map(repr, error.args))))
            if self.pool:
                self.pool.deactivate(self.unresolved_address)
            raise

    def fetch_message(self):
        """ Receive at least one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        if self._closed:
            raise self.Error("Failed to read from closed connection "
                             "{!r} ({!r})".format(self.unresolved_address,
                                                  self.server.address))
        if self._defunct:
            raise self.Error("Failed to read from defunct connection "
                             "{!r} ({!r})".format(self.unresolved_address,
                                                  self.server.address))
        if not self.responses:
            return 0, 0

        # Receive exactly one message
        try:
            details, summary_signature, summary_metadata = next(self.inbox)
        except (IOError, OSError) as error:
            log.error("Failed to read data from connection "
                      "{!r} ({!r}); ({!r})".
                      format(self.unresolved_address,
                             self.server.address,
                             "; ".join(map(repr, error.args))))
            if self.pool:
                self.pool.deactivate(self.unresolved_address)
            raise

        if details:
            log.debug("[#%04X]  S: RECORD * %d", self.local_port, len(details))  # TODO
            self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == b"\x70":
            log.debug("[#%04X]  S: SUCCESS %r", self.local_port, summary_metadata)
            response.on_success(summary_metadata or {})
        elif summary_signature == b"\x7E":
            log.debug("[#%04X]  S: IGNORED", self.local_port)
            response.on_ignored(summary_metadata or {})
        elif summary_signature == b"\x7F":
            log.debug("[#%04X]  S: FAILURE %r", self.local_port, summary_metadata)
            try:
                response.on_failure(summary_metadata or {})
            except (ConnectionExpired, ServiceUnavailable, DatabaseUnavailableError):
                if self.pool:
                    self.pool.deactivate(self.unresolved_address),
                raise
            except (NotALeaderError, ForbiddenOnReadOnlyDatabaseError):
                if self.pool:
                    self.pool.remove_writer(self.unresolved_address),
                raise
        else:
            raise ProtocolError("Unexpected response message with "
                                "signature %02X" % summary_signature)

        return len(details), 1

    def _set_defunct(self, error=None):
        message = ("Failed to read from defunct connection " 
                   "{!r} ({!r})".format(self.unresolved_address,
                                        self.server.address))
        log.error(message)
        # We were attempting to receive data but the connection
        # has unexpectedly terminated. So, we need to close the
        # connection from the client side, and remove the address
        # from the connection pool.
        self._defunct = True
        self.close()
        if self.pool:
            self.pool.deactivate(self.unresolved_address)
        # Iterate through the outstanding responses, and if any correspond
        # to COMMIT requests then raise an error to signal that we are
        # unable to confirm that the COMMIT completed successfully.
        for response in self.responses:
            if isinstance(response, CommitResponse):
                raise IncompleteCommitError(message)
        raise self.Error(message)

    def timedout(self):
        return 0 <= self._max_connection_lifetime <= perf_counter() - self._creation_timestamp

    def fetch_all(self):
        """ Fetch all outstanding messages.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        detail_count = summary_count = 0
        while self.responses:
            response = self.responses[0]
            while not response.complete:
                detail_delta, summary_delta = self.fetch_message()
                detail_count += detail_delta
                summary_count += summary_delta
        return detail_count, summary_count

    def close(self):
        """ Close the connection.
        """
        if not self._closed:
            if not self._defunct:
                log.debug("[#%04X]  C: GOODBYE", self.local_port)
                self._append(b"\x02", ())
                try:
                    self._send_all()
                except:
                    pass
            log.debug("[#%04X]  C: <CLOSE>", self.local_port)
            try:
                self.socket.close()
            except IOError:
                pass
            finally:
                self._closed = True

    def closed(self):
        return self._closed

    def defunct(self):
        return self._defunct


class AbstractConnectionPool(object):
    """ A collection of connections to one or more server addresses.
    """

    _closed = False

    def __init__(self, connector, **config):
        self.connector = connector
        self.connections = {}
        self.lock = RLock()
        self.cond = Condition(self.lock)
        self._max_connection_pool_size = config.get("max_connection_pool_size", DEFAULT_MAX_CONNECTION_POOL_SIZE)
        self._connection_acquisition_timeout = config.get("connection_acquisition_timeout", DEFAULT_CONNECTION_ACQUISITION_TIMEOUT)

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
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:
                connections = self.connections[address] = deque()

            connection_acquisition_start_timestamp = perf_counter()
            while True:
                # try to find a free connection in pool
                for connection in list(connections):
                    if connection.closed() or connection.defunct() or connection.timedout():
                        connections.remove(connection)
                        continue
                    if not connection.in_use:
                        connection.in_use = True
                        return connection
                # all connections in pool are in-use
                infinite_connection_pool = (self._max_connection_pool_size < 0 or
                                            self._max_connection_pool_size == float("inf"))
                can_create_new_connection = infinite_connection_pool or len(connections) < self._max_connection_pool_size
                if can_create_new_connection:
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

                # failed to obtain a connection from pool because the pool is full and no free connection in the pool
                span_timeout = self._connection_acquisition_timeout - (perf_counter() - connection_acquisition_start_timestamp)
                if span_timeout > 0:
                    self.cond.wait(span_timeout)
                    # if timed out, then we throw error. This time computation is needed, as with python 2.7, we cannot
                    # tell if the condition is notified or timed out when we come to this line
                    if self._connection_acquisition_timeout <= (perf_counter() - connection_acquisition_start_timestamp):
                        raise ClientError("Failed to obtain a connection from pool within {!r}s".format(
                            self._connection_acquisition_timeout))
                else:
                    raise ClientError("Failed to obtain a connection from pool within {!r}s".format(self._connection_acquisition_timeout))

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
            self.cond.notify_all()

    def in_use_connection_count(self, address):
        """ Count the number of connections currently in use to a given
        address.
        """
        try:
            connections = self.connections[address]
        except KeyError:
            return 0
        else:
            return sum(1 if connection.in_use else 0 for connection in connections)

    def deactivate(self, address):
        """ Deactivate an address from the connection pool, if present, closing
        all idle connection to that address
        """
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError: # already removed from the connection pool
                return
            for conn in list(connections):
                if not conn.in_use:
                    connections.remove(conn)
                    try:
                        conn.close()
                    except IOError:
                        pass
            if not connections:
                self.remove(address)

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
        if self._closed:
            return
        try:
            with self.lock:
                if not self._closed:
                    self._closed = True
                    for address in list(self.connections):
                        self.remove(address)
        except TypeError as e:
            pass

    def closed(self):
        """ Return :const:`True` if this pool is closed, :const:`False`
        otherwise.
        """
        with self.lock:
            return self._closed


class ConnectionPool(AbstractConnectionPool):

    def __init__(self, connector, address, **config):
        super(ConnectionPool, self).__init__(connector, **config)
        self.address = address

    def acquire(self, access_mode=None):
        return self.acquire_direct(self.address)


class Response(object):
    """ Subscriber object for a full response (zero or
    more detail messages followed by one summary message).
    """

    def __init__(self, connection, **handlers):
        self.connection = connection
        self.handlers = handlers
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
        handler = self.handlers.get("on_summary")
        if callable(handler):
            handler()

    def on_failure(self, metadata):
        """ Called when a FAILURE message has been received.
        """
        self.connection.reset()
        handler = self.handlers.get("on_failure")
        if callable(handler):
            handler(metadata)
        handler = self.handlers.get("on_summary")
        if callable(handler):
            handler()
        raise CypherError.hydrate(**metadata)

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
        message = metadata.get("message", "Connection initialisation failed")
        if code == "Neo.ClientError.Security.Unauthorized":
            raise AuthError(message)
        else:
            raise ServiceUnavailable(message)


class CommitResponse(Response):

    pass


# TODO: remove in 2.0
def _last_bookmark(b0, b1):
    """ Return the latest of two bookmarks by looking for the maximum
    integer value following the last colon in the bookmark string.
    """
    n = [None, None]
    _, _, n[0] = b0.rpartition(":")
    _, _, n[1] = b1.rpartition(":")
    for i in range(2):
        try:
            n[i] = int(n[i])
        except ValueError:
            raise ValueError("Invalid bookmark: {}".format(b0))
    return b0 if n[0] > n[1] else b1


# TODO: remove in 2.0
def last_bookmark(bookmarks):
    """ The bookmark returned by the last :class:`.Transaction`.
    """
    last = None
    for bookmark in bookmarks:
        if last is None:
            last = bookmark
        else:
            last = _last_bookmark(last, bookmark)
    return last


def _connect(resolved_address, **config):
    """

    :param resolved_address:
    :param config:
    :return: socket object
    """
    s = None
    try:
        if len(resolved_address) == 2:
            s = socket(AF_INET)
        elif len(resolved_address) == 4:
            s = socket(AF_INET6)
        else:
            raise ValueError("Unsupported address "
                             "{!r}".format(resolved_address))
        t = s.gettimeout()
        s.settimeout(config.get("connection_timeout",
                                DEFAULT_CONNECTION_TIMEOUT))
        log.debug("[#0000]  C: <OPEN> %s", resolved_address)
        s.connect(resolved_address)
        s.settimeout(t)
        keep_alive = 1 if config.get("keep_alive", DEFAULT_KEEP_ALIVE) else 0
        s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, keep_alive)
    except SocketTimeout:
        log.debug("[#0000]  C: <TIMEOUT> %s", resolved_address)
        log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
        s.close()
        raise ServiceUnavailable("Timed out trying to establish connection "
                                 "to {!r}".format(resolved_address))
    except OSError as error:
        log.debug("[#0000]  C: <ERROR> %s %s", type(error).__name__,
                  " ".join(map(repr, error.args)))
        log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
        s.close()
        raise ServiceUnavailable("Failed to establish connection to {!r} "
                                 "(reason {})".format(resolved_address, error))
    else:
        return s


def _secure(s, host, ssl_context):
    local_port = s.getsockname()[1]
    # Secure the connection if an SSL context has been provided
    if ssl_context:
        log.debug("[#%04X]  C: <SECURE> %s", local_port, host)
        try:
            sni_host = host if HAS_SNI and host else None
            s = ssl_context.wrap_socket(s, server_hostname=sni_host)
        except SSLError as cause:
            s.close()
            error = SecurityError("Failed to establish secure connection "
                                  "to {!r}".format(cause.args[1]))
            error.__cause__ = cause
            raise error
        else:
            # Check that the server provides a certificate
            der_encoded_server_certificate = s.getpeercert(binary_form=True)
            if der_encoded_server_certificate is None:
                s.close()
                raise ProtocolError("When using a secure socket, the server "
                                    "should always provide a certificate")
    else:
        der_encoded_server_certificate = None
    return s, der_encoded_server_certificate


def _handshake(s, resolved_address, der_encoded_server_certificate, **config):
    """

    :param s:
    :return:
    """
    local_port = s.getsockname()[1]

    # Send details of the protocol versions supported
    supported_versions = [3, 0, 0, 0]
    handshake = [MAGIC_PREAMBLE] + supported_versions
    log.debug("[#%04X]  C: <MAGIC> 0x%08X", local_port, MAGIC_PREAMBLE)
    log.debug("[#%04X]  C: <HANDSHAKE> 0x%08X 0x%08X 0x%08X 0x%08X",
              local_port, *supported_versions)
    data = b"".join(struct_pack(">I", num) for num in handshake)
    s.sendall(data)

    # Handle the handshake response
    ready_to_read = False
    while not ready_to_read:
        ready_to_read, _, _ = select((s,), (), (), 1)
    try:
        data = s.recv(4)
    except OSError:
        raise ServiceUnavailable("Failed to read any data from server {!r} "
                                 "after connected".format(resolved_address))
    data_size = len(data)
    if data_size == 0:
        # If no data is returned after a successful select
        # response, the server has closed the connection
        log.debug("[#%04X]  S: <CLOSE>", local_port)
        s.close()
        raise ServiceUnavailable("Connection to %r closed without handshake "
                                 "response" % (resolved_address,))
    if data_size != 4:
        # Some garbled data has been received
        log.debug("[#%04X]  S: @*#!", local_port)
        s.close()
        raise ProtocolError("Expected four byte Bolt handshake response "
                            "from %r, received %r instead; check for "
                            "incorrect port number" % (resolved_address, data))
    agreed_version, = struct_unpack(">I", data)
    log.debug("[#%04X]  S: <HANDSHAKE> 0x%08X", local_port, agreed_version)
    if agreed_version == 0:
        log.debug("[#%04X]  C: <CLOSE>", local_port)
        s.shutdown(SHUT_RDWR)
        s.close()
    elif agreed_version in (3,):
        connection = Connection(
            agreed_version, resolved_address, s,
            der_encoded_server_certificate=der_encoded_server_certificate,
            **config)
        connection.hello()
        return connection
    elif agreed_version == 0x48545450:
        log.debug("[#%04X]  S: <CLOSE>", local_port)
        s.close()
        raise ServiceUnavailable("Cannot to connect to Bolt service on {!r} "
                                 "(looks like HTTP)".format(resolved_address))
    else:
        log.debug("[#%04X]  S: <CLOSE>", local_port)
        s.close()
        raise ProtocolError("Unknown Bolt protocol version: "
                            "{}".format(agreed_version))


def connect(address, **config):
    """ Connect and perform a handshake and return a valid Connection object,
    assuming a protocol version can be agreed.
    """
    ssl_context = make_ssl_context(**config)
    last_error = None
    # Establish a connection to the host and port specified
    # Catches refused connections see:
    # https://docs.python.org/2/library/errno.html
    log.debug("[#0000]  C: <RESOLVE> %s", address)
    resolver = Resolver(custom_resolver=config.get("resolver"))
    resolver.addresses.append(address)
    resolver.custom_resolve()
    resolver.dns_resolve()
    for resolved_address in resolver.addresses:
        s = None
        try:
            host = address[0]
            s = _connect(resolved_address, **config)
            s, der_encoded_server_certificate = _secure(s, host, ssl_context)
            connection = _handshake(s, address, der_encoded_server_certificate,
                                    **config)
        except Exception as error:
            if s:
                s.close()
            last_error = error
        else:
            return connection
    if last_error is None:
        raise ServiceUnavailable("Failed to resolve addresses for %s" % address)
    else:
        raise last_error
