#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


"""
This module contains the low-level functionality required for speaking
Bolt. It is not intended to be used directly by driver users. Instead,
the `session` module provides the main user-facing abstractions.
"""


__all__ = [
    "Bolt",
    "BoltPool",
    "ConnectionErrorHandler",
    "Neo4jPool",
    "check_supported_server_product",
]

import abc
from collections import (
    defaultdict,
    deque,
)
from contextlib import contextmanager
import logging
from logging import getLogger
from random import choice
from threading import (
    Condition,
    RLock,
)
from time import perf_counter

from neo4j._exceptions import (
    BoltError,
    BoltHandshakeError,
)
from neo4j._deadline import (
    connection_deadline,
    Deadline,
    merge_deadlines,
    merge_deadlines_and_timeouts,
)
from neo4j.addressing import Address
from neo4j.api import (
    READ_ACCESS,
    ServerInfo,
    Version,
    WRITE_ACCESS,
)
from neo4j.conf import (
    PoolConfig,
    WorkspaceConfig,
)
from neo4j.exceptions import (
    AuthError,
    ClientError,
    ConfigurationError,
    DriverError,
    IncompleteCommit,
    Neo4jError,
    ReadServiceUnavailable,
    ServiceUnavailable,
    SessionExpired,
    UnsupportedServerProduct,
    WriteServiceUnavailable,
)
from neo4j.io._common import (
    CommitResponse,
    ConnectionErrorHandler,
    Inbox,
    InitResponse,
    Outbox,
    Response,
)
from neo4j.io._socket import BoltSocket
from neo4j.meta import get_user_agent
from neo4j.packstream import (
    Packer,
    Unpacker,
)
from neo4j.routing import RoutingTable

# Set up logger
log = getLogger("neo4j")


class Bolt(abc.ABC):
    """ Server connection for Bolt protocol.

    A :class:`.Bolt` should be constructed following a
    successful .open()

    Bolt handshake and takes the socket over which
    the handshake was carried out.
    """

    MAGIC_PREAMBLE = b"\x60\x60\xB0\x17"

    PROTOCOL_VERSION = None

    # flag if connection needs RESET to go back to READY state
    is_reset = False

    # The socket
    in_use = False

    # The socket
    _closing = False
    _closed = False

    # The socket
    _defunct = False

    #: The pool of which this connection is a member
    pool = None

    # Store the id of the most recent ran query to be able to reduce sent bits by
    # using the default (-1) to refer to the most recent query when pulling
    # results for it.
    most_recent_qid = None

    def __init__(self, unresolved_address, sock, max_connection_lifetime, *, auth=None, user_agent=None, routing_context=None):
        self.unresolved_address = unresolved_address
        self.socket = sock
        self.local_port = self.socket.getsockname()[1]
        self.server_info = ServerInfo(Address(sock.getpeername()), self.PROTOCOL_VERSION)
        # so far `connection.recv_timeout_seconds` is the only available
        # configuration hint that exists. Therefore, all hints can be stored at
        # connection level. This might change in the future.
        self.configuration_hints = {}
        # back ported protocol patches negotiated with the server
        self.bolt_patches = set()
        self.outbox = Outbox()
        self.inbox = Inbox(self.socket, on_error=self._set_defunct_read)
        self.packer = Packer(self.outbox)
        self.unpacker = Unpacker(self.inbox)
        self.responses = deque()
        self._max_connection_lifetime = max_connection_lifetime
        self._creation_timestamp = perf_counter()
        self.routing_context = routing_context

        # Determine the user agent
        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = get_user_agent()

        # Determine auth details
        if not auth:
            self.auth_dict = {}
        elif isinstance(auth, tuple) and 2 <= len(auth) <= 3:
            from neo4j import Auth
            self.auth_dict = vars(Auth("basic", *auth))
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

    @property
    @abc.abstractmethod
    def supports_multiple_results(self):
        """ Boolean flag to indicate if the connection version supports multiple
        queries to be buffered on the server side (True) or if all results need
        to be eagerly pulled before sending the next RUN (False).
        """
        pass

    @property
    @abc.abstractmethod
    def supports_multiple_databases(self):
        """ Boolean flag to indicate if the connection version supports multiple
        databases.
        """
        pass

    @classmethod
    def protocol_handlers(cls, protocol_version=None):
        """ Return a dictionary of available Bolt protocol handlers,
        keyed by version tuple. If an explicit protocol version is
        provided, the dictionary will contain either zero or one items,
        depending on whether that version is supported. If no protocol
        version is provided, all available versions will be returned.

        :param protocol_version: tuple identifying a specific protocol
            version (e.g. (3, 5)) or None
        :return: dictionary of version tuple to handler class for all
            relevant and supported protocol versions
        :raise TypeError: if protocol version is not passed in a tuple
        """

        # Carry out Bolt subclass imports locally to avoid circular dependency issues.
        from neo4j.io._bolt3 import Bolt3
        from neo4j.io._bolt4 import Bolt4x0, Bolt4x1, Bolt4x2, Bolt4x3, Bolt4x4

        handlers = {
            Bolt3.PROTOCOL_VERSION: Bolt3,
            Bolt4x0.PROTOCOL_VERSION: Bolt4x0,
            Bolt4x1.PROTOCOL_VERSION: Bolt4x1,
            Bolt4x2.PROTOCOL_VERSION: Bolt4x2,
            Bolt4x3.PROTOCOL_VERSION: Bolt4x3,
            Bolt4x4.PROTOCOL_VERSION: Bolt4x4,
        }

        if protocol_version is None:
            return handlers

        if not isinstance(protocol_version, tuple):
            raise TypeError("Protocol version must be specified as a tuple")

        if protocol_version in handlers:
            return {protocol_version: handlers[protocol_version]}

        return {}

    @classmethod
    def version_list(cls, versions, limit=4):
        """ Return a list of supported protocol versions in order of
        preference. The number of protocol versions (or ranges)
        returned is limited to four.
        """
        # In fact, 4.3 is the fist version to support ranges. However, the range
        # support got backported to 4.2. But even if the server is too old to
        # have the backport, negotiating BOLT 4.1 is no problem as it's
        # equivalent to 4.2
        first_with_range_support = Version(4, 2)
        result = []
        for version in versions:
            if (result
                    and version >= first_with_range_support
                    and result[-1][0] == version[0]
                    and result[-1][1][1] == version[1] + 1):
                # can use range to encompass this version
                result[-1][1][1] = version[1]
                continue
            result.append(Version(version[0], [version[1], version[1]]))
            if len(result) == 4:
                break
        return result

    @classmethod
    def get_handshake(cls):
        """ Return the supported Bolt versions as bytes.
        The length is 16 bytes as specified in the Bolt version negotiation.
        :return: bytes
        """
        supported_versions = sorted(cls.protocol_handlers().keys(), reverse=True)
        offered_versions = cls.version_list(supported_versions)
        return b"".join(version.to_bytes() for version in offered_versions).ljust(16, b"\x00")

    @classmethod
    def ping(cls, address, *, timeout=None, **config):
        """ Attempt to establish a Bolt connection, returning the
        agreed Bolt protocol version if successful.
        """
        config = PoolConfig.consume(config)
        try:
            s, protocol_version, handshake, data = BoltSocket.connect(
                address,
                timeout=timeout,
                custom_resolver=config.resolver,
                ssl_context=config.get_ssl_context(),
                keep_alive=config.keep_alive,
            )
        except (ServiceUnavailable, SessionExpired, BoltHandshakeError):
            return None
        else:
            BoltSocket.close_socket(s)
            return protocol_version

    @classmethod
    def open(cls, address, *, auth=None, timeout=None, routing_context=None, **pool_config):
        """ Open a new Bolt connection to a given server address.

        :param address:
        :param auth:
        :param timeout: the connection timeout in seconds
        :param routing_context: dict containing routing context
        :param pool_config:
        :return:
        :raise BoltHandshakeError: raised if the Bolt Protocol can not negotiate a protocol version.
        :raise ServiceUnavailable: raised if there was a connection issue.
        """
        def time_remaining():
            if timeout is None:
                return None
            t = timeout - (perf_counter() - t0)
            return t if t > 0 else 0

        t0 = perf_counter()
        pool_config = PoolConfig.consume(pool_config)

        socket_connection_timeout = pool_config.connection_timeout
        if socket_connection_timeout is None:
            socket_connection_timeout = time_remaining()
        elif timeout is not None:
            socket_connection_timeout = min(pool_config.connection_timeout,
                                            time_remaining())
        s, pool_config.protocol_version, handshake, data = BoltSocket.connect(
            address,
            timeout=socket_connection_timeout,
            custom_resolver=pool_config.resolver,
            ssl_context=pool_config.get_ssl_context(),
            keep_alive=pool_config.keep_alive,
        )

        # Carry out Bolt subclass imports locally to avoid circular dependency
        # issues.
        if pool_config.protocol_version == (3, 0):
            from neo4j.io._bolt3 import Bolt3
            bolt_cls = Bolt3
        elif pool_config.protocol_version == (4, 0):
            from neo4j.io._bolt4 import Bolt4x0
            bolt_cls = Bolt4x0
        elif pool_config.protocol_version == (4, 1):
            from neo4j.io._bolt4 import Bolt4x1
            bolt_cls = Bolt4x1
        elif pool_config.protocol_version == (4, 2):
            from neo4j.io._bolt4 import Bolt4x2
            bolt_cls = Bolt4x2
        elif pool_config.protocol_version == (4, 3):
            from neo4j.io._bolt4 import Bolt4x3
            bolt_cls = Bolt4x3
        elif pool_config.protocol_version == (4, 4):
            from neo4j.io._bolt4 import Bolt4x4
            bolt_cls = Bolt4x4
        else:
            log.debug("[#%04X]  S: <CLOSE>", s.getsockname()[1])
            BoltSocket.close_socket(s)

            supported_versions = Bolt.protocol_handlers().keys()
            raise BoltHandshakeError("The Neo4J server does not support communication with this driver. This driver have support for Bolt Protocols {}".format(supported_versions), address=address, request_data=handshake, response_data=data)

        connection = bolt_cls(
            address, s, pool_config.max_connection_lifetime, auth=auth,
            user_agent=pool_config.user_agent, routing_context=routing_context
        )

        try:
            connection.socket.set_deadline(time_remaining())
            try:
                connection.hello()
            finally:
                connection.socket.set_deadline(None)
        except Exception:
            connection.close_non_blocking()
            raise

        return connection

    @property
    @abc.abstractmethod
    def encrypted(self):
        pass

    @property
    @abc.abstractmethod
    def der_encoded_server_certificate(self):
        pass

    @abc.abstractmethod
    def hello(self):
        """ Appends a HELLO message to the outgoing queue, sends it and consumes
         all remaining messages.
        """
        pass

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    @abc.abstractmethod
    def route(self, database=None, imp_user=None, bookmarks=None):
        """ Fetch a routing table from the server for the given
        `database`. For Bolt 4.3 and above, this appends a ROUTE
        message; for earlier versions, a procedure call is made via
        the regular Cypher execution mechanism. In all cases, this is
        sent to the network, and a response is fetched.

        :param database: database for which to fetch a routing table
        :param imp_user: the user to impersonate
        :param bookmarks: iterable of bookmark values after which this
                          transaction should begin
        :return: dictionary of raw routing data
        """
        pass

    @abc.abstractmethod
    def run(self, query, parameters=None, mode=None, bookmarks=None,
            metadata=None, timeout=None, db=None, imp_user=None, **handlers):
        """ Appends a RUN message to the output queue.

        :param query: Cypher query string
        :param parameters: dictionary of Cypher parameters
        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this transaction should begin
        :param metadata: custom metadata dictionary to attach to the transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
        :param imp_user: the user to impersonate
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def discard(self, n=-1, qid=-1, **handlers):
        """ Appends a DISCARD message to the output queue.

        :param n: number of records to discard, default = -1 (ALL)
        :param qid: query ID to discard for, default = -1 (last query)
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def pull(self, n=-1, qid=-1, **handlers):
        """ Appends a PULL message to the output queue.

        :param n: number of records to pull, default = -1 (ALL)
        :param qid: query ID to pull for, default = -1 (last query)
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None,
              db=None, imp_user=None, **handlers):
        """ Appends a BEGIN message to the output queue.

        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this transaction should begin
        :param metadata: custom metadata dictionary to attach to the transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
        :param imp_user: the user to impersonate
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def commit(self, **handlers):
        """ Appends a COMMIT message to the output queue."""
        pass

    @abc.abstractmethod
    def rollback(self, **handlers):
        """ Appends a ROLLBACK message to the output queue."""
        pass

    @abc.abstractmethod
    def reset(self):
        """ Appends a RESET message to the outgoing queue, sends it and consumes
         all remaining messages.
        """
        pass

    @abc.abstractmethod
    def goodbye(self):
        """Append a GOODBYE message to the outgoing queue."""
        pass

    def _append(self, signature, fields=(), response=None):
        """ Appends a message to the outgoing queue.

        :param signature: the signature of the message
        :param fields: the fields of the message as a tuple
        :param response: a response object to handle callbacks
        """
        with self.outbox.tmp_buffer():
            self.packer.pack_struct(signature, fields)
        self.outbox.wrap_message()
        self.responses.append(response)

    def _send_all(self):
        data = self.outbox.view()
        if data:
            try:
                self.socket.sendall(data)
            except OSError as error:
                self._set_defunct_write(error)
            self.outbox.clear()

    def send_all(self):
        """ Send all queued messages to the server.
        """
        if self.closed():
            raise ServiceUnavailable("Failed to write to closed connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        if self.defunct():
            raise ServiceUnavailable("Failed to write to defunct connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        self._send_all()

    @abc.abstractmethod
    def fetch_message(self):
        """ Receive at most one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        pass

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

    def _set_defunct_read(self, error=None, silent=False):
        message = "Failed to read from defunct connection {!r} ({!r})".format(
            self.unresolved_address, self.server_info.address
        )
        self._set_defunct(message, error=error, silent=silent)

    def _set_defunct_write(self, error=None, silent=False):
        message = "Failed to write data to connection {!r} ({!r})".format(
            self.unresolved_address, self.server_info.address
        )
        self._set_defunct(message, error=error, silent=silent)

    def _set_defunct(self, message, error=None, silent=False):
        direct_driver = isinstance(self.pool, BoltPool)

        if error:
            log.debug("[#%04X]  %r", self.socket.getsockname()[1], error)
        log.error(message)
        # We were attempting to receive data but the connection
        # has unexpectedly terminated. So, we need to close the
        # connection from the client side, and remove the address
        # from the connection pool.
        self._defunct = True
        if not self._closing:
            # If we fail while closing the connection, there is no need to
            # remove the connection from the pool, nor to try to close the
            # connection again.
            self.close()
            if self.pool:
                self.pool.deactivate(address=self.unresolved_address)
        # Iterate through the outstanding responses, and if any correspond
        # to COMMIT requests then raise an error to signal that we are
        # unable to confirm that the COMMIT completed successfully.
        if silent:
            return
        for response in self.responses:
            if isinstance(response, CommitResponse):
                if error:
                    raise IncompleteCommit(message) from error
                else:
                    raise IncompleteCommit(message)

        if direct_driver:
            if error:
                raise ServiceUnavailable(message) from error
            else:
                raise ServiceUnavailable(message)
        else:
            if error:
                raise SessionExpired(message) from error
            else:
                raise SessionExpired(message)

    def stale(self):
        return (self._stale
                or (0 <= self._max_connection_lifetime
                    <= perf_counter() - self._creation_timestamp))

    _stale = False

    def set_stale(self):
        self._stale = True

    def close(self):
        """Close the connection."""
        if self._closed or self._closing:
            return
        self._closing = True
        if not self._defunct:
            self.goodbye()
            try:
                self._send_all()
            except (OSError, BoltError, DriverError):
                pass
        log.debug("[#%04X]  C: <CLOSE>", self.local_port)
        try:
            self.socket.close()
        except OSError:
            pass
        finally:
            self._closed = True

    async def close_non_blocking(self):
        """Set the socket to non-blocking and close it.
        This will try to send the `GOODBYE` message (given the socket is not
        marked as defunct). However, should the write operation require
        blocking (e.g., a full network buffer), then the socket will be closed
        immediately (without `GOODBYE` message).
        """
        if self._closed or self._closing:
            return
        self.socket.settimeout(0)
        self.close()

    @abc.abstractmethod
    def closed(self):
        pass

    @abc.abstractmethod
    def defunct(self):
        pass


class IOPool:
    """ A collection of connections to one or more server addresses.
    """

    def __init__(self, opener, pool_config, workspace_config):
        assert callable(opener)
        assert isinstance(pool_config, PoolConfig)
        assert isinstance(workspace_config, WorkspaceConfig)

        self.opener = opener
        self.pool_config = pool_config
        self.workspace_config = workspace_config
        self.connections = defaultdict(deque)
        self.connections_reservations = defaultdict(lambda: 0)
        self.lock = RLock()
        self.cond = Condition(self.lock)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _acquire_from_pool(self, address):
        with self.lock:
            for connection in list(self.connections.get(address, [])):
                if connection.in_use:
                    continue
                connection.pool = self
                connection.in_use = True
                return connection
        return None  # no free connection available

    def _acquire_from_pool_checked(
        self, address, health_check, deadline
    ):
        while not deadline.expired():
            connection = self._acquire_from_pool(address)
            if not connection:
                return None  # no free connection available
            if not health_check(connection, deadline):
                # `close` is a noop on already closed connections.
                # This is to make sure that the connection is
                # gracefully closed, e.g. if it's just marked as
                # `stale` but still alive.
                if log.isEnabledFor(logging.DEBUG):
                    log.debug(
                        "[#%04X]  C: <POOL> removing old connection "
                        "(closed=%s, defunct=%s, stale=%s, in_use=%s)",
                        connection.local_port,
                        connection.closed(), connection.defunct(),
                        connection.stale(), connection.in_use
                    )
                connection.close()
                with self.lock:
                    try:
                        self.connections.get(address, []).remove(connection)
                    except ValueError:
                        # If closure fails (e.g. because the server went
                        # down), all connections to the same address will
                        # be removed. Therefore, we silently ignore if the
                        # connection isn't in the pool anymore.
                        pass
                continue  # try again with a new connection
            else:
                return connection

    def _acquire_new_later(self, address, deadline):
        def connection_creator():
            released_reservation = False
            try:
                try:
                    connection = self.opener(
                        address, deadline.to_timeout()
                    )
                except ServiceUnavailable:
                    self.deactivate(address)
                    raise
                connection.pool = self
                connection.in_use = True
                with self.lock:
                    self.connections_reservations[address] -= 1
                    released_reservation = True
                    self.connections[address].append(connection)
                return connection
            finally:
                if not released_reservation:
                    with self.lock:
                        self.connections_reservations[address] -= 1

        max_pool_size = self.pool_config.max_connection_pool_size
        infinite_pool_size = (max_pool_size < 0
                              or max_pool_size == float("inf"))
        with self.lock:
            connections = self.connections[address]
            pool_size = (len(connections)
                         + self.connections_reservations[address])
            can_create_new_connection = (infinite_pool_size
                                         or pool_size < max_pool_size)
            self.connections_reservations[address] += 1
        if can_create_new_connection:
            return connection_creator

    def _acquire(self, address, deadline):

        """ Acquire a connection to a given address from the pool.
        The address supplied should always be an IP address, not
        a host name.

        This method is thread safe.
        """
        def health_check(connection_, _deadline):
            if (connection_.closed()
                    or connection_.defunct()
                    or connection_.stale()):
                return False
            return True

        while True:
            # try to find a free connection in the pool
            connection = self._acquire_from_pool_checked(
                address, health_check, deadline
            )
            if connection:
                return connection
            # all connections in pool are in-use
            with self.lock:
                connection_creator = self._acquire_new_later(
                    address, deadline
                )
                if connection_creator:
                    break

                # failed to obtain a connection from pool because the
                # pool is full and no free connection in the pool
                timeout = deadline.to_timeout()
                if (
                    timeout == 0  # deadline expired
                    or not self.cond.wait(timeout)
                ):
                    raise ClientError(
                        "Failed to obtain a connection from pool within {!r}s"
                        .format(deadline.original_timeout)
                    )
        return connection_creator()

    def acquire(self, access_mode, timeout, acquisition_timeout, database,
                bookmarks):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param access_mode:
        :param timeout: total timeout (including potential preparation)
        :param acquisition_timeout: timeout for actually acquiring a connection
        :param database:
        :param bookmarks:
        """

    def release(self, *connections):
        """ Release a connection back into the pool.
        This method is thread safe.
        """
        with self.lock:
            for connection in connections:
                if not (connection.defunct()
                        or connection.closed()
                        or connection.is_reset):
                    try:
                        connection.reset()
                    except (Neo4jError, DriverError, BoltError) as e:
                        log.debug(
                            "Failed to reset connection on release: %s", e
                        )
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

    def mark_all_stale(self):
        with self.lock:
            for address in self.connections:
                for connection in self.connections[address]:
                    connection.set_stale()

    def deactivate(self, address):
        """ Deactivate an address from the connection pool, if present, closing
        all idle connection to that address
        """
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:  # already removed from the connection pool
                return
            closable_connections = [
                conn for conn in connections if not conn.in_use
            ]
            # First remove all connections in question, then try to close them.
            # If closing of a connection fails, we will end up in this method
            # again.
            for conn in closable_connections:
                connections.remove(conn)
            for conn in closable_connections:
                conn.close()
            if not self.connections[address]:
                del self.connections[address]

    def on_write_failure(self, address):
        raise WriteServiceUnavailable("No write service available for pool {}".format(self))

    def close(self):
        """ Close all connections and empty the pool.
        This method is thread safe.
        """
        try:
            with self.lock:
                for address in list(self.connections):
                    for connection in self.connections.pop(address, ()):
                        try:
                            connection.close()
                        except OSError:
                            pass
        except TypeError:
            pass


BoltSocket.Bolt = Bolt


class BoltPool(IOPool):

    @classmethod
    def open(cls, address, *, auth, pool_config, workspace_config):
        """Create a new BoltPool

        :param address:
        :param auth:
        :param pool_config:
        :param workspace_config:
        :return: BoltPool
        """

        def opener(addr, timeout):
            return Bolt.open(
                addr, auth=auth, timeout=timeout, routing_context=None,
                **pool_config
            )

        pool = cls(opener, pool_config, workspace_config, address)
        return pool

    def __init__(self, opener, pool_config, workspace_config, address):
        super(BoltPool, self).__init__(opener, pool_config, workspace_config)
        self.address = address

    def __repr__(self):
        return "<{} address={!r}>".format(self.__class__.__name__, self.address)

    def acquire(self, access_mode, timeout,  acquisition_timeout, database,
                bookmarks):
        # The access_mode and database is not needed for a direct connection, its just there for consistency.
        deadline = merge_deadlines_and_timeouts(timeout, acquisition_timeout)
        return self._acquire(self.address, deadline)


class Neo4jPool(IOPool):
    """ Connection pool with routing table.
    """

    @classmethod
    def open(cls, *addresses, auth, pool_config, workspace_config, routing_context=None):
        """Create a new Neo4jPool

        :param addresses: one or more address as positional argument
        :param auth:
        :param pool_config:
        :param workspace_config:
        :param routing_context:
        :return: Neo4jPool
        """

        address = addresses[0]
        if routing_context is None:
            routing_context = {}
        elif "address" in routing_context:
            raise ConfigurationError("The key 'address' is reserved for routing context.")
        routing_context["address"] = str(address)

        def opener(addr, timeout):
            return Bolt.open(addr, auth=auth, timeout=timeout,
                             routing_context=routing_context, **pool_config)

        pool = cls(opener, pool_config, workspace_config, address)
        return pool

    def __init__(self, opener, pool_config, workspace_config, address):
        """

        :param opener:
        :param pool_config:
        :param workspace_config:
        :param address:
        """
        super(Neo4jPool, self).__init__(opener, pool_config, workspace_config)
        # Each database have a routing table, the default database is a special case.
        log.debug("[#0000]  C: <NEO4J POOL> routing address %r", address)
        self.address = address
        self.routing_tables = {workspace_config.database: RoutingTable(database=workspace_config.database, routers=[address])}
        self.refresh_lock = RLock()

    def __repr__(self):
        """ The representation shows the initial routing addresses.

        :return: The representation
        :rtype: str
        """
        return "<{} addresses={!r}>".format(self.__class__.__name__, self.get_default_database_initial_router_addresses())

    @contextmanager
    def _refresh_lock_deadline(self, deadline):
        timeout = deadline.to_timeout()
        if timeout is None:
            timeout = -1
        if not self.refresh_lock.acquire(timeout=timeout):
            raise ClientError(
                "pool failed to update routing table within {!r}s (timeout)"
                .format(deadline.original_timeout)
            )

        try:
            yield
        finally:
            self.refresh_lock.release()

    @property
    def first_initial_routing_address(self):
        return self.get_default_database_initial_router_addresses()[0]

    def get_default_database_initial_router_addresses(self):
        """ Get the initial router addresses for the default database.

        :return:
        :rtype: OrderedSet
        """
        return self.get_routing_table_for_default_database().initial_routers

    def get_default_database_router_addresses(self):
        """ Get the router addresses for the default database.

        :return:
        :rtype: OrderedSet
        """
        return self.get_routing_table_for_default_database().routers

    def get_routing_table_for_default_database(self):
        return self.routing_tables[self.workspace_config.database]

    def get_or_create_routing_table(self, database):
        with self.refresh_lock:
            if database not in self.routing_tables:
                self.routing_tables[database] = RoutingTable(
                    database=database,
                    routers=self.get_default_database_initial_router_addresses()
                )
            return self.routing_tables[database]

    def fetch_routing_info(self, address, database, imp_user, bookmarks,
                           deadline):
        """ Fetch raw routing info from a given router address.

        :param address: router address
        :param database: the database name to get routing table for
        :param imp_user: the user to impersonate while fetching the routing
                         table
        :type imp_user: str or None
        :param bookmarks: iterable of bookmark values after which the routing
                          info should be fetched
        :param deadline: connection acquisition deadline

        :return: list of routing records, or None if no connection
            could be established or if no readers or writers are present
        :raise ServiceUnavailable: if the server does not support
            routing, or if routing support is broken or outdated
        """
        cx = self._acquire(address, deadline)
        try:
            with connection_deadline(cx, deadline):
                routing_table = cx.route(
                    database or self.workspace_config.database,
                    imp_user or self.workspace_config.impersonated_user,
                    bookmarks
                )
        finally:
            self.release(cx)
        return routing_table

    def fetch_routing_table(self, *, address, deadline, database, imp_user,
                            bookmarks):
        """ Fetch a routing table from a given router address.

        :param address: router address
        :param deadline: deadline
        :param database: the database name
        :type: str
        :param imp_user: the user to impersonate while fetching the routing
                         table
        :type imp_user: str or None
        :param bookmarks: bookmarks used when fetching routing table

        :return: a new RoutingTable instance or None if the given router is
                 currently unable to provide routing information
        """
        new_routing_info = None
        try:
            new_routing_info = self.fetch_routing_info(
                address, database, imp_user, bookmarks, deadline
            )
        except Neo4jError as e:
            # checks if the code is an error that is caused by the client. In
            # this case there is no sense in trying to fetch a RT from another
            # router. Hence, the driver should fail fast during discovery.
            if e.is_fatal_during_discovery():
                raise
        except (ServiceUnavailable, SessionExpired):
            pass
        if not new_routing_info:
            log.debug("Failed to fetch routing info %s", address)
            return None
        else:
            servers = new_routing_info[0]["servers"]
            ttl = new_routing_info[0]["ttl"]
            database = new_routing_info[0].get("db", database)
            new_routing_table = RoutingTable.parse_routing_info(
                database=database, servers=servers, ttl=ttl
            )

        # Parse routing info and count the number of each type of server
        num_routers = len(new_routing_table.routers)
        num_readers = len(new_routing_table.readers)

        # num_writers = len(new_routing_table.writers)
        # If no writers are available. This likely indicates a temporary state,
        # such as leader switching, so we should not signal an error.

        # No routers
        if num_routers == 0:
            log.debug("No routing servers returned from server %s", address)
            return None

        # No readers
        if num_readers == 0:
            log.debug("No read servers returned from server %s", address)
            return None

        # At least one of each is fine, so return this table
        return new_routing_table

    def _update_routing_table_from(
            self, *routers, database, imp_user, bookmarks, deadline,
            database_callback
    ):
        """ Try to update routing tables with the given routers.

        :return: True if the routing table is successfully updated,
        otherwise False
        """
        log.debug("Attempting to update routing table from {}".format(", ".join(map(repr, routers))))
        for router in routers:
            for address in router.resolve(resolver=self.pool_config.resolver):
                if deadline.expired():
                    return False
                new_routing_table = self.fetch_routing_table(
                    address=address, deadline=deadline, database=database,
                    imp_user=imp_user, bookmarks=bookmarks
                )
                if new_routing_table is not None:
                    new_database = new_routing_table.database
                    old_routing_table = self.get_or_create_routing_table(
                        new_database
                    )
                    old_routing_table.update(new_routing_table)
                    log.debug(
                        "[#0000]  C: <UPDATE ROUTING TABLE> address=%r (%r)",
                        address, self.routing_tables[new_database]
                    )
                    if callable(database_callback):
                        database_callback(new_database)
                    return True
            self.deactivate(router)
        return False

    def update_routing_table(self, *, database, imp_user, bookmarks,
                             timeout=None, database_callback=None):
        """ Update the routing table from the first router able to provide
        valid routing information.

        :param database: The database name
        :param imp_user: the user to impersonate while fetching the routing
                         table
        :type imp_user: str or None
        :param bookmarks: bookmarks used when fetching routing table
        :param timeout: timeout in seconds for how long to try updating
        :param database_callback: A callback function that will be called with
            the database name as only argument when a new routing table has been
            acquired. This database name might different from `database` if that
            was None and the underlying protocol supports reporting back the
            actual database.

        :raise neo4j.exceptions.ServiceUnavailable:
        """
        deadline = merge_deadlines_and_timeouts(
            timeout, self.pool_config.update_routing_table_timeout
        )
        with self._refresh_lock_deadline(deadline):
            # copied because it can be modified
            existing_routers = set(
                self.get_or_create_routing_table(database).routers
            )

            prefer_initial_routing_address = \
                self.routing_tables[database].initialized_without_writers

            if prefer_initial_routing_address:
                # TODO: Test this state
                if self._update_routing_table_from(
                        self.first_initial_routing_address, database=database,
                        imp_user=imp_user, bookmarks=bookmarks,
                        deadline=deadline, database_callback=database_callback
                ):
                    # Why is only the first initial routing address used?
                    return
            if self._update_routing_table_from(
                    *(existing_routers - {self.first_initial_routing_address}),
                    database=database, imp_user=imp_user, bookmarks=bookmarks,
                    deadline=deadline, database_callback=database_callback
            ):
                return

            if not prefer_initial_routing_address:
                if self._update_routing_table_from(
                    self.first_initial_routing_address, database=database,
                    imp_user=imp_user, bookmarks=bookmarks,
                    deadline=deadline, database_callback=database_callback
                ):
                    # Why is only the first initial routing address used?
                    return

            # None of the routers have been successful, so just fail
            log.error("Unable to retrieve routing information")
            raise ServiceUnavailable("Unable to retrieve routing information")

    def update_connection_pool(self, *, database):
        servers = self.get_or_create_routing_table(database).servers()
        for address in list(self.connections):
            if address.unresolved not in servers:
                super(Neo4jPool, self).deactivate(address)

    def ensure_routing_table_is_fresh(
            self, *, access_mode, database, imp_user, bookmarks, deadline=None,
            database_callback=None
    ):
        """ Update the routing table if stale.

        This method performs two freshness checks, before and after acquiring
        the refresh lock. If the routing table is already fresh on entry, the
        method exits immediately; otherwise, the refresh lock is acquired and
        the second freshness check that follows determines whether an update
        is still required.

        This method is thread-safe.

        :return: `True` if an update was required, `False` otherwise.
        """
        from neo4j.api import READ_ACCESS
        with self._refresh_lock_deadline(deadline):
            if self.get_or_create_routing_table(database)\
                    .is_fresh(readonly=(access_mode == READ_ACCESS)):
                # Readers are fresh.
                return False

            self.update_routing_table(
                database=database, imp_user=imp_user, bookmarks=bookmarks,
                timeout=deadline, database_callback=database_callback
            )
            self.update_connection_pool(database=database)

            for database in list(self.routing_tables.keys()):
                # Remove unused databases in the routing table
                # Remove the routing table after a timeout = TTL + 30s
                log.debug("[#0000]  C: <ROUTING AGED> database=%s", database)
                if (self.routing_tables[database].should_be_purged_from_memory()
                        and database != self.workspace_config.database):
                    del self.routing_tables[database]

            return True

    def _select_address(self, *, access_mode, database):
        from neo4j.api import READ_ACCESS
        """ Selects the address with the fewest in-use connections.
        """
        with self.refresh_lock:
            if access_mode == READ_ACCESS:
                addresses = self.routing_tables[database].readers
            else:
                addresses = self.routing_tables[database].writers
            addresses_by_usage = {}
            for address in addresses:
                addresses_by_usage.setdefault(
                    self.in_use_connection_count(address), []
                ).append(address)
        if not addresses_by_usage:
            if access_mode == READ_ACCESS:
                raise ReadServiceUnavailable(
                    "No read service currently available"
                )
            else:
                raise WriteServiceUnavailable(
                    "No write service currently available"
                )
        return choice(addresses_by_usage[min(addresses_by_usage)])

    def acquire(self, access_mode, timeout, acquisition_timeout, database,
                bookmarks):
        if access_mode not in (WRITE_ACCESS, READ_ACCESS):
            raise ClientError("Non valid 'access_mode'; {}".format(access_mode))
        if not timeout:
            raise ClientError("'timeout' must be a float larger than 0; {}"
                              .format(timeout))
        if not acquisition_timeout:
            raise ClientError("'acquisition_timeout' must be a float larger "
                              "than 0; {}".format(acquisition_timeout))
        deadline = Deadline.from_timeout_or_deadline(timeout)

        from neo4j.api import check_access_mode
        access_mode = check_access_mode(access_mode)
        with self._refresh_lock_deadline(deadline):
            log.debug("[#0000]  C: <ROUTING TABLE ENSURE FRESH> %r",
                      self.routing_tables)
            self.ensure_routing_table_is_fresh(
                access_mode=access_mode, database=database, imp_user=None,
                bookmarks=bookmarks, deadline=deadline
            )

        # Making sure the routing table is fresh is not considered part of the
        # connection acquisition. Hence, the acquisition_timeout starts now!
        deadline = merge_deadlines(
            deadline, Deadline.from_timeout_or_deadline(acquisition_timeout)
        )
        while True:
            try:
                # Get an address for a connection that have the fewest in-use
                # connections.
                address = self._select_address(access_mode=access_mode,
                                               database=database)
            except (ReadServiceUnavailable, WriteServiceUnavailable) as err:
                raise SessionExpired("Failed to obtain connection towards '%s' server." % access_mode) from err
            try:
                log.debug("[#0000]  C: <ACQUIRE ADDRESS> database=%r address=%r", database, address)
                # should always be a resolved address
                connection = self._acquire(address, deadline)
            except ServiceUnavailable:
                self.deactivate(address=address)
            else:
                return connection

    def deactivate(self, address):
        """ Deactivate an address from the connection pool,
        if present, remove from the routing table and also closing
        all idle connections to that address.
        """
        log.debug("[#0000]  C: <ROUTING> Deactivating address %r", address)
        # We use `discard` instead of `remove` here since the former
        # will not fail if the address has already been removed.
        for database in self.routing_tables.keys():
            self.routing_tables[database].routers.discard(address)
            self.routing_tables[database].readers.discard(address)
            self.routing_tables[database].writers.discard(address)
        log.debug("[#0000]  C: <ROUTING> table=%r", self.routing_tables)
        super(Neo4jPool, self).deactivate(address)

    def on_write_failure(self, address):
        """ Remove a writer address from the routing table, if present.
        """
        log.debug("[#0000]  C: <ROUTING> Removing writer %r", address)
        for database in self.routing_tables.keys():
            self.routing_tables[database].writers.discard(address)
        log.debug("[#0000]  C: <ROUTING> table=%r", self.routing_tables)


def check_supported_server_product(agent):
    """ Checks that a server product is supported by the driver by
    looking at the server agent string.

    :param agent: server agent string to check for validity
    :raises UnsupportedServerProduct: if the product is not supported
    """
    if not agent.startswith("Neo4j/"):
        raise UnsupportedServerProduct(agent)
