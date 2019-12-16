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
    "Bolt",
    "BoltPool",
    "Neo4jPool",
]


from collections import deque
from logging import getLogger
from random import choice
from select import select

from socket import (
    socket,
    SOL_SOCKET,
    SO_KEEPALIVE,
    SHUT_RDWR,
    timeout as SocketTimeout,
    AF_INET,
    AF_INET6,
)

from ssl import HAS_SNI, SSLSocket, SSLError
from struct import pack as struct_pack, unpack as struct_unpack
from threading import Lock, RLock, Condition
from time import perf_counter

from neo4j.addressing import Address, AddressList

from neo4j.conf import (
    Config,
    PoolConfig,
)
from neo4j.errors import (
    BoltRoutingError,
    Neo4jAvailabilityError,
)
from neo4j.exceptions import (
    ProtocolError,
    SecurityError,
    ServiceUnavailable,
    AuthError,
    IncompleteCommitError,
    ConnectionExpired,
    DatabaseUnavailableError,
    NotALeaderError,
    ForbiddenOnReadOnlyDatabaseError,
    ClientError,
)

from neo4j.routing import RoutingTable

from neo4j.debug import watch

DEFAULT_KEEP_ALIVE = True

DEFAULT_CONNECTION_TIMEOUT = 5.0  # 5s

# Set up logger
log = getLogger("neo4j")

watch("neo4j")


class Bolt:
    """ Server connection for Bolt protocol

    A :class:`.BoltVersion` should be constructed following a
    successful Bolt handshake and takes the socket over which
    the handshake was carried out.

    Bolt.open returns a Bolt object containing a connection
    with the specified Bolt protocol version negotiated with a Bolt handshake.
    """

    MAGIC_PREAMBLE = 0x6060B017

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

    @classmethod
    def ping(cls, address, *, timeout=None, **config):
        """ Attempt to establish a Bolt connection, returning the
        agreed Bolt protocol version if successful.
        """
        config = PoolConfig.consume(config)
        try:
            s, protocol_version = connect(address, timeout=timeout, config=config)
        except ServiceUnavailable:
            return None
        else:
            log.debug("[#{port:04X}]  S: <CLOSE>".format(port=s.getpeername()[1]))
            s.shutdown(SHUT_RDWR)
            s.close()
            return protocol_version

    @classmethod
    def open(cls, address, *, auth=None, timeout=None, **config):
        """ Open a new Bolt connection to a given server address.

        :param address:
        :param auth:
        :param timeout:
        :param config:
        :return:
        """
        config = PoolConfig.consume(config)
        # s, config.protocol_version = connect(address, timeout=timeout, config=config)

        s, protocol_version = connect(address, timeout=timeout, config=config)
        # Depending on which version was agreed load correct Bolt version
        if protocol_version == (3, 0):
            config.protocol_version = protocol_version
            from neo4j.io._bolt3_0 import Bolt3
            connection = Bolt3(address, s, auth=auth, **config)
        else:
            log.debug("[#{port:04X}]  S: <CLOSE>".format(port=s.getpeername()[1]))
            s.shutdown(SHUT_RDWR)
            s.close()
            raise ProtocolError("Driver does not support Bolt protocol version: {}".format(protocol_version))

        connection.hello()
        return connection

    @property
    def secure(self):
        return isinstance(self.socket, SSLSocket)

    @property
    def der_encoded_server_certificate(self):
        return self.socket.getpeercert(binary_form=True)

    @property
    def local_port(self):
        try:
            return self.socket.getsockname()[1]
        except IOError:
            return 0

    def hello(self):
        raise NotImplementedError

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, statement, parameters=None, mode=None, bookmarks=None, metadata=None, timeout=None, **handlers):
        raise NotImplementedError

    def discard_all(self, **handlers):
        raise NotImplementedError

    def pull_all(self, **handlers):
        raise NotImplementedError

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None, **handlers):
        raise NotImplementedError

    def commit(self, **handlers):
        raise NotImplementedError

    def rollback(self, **handlers):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def send_all(self):
        raise NotImplementedError

    def fetch_message(self):
        raise NotImplementedError

    def timedout(self):
        return 0 <= self._max_connection_lifetime <= perf_counter() - self._creation_timestamp

    def fetch_all(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def closed(self):
        raise NotImplementedError
        # return self._closed

    def defunct(self):
        raise NotImplementedError
        # return self._defunct


class IOPool:
    """ A collection of connections to one or more server addresses.
    """

    _default_acquire_timeout = 60  # seconds

    _default_max_size = 100

    def __init__(self, opener, config):
        assert callable(opener)
        assert isinstance(config, PoolConfig)
        self.opener = opener
        self.config = config
        self.connections = {}
        self.lock = RLock()
        self.cond = Condition(self.lock)
        self._max_connection_pool_size = config.max_size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _acquire(self, address, timeout):
        """ Acquire a connection to a given address from the pool.
        The address supplied should always be an IP address, not
        a host name.

        This method is thread safe.
        """
        t0 = perf_counter()
        if timeout is None:
            timeout = self._default_acquire_timeout

        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:
                connections = self.connections[address] = deque()

            def time_remaining():
                t = timeout - (perf_counter() - t0)
                return t if t > 0 else 0

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
                infinite_pool_size = (self._max_connection_pool_size < 0 or
                                      self._max_connection_pool_size == float("inf"))
                can_create_new_connection = infinite_pool_size or len(connections) < self._max_connection_pool_size
                if can_create_new_connection:
                    timeout = min(self.config.connect_timeout, time_remaining())
                    try:
                        connection = self.opener(address, timeout)
                    except ServiceUnavailable:
                        self.remove(address)
                        raise
                    else:
                        connection.pool = self
                        connection.in_use = True
                        connections.append(connection)
                        return connection

                # failed to obtain a connection from pool because the
                # pool is full and no free connection in the pool
                if time_remaining():
                    self.cond.wait(time_remaining())
                    # if timed out, then we throw error. This time
                    # computation is needed, as with python 2.7, we
                    # cannot tell if the condition is notified or
                    # timed out when we come to this line
                    if not time_remaining():
                        raise ClientError("Failed to obtain a connection from pool "
                                          "within {!r}s".format(timeout))
                else:
                    raise ClientError("Failed to obtain a connection from pool "
                                      "within {!r}s".format(timeout))

    def acquire(self, access_mode=None, timeout=None):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param access_mode:
        :param timeout:
        """

    def release(self, *connections):
        """ Release a connection back into the pool.
        This method is thread safe.
        """
        with self.lock:
            for connection in connections:
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
            except KeyError:  # already removed from the connection pool
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

    def on_write_failure(self, address):
        raise Neo4jAvailabilityError("No write service available for pool {}".format(self))

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
        try:
            with self.lock:
                for address in list(self.connections):
                    self.remove(address)
        except TypeError:
            pass


class BoltPool(IOPool):

    @classmethod
    def open(cls, address, *, auth=None, **config):
        pool_config = PoolConfig.consume(config)

        def opener(addr, timeout):
            return Bolt.open(addr, auth=auth, timeout=timeout, **pool_config)

        pool = cls(opener, pool_config, address)
        seeds = [pool.acquire() for _ in range(pool_config.init_size)]
        pool.release(*seeds)
        return pool

    def __init__(self, opener, config, address):
        super(BoltPool, self).__init__(opener, config)
        self.address = address

    def __repr__(self):
        return "<{} address={!r}>".format(self.__class__.__name__, self.address)

    def acquire(self, access_mode=None, timeout=None):
        return self._acquire(self.address, timeout)


class Neo4jPool(IOPool):
    """ Connection pool with routing table.
    """

    @classmethod
    def open(cls, *addresses, auth=None, routing_context=None, **config):
        pool_config = PoolConfig.consume(config)

        def opener(addr, timeout):
            return Bolt.open(addr, auth=auth, timeout=timeout, **pool_config)

        pool = cls(opener, pool_config, addresses, routing_context)
        try:
            pool.update_routing_table()
        except Exception:
            pool.close()
            raise
        else:
            return pool

    def __init__(self, opener, config, addresses, routing_context):
        super(Neo4jPool, self).__init__(opener, config)
        self.routing_table = RoutingTable(addresses)
        self.routing_context = routing_context
        self.missing_writer = False
        self.refresh_lock = Lock()

    def __repr__(self):
        return "<{} addresses={!r}>".format(self.__class__.__name__,
                                            self.routing_table.initial_routers)

    @property
    def initial_address(self):
        return self.routing_table.initial_routers[0]

    def fetch_routing_info(self, address):
        """ Fetch raw routing info from a given router address.

        :param address: router address
        :return: list of routing records or
                 None if no connection could be established
        :raise ServiceUnavailable: if the server does not support routing or
                                   if routing support is broken
        """
        metadata = {}
        records = []

        def fail(md):
            if md.get("code") == "Neo.ClientError.Procedure.ProcedureNotFound":
                raise BoltRoutingError("Server does not support routing", address)
            else:
                raise BoltRoutingError("Routing support broken on server", address)

        try:
            with self._acquire(address, timeout=300) as cx:  # TODO: remove magic timeout number
                _, _, server_version = (cx.server.agent or "").partition("/")
                log.debug("[#%04X]  C: <ROUTING> query=%r", cx.local_port, self.routing_context or {})
                cx.run("CALL dbms.cluster.routing.getRoutingTable($context)",
                       {"context": self.routing_context}, on_success=metadata.update, on_failure=fail)
                cx.pull_all(on_success=metadata.update, on_records=records.extend)
                cx.send_all()
                cx.fetch_all()
                routing_info = [dict(zip(metadata.get("fields", ()), values)) for values in records]
                log.debug("[#%04X]  S: <ROUTING> info=%r", cx.local_port, routing_info)
            return routing_info
        except BoltRoutingError as error:
            raise ServiceUnavailable(*error.args)
        except ServiceUnavailable:
            self.deactivate(address)
            return None

    def fetch_routing_table(self, address):
        """ Fetch a routing table from a given router address.

        :param address: router address
        :return: a new RoutingTable instance or None if the given router is
                 currently unable to provide routing information
        :raise ServiceUnavailable: if no writers are available
        :raise ProtocolError: if the routing information received is unusable
        """
        new_routing_info = self.fetch_routing_info(address)
        if new_routing_info is None:
            return None
        elif not new_routing_info:
            raise BoltRoutingError("Invalid routing table", address)
        else:
            servers = new_routing_info[0]["servers"]
            ttl = new_routing_info[0]["ttl"]
            new_routing_table = RoutingTable.parse_routing_info(servers, ttl)

        # Parse routing info and count the number of each type of server
        num_routers = len(new_routing_table.routers)
        num_readers = len(new_routing_table.readers)
        num_writers = len(new_routing_table.writers)

        # No writers are available. This likely indicates a temporary state,
        # such as leader switching, so we should not signal an error.
        # When no writers available, then we flag we are reading in absence of writer
        self.missing_writer = (num_writers == 0)

        # No routers
        if num_routers == 0:
            raise BoltRoutingError("No routing servers returned from server", address)

        # No readers
        if num_readers == 0:
            raise BoltRoutingError("No read servers returned from server", address)

        # At least one of each is fine, so return this table
        return new_routing_table

    def update_routing_table_from(self, *routers):
        """ Try to update routing tables with the given routers.

        :return: True if the routing table is successfully updated,
        otherwise False
        """
        log.debug("Attempting to update routing table from "
                  "{}".format(", ".join(map(repr, routers))))
        for router in routers:
            new_routing_table = self.fetch_routing_table(router)
            if new_routing_table is not None:
                self.routing_table.update(new_routing_table)
                log.debug("Successfully updated routing table from "
                          "{!r} ({!r})".format(router, self.routing_table))
                return True
        return False

    def update_routing_table(self):
        """ Update the routing table from the first router able to provide
        valid routing information.
        """
        # copied because it can be modified
        existing_routers = list(self.routing_table.routers)

        has_tried_initial_routers = False
        if self.missing_writer:
            has_tried_initial_routers = True
            if self.update_routing_table_from(self.initial_address):
                return

        if self.update_routing_table_from(*existing_routers):
            return

        if not has_tried_initial_routers and self.initial_address not in existing_routers:
            if self.update_routing_table_from(self.initial_address):
                return

        # None of the routers have been successful, so just fail
        log.error("Unable to retrieve routing information")
        raise ServiceUnavailable("Unable to retrieve routing information")

    def update_connection_pool(self):
        servers = self.routing_table.servers()
        for address in list(self.connections):
            if address not in servers:
                super(Neo4jPool, self).deactivate(address)

    def ensure_routing_table_is_fresh(self, access_mode):
        """ Update the routing table if stale.

        This method performs two freshness checks, before and after acquiring
        the refresh lock. If the routing table is already fresh on entry, the
        method exits immediately; otherwise, the refresh lock is acquired and
        the second freshness check that follows determines whether an update
        is still required.

        This method is thread-safe.

        :return: `True` if an update was required, `False` otherwise.
        """
        from neo4j import READ_ACCESS
        if self.routing_table.is_fresh(readonly=(access_mode == READ_ACCESS)):
            return False
        with self.refresh_lock:
            if self.routing_table.is_fresh(readonly=(access_mode == READ_ACCESS)):
                if access_mode == READ_ACCESS:
                    # if reader is fresh but writers is not fresh, then we are reading in absence of writer
                    self.missing_writer = not self.routing_table.is_fresh(readonly=False)
                return False
            self.update_routing_table()
            self.update_connection_pool()
            return True

    def _select_address(self, access_mode=None):
        from neo4j import READ_ACCESS
        """ Selects the address with the fewest in-use connections.
        """
        self.ensure_routing_table_is_fresh(access_mode)
        if access_mode == READ_ACCESS:
            addresses = self.routing_table.readers
        else:
            addresses = self.routing_table.writers
        addresses_by_usage = {}
        for address in addresses:
            addresses_by_usage.setdefault(self.in_use_connection_count(address), []).append(address)
        if not addresses_by_usage:
            raise Neo4jAvailabilityError("No {} service currently available".format(
                "read" if access_mode == READ_ACCESS else "write"))
        return choice(addresses_by_usage[min(addresses_by_usage)])

    def acquire(self, access_mode=None, timeout=None):
        from neo4j import READ_ACCESS, WRITE_ACCESS
        if access_mode is None:
            access_mode = WRITE_ACCESS
        if access_mode not in (READ_ACCESS, WRITE_ACCESS):
            raise ValueError("Unsupported access mode {}".format(access_mode))
        while True:
            try:
                address = self._select_address(access_mode)
            except Neo4jAvailabilityError as err:
                raise ConnectionExpired("Failed to obtain connection "
                                        "towards '%s' server." % access_mode) from err
            try:
                connection = self._acquire(address, timeout=timeout)  # should always be a resolved address
                connection.Error = ConnectionExpired
            except ServiceUnavailable:
                self.deactivate(address)
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
        self.routing_table.routers.discard(address)
        self.routing_table.readers.discard(address)
        self.routing_table.writers.discard(address)
        log.debug("[#0000]  C: <ROUTING> table=%r", self.routing_table)
        super(Neo4jPool, self).deactivate(address)

    def on_write_failure(self, address):
        """ Remove a writer address from the routing table, if present.
        """
        log.debug("[#0000]  C: <ROUTING> Removing writer %r", address)
        self.routing_table.writers.discard(address)
        log.debug("[#0000]  C: <ROUTING> table=%r", self.routing_table)


def connect(address, *, timeout=None, config):
    """ Connect and perform a handshake and return a valid Connection object,
    assuming a protocol version can be agreed.
    """
    last_error = None
    # Establish a connection to the host and port specified
    # Catches refused connections see:
    # https://docs.python.org/2/library/errno.html
    log.debug("[#0000]  C: <RESOLVE> %s", address)
    address_list = AddressList([address])
    address_list.custom_resolve(config.get("resolver"))
    address_list.dns_resolve()
    for resolved_address in address_list:
        s = None
        try:
            host = address[0]

            # Connect
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
                if timeout is None:
                    s.settimeout(DEFAULT_CONNECTION_TIMEOUT)
                else:
                    s.settimeout(timeout)
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

            # Check Secure Connection
            # Secure the connection if an SSL context has been provided

            ssl_context = config.get_ssl_context()
            local_port = s.getsockname()[1]

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

            local_port = s.getsockname()[1]

            # Send details of the protocol versions supported
            supported_versions = [3, 0, 0, 0]
            handshake = [Bolt.MAGIC_PREAMBLE] + supported_versions

            log.debug("[#{port:04X}]  C: <MAGIC> 0x{magic:08X}".format(port=local_port, magic=Bolt.MAGIC_PREAMBLE))
            log.debug("[#{port:04X}]  C: <HANDSHAKE> 0x{v_a:08X} 0x{v_b:08X} 0x{v_c:08X} 0x{v_d:08X}".format(
                port=local_port,
                v_a=supported_versions[0],
                v_b=supported_versions[1],
                v_c=supported_versions[2],
                v_d=supported_versions[3],
            ))

            data = b"".join(struct_pack(">I", num) for num in handshake)
            s.sendall(data)

            # Handle the handshake response
            ready_to_read = False
            while not ready_to_read:
                ready_to_read, _, _ = select((s,), (), (), 1)
            try:
                data = s.recv(4)
            except OSError:
                raise ServiceUnavailable(
                    "Failed to read any data from server {address} after connected".format(address=resolved_address))

            data_size = len(data)
            if data_size == 0:
                # If no data is returned after a successful select
                # response, the server has closed the connection
                log.debug("[#{port:04X}]  S: <CLOSE>".format(port=local_port))
                s.close()
                raise ServiceUnavailable(
                    "Connection to {address} closed without handshake response".format(address=resolved_address))

            if data_size != 4:
                # Some garbled data has been received
                log.debug("[#{port:04X}]  S: @*#!".format(local_port))
                s.close()
                raise ProtocolError("Expected four byte Bolt handshake response "
                                    "from %r, received %r instead; check for "
                                    "incorrect port number" % (resolved_address, data))
            elif data == b"HTTP":
                log.debug("[#{port:04X}]  S: <CLOSE>".format(port=local_port))
                s.close()
                raise ServiceUnavailable(
                    "Cannot to connect to Bolt service on {address} (looks like HTTP)".format(address=resolved_address))

            agreed_version = (data[-1], data[-2])
            log.debug("[#{port:04X}]  S: <HANDSHAKE> 0x{minor:06X}{major:02X}".format(port=local_port,
                                                                                      minor=agreed_version[1],
                                                                                      major=agreed_version[0]))
            return s, agreed_version
        except Exception as error:
            if s:
                s.close()
            last_error = error
    if last_error is None:
        raise ServiceUnavailable("Failed to resolve addresses for %s" % address)
    else:
        raise last_error
