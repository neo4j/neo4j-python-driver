#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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
from time import perf_counter

from socket import (
    socket,
    SOL_SOCKET,
    SO_KEEPALIVE,
    SHUT_RDWR,
    timeout as SocketTimeout,
    AF_INET,
    AF_INET6,
)

from ssl import (
    HAS_SNI,
    SSLError,
)

from struct import (
    pack as struct_pack,
)

from threading import (
    Lock,
    RLock,
    Condition,
)

from neo4j.addressing import Address
from neo4j.conf import PoolConfig
from neo4j._exceptions import (
    BoltRoutingError,
    BoltSecurityError,
    BoltProtocolError,
    BoltHandshakeError,
)
from neo4j.exceptions import (
    ServiceUnavailable,
    ClientError,
    SessionExpired,
    ReadServiceUnavailable,
    WriteServiceUnavailable,
)
from neo4j.routing import RoutingTable

# Set up logger
log = getLogger("neo4j")


class Bolt:
    """ Server connection for Bolt protocol.

    A :class:`.Bolt` should be constructed following a
    successful .open()

    Bolt handshake and takes the socket over which
    the handshake was carried out.
    """

    MAGIC_PREAMBLE = b"\x60\x60\xB0\x17"

    PROTOCOL_VERSION = None

    @classmethod
    def get_handshake(cls):
        """ Return the supported Bolt versions as bytes.
        The length is 16 bytes as specified in the Bolt version negotiation.
        :return: bytes
        """
        offered_versions = sorted(cls.protocol_handlers().keys(), reverse=True)[:4]
        return b"".join(version.to_bytes() for version in offered_versions).ljust(16, b"\x00")

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

        # Carry out subclass imports locally to avoid circular
        # dependency issues.
        from neo4j.io._bolt3 import Bolt3
        from neo4j.io._bolt4x0 import Bolt4x0

        handlers = {
            Bolt3.PROTOCOL_VERSION: Bolt3,
            Bolt4x0.PROTOCOL_VERSION: Bolt4x0
        }

        if protocol_version is None:
            return handlers

        if not isinstance(protocol_version, tuple):
            raise TypeError("Protocol version must be specified as a tuple")

        if protocol_version in handlers:
            return {protocol_version: handlers[protocol_version]}

        return {}

    @classmethod
    def ping(cls, address, *, timeout=None, **config):
        """ Attempt to establish a Bolt connection, returning the
        agreed Bolt protocol version if successful.
        """
        config = PoolConfig.consume(config)
        try:
            s, protocol_version, handshake, data = connect(
                address,
                timeout=timeout,
                custom_resolver=config.resolver,
                ssl_context=config.get_ssl_context(),
                keep_alive=config.keep_alive,
            )
        except ServiceUnavailable:
            return None
        except BoltHandshakeError as e:
            return None
        else:
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
        :raise BoltHandshakeError: raised if the Bolt Protocol can not negotiate a protocol version.
        :raise ServiceUnavailable: raised if there was a connection issue.
        """
        config = PoolConfig.consume(config)
        s, config.protocol_version, handshake, data = connect(
            address,
            timeout=timeout,
            custom_resolver=config.resolver,
            ssl_context=config.get_ssl_context(),
            keep_alive=config.keep_alive,
        )

        if config.protocol_version == (3, 0):
            from neo4j.io._bolt3 import Bolt3
            connection = Bolt3(address, s, auth=auth, **config)
        elif config.protocol_version == (4, 0):
            from neo4j.io._bolt4x0 import Bolt4x0
            connection = Bolt4x0(address, s, auth=auth, **config)
        else:
            log.debug("[#%04X]  S: <CLOSE>", s.getpeername()[1])
            s.shutdown(SHUT_RDWR)
            s.close()

            supported_versions = Bolt.protocol_handlers().keys()
            raise BoltHandshakeError("The Neo4J server does not support communication with this driver. This driver have support for Bolt Protocols {}".format(supported_versions), address=address, request_data=handshake, response_data=data)

        connection.hello()
        return connection

    @property
    def encrypted(self):
        raise NotImplementedError

    @property
    def der_encoded_server_certificate(self):
        raise NotImplementedError

    @property
    def local_port(self):
        raise NotImplementedError

    def hello(self):
        raise NotImplementedError

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, query, parameters=None, mode=None, bookmarks=None, metadata=None,
            timeout=None, db=None, **handlers):
        """ Appends a RUN message to the output stream.

        :param query: Cypher query string
        :param parameters: dictionary of Cypher parameters
        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this transaction should begin
        :param metadata: custom metadata dictionary to attach to the transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """

    def discard(self, n=-1, qid=-1, **handlers):
        """ Appends a DISCARD message to the output stream.

        :param n: number of records to discard, default = -1 (ALL)
        :param qid: query ID to discard for, default = -1 (last query)
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """

    def pull(self, n=-1, qid=-1, **handlers):
        """ Appends a PULL message to the output stream.

        :param n: number of records to pull, default = -1 (ALL)
        :param qid: query ID to pull for, default = -1 (last query)
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None, db=None, **handlers):
        """ Appends a BEGIN message to the output stream.

        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this transaction should begin
        :param metadata: custom metadata dictionary to attach to the transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """

    def commit(self, **handlers):
        raise NotImplementedError

    def rollback(self, **handlers):
        raise NotImplementedError

    def reset(self):
        """ Add a RESET message to the outgoing queue, send
        it and consume all remaining messages.
        """
        raise NotImplementedError

    def send_all(self):
        """ Send all queued messages to the server.
        """
        raise NotImplementedError

    def fetch_message(self):
        """ Receive at least one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        raise NotImplementedError

    def timedout(self):
        raise NotImplementedError

    def fetch_all(self):
        """ Fetch all outstanding messages.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        raise NotImplementedError

    def close(self):
        """ Close the connection.
        """
        raise NotImplementedError

    def closed(self):
        raise NotImplementedError

    def defunct(self):
        raise NotImplementedError


class IOPool:
    """ A collection of connections to one or more server addresses.
    """
    from neo4j.conf import PoolConfig, WorkspaceConfig

    _default_acquire_timeout = WorkspaceConfig.connection_acquisition_timeout

    def __init__(self, opener, pool_config):
        assert callable(opener)
        assert isinstance(pool_config, PoolConfig)
        self.opener = opener
        self.pool_config = pool_config
        self.connections = {}
        self.lock = RLock()
        self.cond = Condition(self.lock)

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
                infinite_pool_size = (self.pool_config.max_connection_pool_size < 0 or self.pool_config.max_connection_pool_size == float("inf"))
                can_create_new_connection = infinite_pool_size or len(connections) < self.pool_config.max_connection_pool_size
                if can_create_new_connection:
                    timeout = min(self.pool_config.connection_timeout, time_remaining())
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
        raise WriteServiceUnavailable("No write service available for pool {}".format(self))

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

    def __init__(self, opener, pool_config, address):
        super(BoltPool, self).__init__(opener, pool_config)
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

    def __init__(self, opener, pool_config, addresses, routing_context):
        super(Neo4jPool, self).__init__(opener, pool_config)
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

    def fetch_routing_info(self, address, timeout=None):
        """ Fetch raw routing info from a given router address.

        :param address: router address
        :param timeout: seconds
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
            with self._acquire(address, timeout) as cx:
                _, _, server_version = (cx.server.agent or "").partition("/")
                log.debug("[#%04X]  C: <ROUTING> query=%r", cx.local_port, self.routing_context or {})
                cx.run("CALL dbms.cluster.routing.getRoutingTable($context)",
                       {"context": self.routing_context}, on_success=metadata.update, on_failure=fail)
                cx.pull(on_success=metadata.update, on_records=records.extend)
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

    def fetch_routing_table(self, address, timeout=None):
        """ Fetch a routing table from a given router address.

        :param address: router address
        :param timeout: seconds
        :return: a new RoutingTable instance or None if the given router is
                 currently unable to provide routing information
        :raise ServiceUnavailable: if no writers are available
        :raise BoltProtocolError: if the routing information received is unusable
        """
        new_routing_info = self.fetch_routing_info(address, timeout)
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
        from neo4j.api import READ_ACCESS
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
        from neo4j.api import READ_ACCESS
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
            if access_mode == READ_ACCESS:
                raise ReadServiceUnavailable("No read service currently available")
            else:
                raise WriteServiceUnavailable("No write service currently available")
        return choice(addresses_by_usage[min(addresses_by_usage)])

    def acquire(self, access_mode=None, timeout=None):
        from neo4j.api import check_access_mode
        access_mode = check_access_mode(access_mode)
        while True:
            try:
                address = self._select_address(access_mode)
            except (ReadServiceUnavailable, WriteServiceUnavailable) as err:
                raise SessionExpired("Failed to obtain connection towards '%s' server." % access_mode) from err
            try:
                connection = self._acquire(address, timeout=timeout)  # should always be a resolved address
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


def _connect(resolved_address, timeout, keep_alive):
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
            raise ValueError("Unsupported address {!r}".format(resolved_address))
        t = s.gettimeout()
        if timeout:
            s.settimeout(timeout)
        log.debug("[#0000]  C: <OPEN> %s", resolved_address)
        s.connect(resolved_address)
        s.settimeout(t)
        keep_alive = 1 if keep_alive else 0
        s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, keep_alive)
    except SocketTimeout:
        log.debug("[#0000]  C: <TIMEOUT> %s", resolved_address)
        log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
        s.close()
        raise ServiceUnavailable("Timed out trying to establish connection to {!r}".format(resolved_address))
    except OSError as error:
        log.debug("[#0000]  C: <ERROR> %s %s", type(error).__name__,
                  " ".join(map(repr, error.args)))
        log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
        s.close()
        raise ServiceUnavailable("Failed to establish connection to {!r} (reason {})".format(resolved_address, error))
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
        except (SSLError, OSError) as cause:
            s.close()
            error = BoltSecurityError(message="Failed to establish encrypted connection.", address=(host, local_port))
            error.__cause__ = cause
            raise error
        else:
            # Check that the server provides a certificate
            der_encoded_server_certificate = s.getpeercert(binary_form=True)
            if der_encoded_server_certificate is None:
                s.close()
                raise BoltProtocolError("When using an encrypted socket, the server should always provide a certificate", address=(host, local_port))
    return s


def _handshake(s, resolved_address):
    """

    :param s: Socket
    :param resolved_address:

    :return: (socket, version, client_handshake, server_response_data)
    """
    local_port = s.getsockname()[1]

    # TODO: Optimize logging code
    handshake = Bolt.get_handshake()
    import struct
    handshake = struct.unpack(">16B", handshake)
    handshake = [handshake[i:i + 4] for i in range(0, len(handshake), 4)]

    supported_versions = [("0x%02X%02X%02X%02X" % (vx[0], vx[1], vx[2], vx[3])) for vx in handshake]

    log.debug("[#%04X]  C: <MAGIC> 0x%08X", local_port, int.from_bytes(Bolt.MAGIC_PREAMBLE, byteorder="big"))
    log.debug("[#%04X]  C: <HANDSHAKE> %s %s %s %s", local_port, *supported_versions)

    data = Bolt.MAGIC_PREAMBLE + Bolt.get_handshake()
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
        raise BoltHandshakeError("Connection to {address} closed without handshake response".format(address=resolved_address), address=resolved_address, request_data=handshake, response_data=None)
    if data_size != 4:
        # Some garbled data has been received
        log.debug("[#%04X]  S: @*#!", local_port)
        s.close()
        raise BoltProtocolError("Expected four byte Bolt handshake response from %r, received %r instead; check for incorrect port number" % (resolved_address, data), address=resolved_address)
    elif data == b"HTTP":
        log.debug("[#%04X]  S: <CLOSE>", local_port)
        s.close()
        raise ServiceUnavailable("Cannot to connect to Bolt service on {!r} "
                                 "(looks like HTTP)".format(resolved_address))
    agreed_version = data[-1], data[-2]
    log.debug("[#%04X]  S: <HANDSHAKE> 0x%06X%02X", local_port, agreed_version[1], agreed_version[0])
    return s, agreed_version, handshake, data


def connect(address, *, timeout, custom_resolver, ssl_context, keep_alive):
    """ Connect and perform a handshake and return a valid Connection object,
    assuming a protocol version can be agreed.
    """
    last_error = None
    # Establish a connection to the host and port specified
    # Catches refused connections see:
    # https://docs.python.org/2/library/errno.html
    log.debug("[#0000]  C: <RESOLVE> %s", address)

    for resolved_address in Address(address).resolve(resolver=custom_resolver):
        s = None
        try:
            host = address[0]
            s = _connect(resolved_address, timeout, keep_alive)
            s = _secure(s, host, ssl_context)
            return _handshake(s, address)
        except Exception as error:
            if s:
                s.close()
            last_error = error
    if last_error is None:
        raise ServiceUnavailable("Failed to resolve addresses for %s" % address)
    else:
        raise last_error
