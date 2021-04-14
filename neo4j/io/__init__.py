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
    "Neo4jPool",
    "check_supported_server_product",
]


from collections import deque
from logging import getLogger
from random import choice
from select import select
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
    HAS_SNI,
    SSLError,
)
from threading import (
    Condition,
    Lock,
    RLock,
)
from time import perf_counter

from neo4j._exceptions import (
    BoltHandshakeError,
    BoltProtocolError,
    BoltRoutingError,
    BoltSecurityError,
)
from neo4j.addressing import Address
from neo4j.api import (
    READ_ACCESS,
    Version,
    WRITE_ACCESS,
)
from neo4j.conf import (
    PoolConfig,
    WorkspaceConfig,
)
from neo4j.exceptions import (
    ClientError,
    ConfigurationError,
    ReadServiceUnavailable,
    ServiceUnavailable,
    SessionExpired,
    UnsupportedServerProduct,
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
        from neo4j.io._bolt4 import Bolt4x0, Bolt4x1, Bolt4x2, Bolt4x3

        handlers = {
            Bolt3.PROTOCOL_VERSION: Bolt3,
            Bolt4x0.PROTOCOL_VERSION: Bolt4x0,
            Bolt4x1.PROTOCOL_VERSION: Bolt4x1,
            Bolt4x2.PROTOCOL_VERSION: Bolt4x2,
            Bolt4x3.PROTOCOL_VERSION: Bolt4x3,
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
        ranges_supported = versions[0] >= Version(4, 3)
        if versions and ranges_supported:
            start, end = 0, 0
            first_major = versions[start][0]
            minors = []
            for end, version in enumerate(versions):
                if version[0] == first_major:
                    minors.append(version[1])
                else:
                    break
            new_versions = ([Version(first_major, minors)] + versions[1:end])[:(limit - 1)]
            try:
                new_versions.append(versions[end])
            except IndexError:
                pass
            return new_versions
        else:
            return versions[:limit]

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
            s, protocol_version, handshake, data = connect(
                address,
                timeout=timeout,
                custom_resolver=config.resolver,
                ssl_context=config.get_ssl_context(),
                keep_alive=config.keep_alive,
            )
        except (ServiceUnavailable, SessionExpired, BoltHandshakeError):
            return None
        else:
            _close_socket(s)
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
        pool_config = PoolConfig.consume(pool_config)
        s, pool_config.protocol_version, handshake, data = connect(
            address,
            timeout=timeout,
            custom_resolver=pool_config.resolver,
            ssl_context=pool_config.get_ssl_context(),
            keep_alive=pool_config.keep_alive,
        )

        if pool_config.protocol_version == (3, 0):
            # Carry out Bolt subclass imports locally to avoid circular dependency issues.
            from neo4j.io._bolt3 import Bolt3
            connection = Bolt3(address, s, pool_config.max_connection_lifetime, auth=auth, user_agent=pool_config.user_agent, routing_context=routing_context)
        elif pool_config.protocol_version == (4, 0):
            # Carry out Bolt subclass imports locally to avoid circular dependency issues.
            from neo4j.io._bolt4 import Bolt4x0
            connection = Bolt4x0(address, s, pool_config.max_connection_lifetime, auth=auth, user_agent=pool_config.user_agent, routing_context=routing_context)
        elif pool_config.protocol_version == (4, 1):
            # Carry out Bolt subclass imports locally to avoid circular dependency issues.
            from neo4j.io._bolt4 import Bolt4x1
            connection = Bolt4x1(address, s, pool_config.max_connection_lifetime, auth=auth, user_agent=pool_config.user_agent, routing_context=routing_context)
        elif pool_config.protocol_version == (4, 2):
            # Carry out Bolt subclass imports locally to avoid circular dependency issues.
            from neo4j.io._bolt4 import Bolt4x2
            connection = Bolt4x2(address, s, pool_config.max_connection_lifetime, auth=auth, user_agent=pool_config.user_agent, routing_context=routing_context)
        elif pool_config.protocol_version == (4, 3):
            # Carry out Bolt subclass imports locally to avoid circular dependency issues.
            from neo4j.io._bolt4 import Bolt4x3
            connection = Bolt4x3(address, s, pool_config.max_connection_lifetime, auth=auth, user_agent=pool_config.user_agent, routing_context=routing_context)
        else:
            log.debug("[#%04X]  S: <CLOSE>", s.getpeername()[1])
            _close_socket(s)

            supported_versions = Bolt.protocol_handlers().keys()
            raise BoltHandshakeError("The Neo4J server does not support communication with this driver. This driver have support for Bolt Protocols {}".format(supported_versions), address=address, request_data=handshake, response_data=data)

        try:
            connection.hello()
        except Exception:
            connection.close()
            raise

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

    def route(self, database=None, bookmarks=None):
        """ Fetch a routing table from the server for the given
        `database`. For Bolt 4.3 and above, this appends a ROUTE
        message; for earlier versions, a procedure call is made via
        the regular Cypher execution mechanism. In all cases, this is
        sent to the network, and a response is fetched.

        :param database: database for which to fetch a routing table
        :param bookmarks: iterable of bookmark values after which this
                          transaction should begin
        :return: dictionary of raw routing data
        """

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

    def __init__(self, opener, pool_config, workspace_config):
        assert callable(opener)
        assert isinstance(pool_config, PoolConfig)
        assert isinstance(workspace_config, WorkspaceConfig)

        self.opener = opener
        self.pool_config = pool_config
        self.workspace_config = workspace_config
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
            timeout = self.workspace_config.connection_acquisition_timeout

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

    def acquire(self, access_mode=None, timeout=None, database=None,
                bookmarks=None):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param access_mode:
        :param timeout:
        :param database:
        :param bookmarks:
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
                    except OSError:
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
                except OSError:
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

    def acquire(self, access_mode=None, timeout=None, database=None,
                bookmarks=None):
        # The access_mode and database is not needed for a direct connection, its just there for consistency.
        return self._acquire(self.address, timeout)


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
        :param addresses:
        """
        super(Neo4jPool, self).__init__(opener, pool_config, workspace_config)
        # Each database have a routing table, the default database is a special case.
        log.debug("[#0000]  C: <NEO4J POOL> routing address %r", address)
        self.address = address
        self.routing_tables = {workspace_config.database: RoutingTable(database=workspace_config.database, routers=[address])}
        self.refresh_lock = Lock()

    def __repr__(self):
        """ The representation shows the initial routing addresses.

        :return: The representation
        :rtype: str
        """
        return "<{} addresses={!r}>".format(self.__class__.__name__, self.get_default_database_initial_router_addresses())

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

    def create_routing_table(self, database):
        if database not in self.routing_tables:
            self.routing_tables[database] = RoutingTable(database=database, routers=self.get_default_database_initial_router_addresses())

    def fetch_routing_info(self, address, database, bookmarks, timeout):
        """ Fetch raw routing info from a given router address.

        :param address: router address
        :param database: the database name to get routing table for
        :param bookmarks: iterable of bookmark values after which the routing
                          info should be fetched
        :param timeout: connection acquisition timeout in seconds

        :return: list of routing records, or None if no connection
            could be established or if no readers or writers are present
        :raise ServiceUnavailable: if the server does not support
            routing, or if routing support is broken or outdated
        """
        try:
            cx = self._acquire(address, timeout)
            try:
                routing_table = cx.route(
                    database or self.workspace_config.database, bookmarks
                )
            finally:
                self.release(cx)
        except BoltRoutingError as error:
            # Connection was successful, but routing support is
            # broken. This may indicate that the routing procedure
            # does not exist (for protocol versions prior to 4.3).
            # This error is converted into ServiceUnavailable,
            # therefore surfacing to the application as a signal that
            # routing is broken.
            log.debug("Routing is broken (%s)", error)
            raise ServiceUnavailable(*error.args)
        except ServiceUnavailable as error:
            # The routing table request suffered a connection
            # failure. This should return a null routing table,
            # signalling to the caller to retry the request
            # elsewhere.
            log.debug("Routing is unavailable (%s)", error)
            routing_table = None
        # If the routing table is empty, deactivate the address.
        if not routing_table:
            self.deactivate(address)
        return routing_table

    def fetch_routing_table(self, *, address, timeout, database, bookmarks):
        """ Fetch a routing table from a given router address.

        :param address: router address
        :param timeout: seconds
        :param database: the database name
        :type: str
        :param bookmarks: bookmarks used when fetching routing table

        :return: a new RoutingTable instance or None if the given router is
                 currently unable to provide routing information

        :raise neo4j.exceptions.ServiceUnavailable: if no writers are available
        :raise neo4j._exceptions.BoltProtocolError: if the routing information received is unusable
        """
        new_routing_info = self.fetch_routing_info(address, database, bookmarks,
                                                   timeout)
        if new_routing_info is None:
            return None
        elif not new_routing_info:
            raise BoltRoutingError("Invalid routing table", address)
        else:
            servers = new_routing_info[0]["servers"]
            ttl = new_routing_info[0]["ttl"]
            new_routing_table = RoutingTable.parse_routing_info(database=database, servers=servers, ttl=ttl)

        # Parse routing info and count the number of each type of server
        num_routers = len(new_routing_table.routers)
        num_readers = len(new_routing_table.readers)

        # num_writers = len(new_routing_table.writers)
        # If no writers are available. This likely indicates a temporary state,
        # such as leader switching, so we should not signal an error.

        # No routers
        if num_routers == 0:
            raise BoltRoutingError("No routing servers returned from server", address)

        # No readers
        if num_readers == 0:
            raise BoltRoutingError("No read servers returned from server", address)

        # At least one of each is fine, so return this table
        return new_routing_table

    def update_routing_table_from(self, *routers, database=None,
                                  bookmarks=None):
        """ Try to update routing tables with the given routers.

        :return: True if the routing table is successfully updated,
        otherwise False
        """
        log.debug("Attempting to update routing table from {}".format(", ".join(map(repr, routers))))
        for router in routers:
            new_routing_table = self.fetch_routing_table(
                address=router, timeout=self.pool_config.connection_timeout,
                database=database, bookmarks=bookmarks
            )
            if new_routing_table is not None:
                self.routing_tables[database].update(new_routing_table)
                log.debug("[#0000]  C: <UPDATE ROUTING TABLE> address={!r} ({!r})".format(router, self.routing_tables[database]))
                return True
        return False

    def update_routing_table(self, *, database, bookmarks):
        """ Update the routing table from the first router able to provide
        valid routing information.

        :param database: The database name
        :param bookmarks: bookmarks used when fetching routing table

        :raise neo4j.exceptions.ServiceUnavailable:
        """
        # copied because it can be modified
        existing_routers = list(self.routing_tables[database].routers)

        has_tried_initial_routers = False
        if self.routing_tables[database].missing_fresh_writer():
            # TODO: Test this state
            has_tried_initial_routers = True
            if self.update_routing_table_from(
                    self.first_initial_routing_address, database=database,
                    bookmarks=bookmarks
            ):
                # Why is only the first initial routing address used?
                return
        if self.update_routing_table_from(*existing_routers, database=database,
                                          bookmarks=bookmarks):
            return

        if (not has_tried_initial_routers
                and self.first_initial_routing_address not in existing_routers):
            if self.update_routing_table_from(
                self.first_initial_routing_address, database=database,
                bookmarks=bookmarks
            ):
                # Why is only the first initial routing address used?
                return

        # None of the routers have been successful, so just fail
        log.error("Unable to retrieve routing information")
        raise ServiceUnavailable("Unable to retrieve routing information")

    def update_connection_pool(self, *, database):
        servers = self.routing_tables[database].servers()
        for address in list(self.connections):
            if address not in servers:
                super(Neo4jPool, self).deactivate(address)

    def ensure_routing_table_is_fresh(self, *, access_mode, database,
                                      bookmarks):
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
        if self.routing_tables[database].is_fresh(readonly=(access_mode == READ_ACCESS)):
            # Readers are fresh.
            return False
        with self.refresh_lock:

            self.update_routing_table(database=database, bookmarks=bookmarks)
            self.update_connection_pool(database=database)

            for database in list(self.routing_tables.keys()):
                # Remove unused databases in the routing table
                # Remove the routing table after a timeout = TTL + 30s
                log.debug("[#0000]  C: <ROUTING AGED> database=%s", database)
                if self.routing_tables[database].should_be_purged_from_memory() and database != self.workspace_config.database:
                    del self.routing_tables[database]

            return True

    def _select_address(self, *, access_mode, database, bookmarks):
        from neo4j.api import READ_ACCESS
        """ Selects the address with the fewest in-use connections.
        """
        self.create_routing_table(database)
        self.ensure_routing_table_is_fresh(
            access_mode=access_mode, database=database, bookmarks=bookmarks
        )
        log.debug("[#0000]  C: <ROUTING TABLE ENSURE FRESH> %r", self.routing_tables)
        if access_mode == READ_ACCESS:
            addresses = self.routing_tables[database].readers
        else:
            addresses = self.routing_tables[database].writers
        addresses_by_usage = {}
        for address in addresses:
            addresses_by_usage.setdefault(self.in_use_connection_count(address), []).append(address)
        if not addresses_by_usage:
            if access_mode == READ_ACCESS:
                raise ReadServiceUnavailable("No read service currently available")
            else:
                raise WriteServiceUnavailable("No write service currently available")
        return choice(addresses_by_usage[min(addresses_by_usage)])

    def acquire(self, access_mode=None, timeout=None, database=None,
                bookmarks=None):
        if access_mode not in (WRITE_ACCESS, READ_ACCESS):
            raise ClientError("Non valid 'access_mode'; {}".format(access_mode))
        if not timeout:
            raise ClientError("'timeout' must be a float larger than 0; {}".format(timeout))

        from neo4j.api import check_access_mode
        access_mode = check_access_mode(access_mode)
        while True:
            try:
                # Get an address for a connection that have the fewest in-use connections.
                address = self._select_address(
                    access_mode=access_mode, database=database,
                    bookmarks=bookmarks
                )
                log.debug("[#0000]  C: <ACQUIRE ADDRESS> database=%r address=%r", database, address)
            except (ReadServiceUnavailable, WriteServiceUnavailable) as err:
                raise SessionExpired("Failed to obtain connection towards '%s' server." % access_mode) from err
            try:
                connection = self._acquire(address, timeout=timeout)  # should always be a resolved address
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
        _close_socket(s)
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
            _close_socket(s)
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
        _close_socket(s)
        raise ServiceUnavailable("Connection to {address} closed without handshake response".format(address=resolved_address))
    if data_size != 4:
        # Some garbled data has been received
        log.debug("[#%04X]  S: @*#!", local_port)
        s.close()
        raise BoltProtocolError("Expected four byte Bolt handshake response from %r, received %r instead; check for incorrect port number" % (resolved_address, data), address=resolved_address)
    elif data == b"HTTP":
        log.debug("[#%04X]  S: <CLOSE>", local_port)
        _close_socket(s)
        raise ServiceUnavailable("Cannot to connect to Bolt service on {!r} "
                                 "(looks like HTTP)".format(resolved_address))
    agreed_version = data[-1], data[-2]
    log.debug("[#%04X]  S: <HANDSHAKE> 0x%06X%02X", local_port, agreed_version[1], agreed_version[0])
    return s, agreed_version, handshake, data


def _close_socket(socket_):
    try:
        socket_.shutdown(SHUT_RDWR)
        socket_.close()
    except OSError:
        pass


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
                _close_socket(s)
            last_error = error
    if last_error is None:
        raise ServiceUnavailable("Failed to resolve addresses for %s" % address)
    else:
        raise last_error


def check_supported_server_product(agent):
    """ Checks that a server product is supported by the driver by
    looking at the server agent string.

    :param agent: server agent string to check for validity
    :raises UnsupportedServerProduct: if the product is not supported
    """
    if not agent.startswith("Neo4j/"):
        raise UnsupportedServerProduct(agent)
