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


from logging import getLogger
from sys import maxsize
from threading import Lock

from neo4j import READ_ACCESS, WRITE_ACCESS
from neo4j.errors import BoltRoutingError
from neo4j.bio.direct import AbstractConnectionPool
from neo4j.exceptions import ConnectionExpired, ServiceUnavailable
from neo4j.aio.bolt3 import RoutingTable


log = getLogger("neobolt")


class LeastConnectedLoadBalancingStrategy:

    def __init__(self, connection_pool):
        self._readers_offset = 0
        self._writers_offset = 0
        self._connection_pool = connection_pool

    def select_reader(self, known_readers):
        address = self._select(self._readers_offset, known_readers)
        self._readers_offset += 1
        return address

    def select_writer(self, known_writers):
        address = self._select(self._writers_offset, known_writers)
        self._writers_offset += 1
        return address

    def _select(self, offset, addresses):
        if not addresses:
            return None
        num_addresses = len(addresses)
        start_index = offset % num_addresses
        index = start_index

        least_connected_address = None
        least_in_use_connections = maxsize

        while True:
            address = addresses[index]
            index = (index + 1) % num_addresses

            in_use_connections = self._connection_pool.in_use_connection_count(address)

            if in_use_connections < least_in_use_connections:
                least_connected_address = address
                least_in_use_connections = in_use_connections

            if index == start_index:
                return least_connected_address


class RoutingConnectionPool(AbstractConnectionPool):
    """ Connection pool with routing table.
    """

    def __init__(self, connector, initial_address, routing_context, *routers, **config):
        super(RoutingConnectionPool, self).__init__(connector, **config)
        self.initial_address = initial_address
        self.routing_context = routing_context
        self.routing_table = RoutingTable(routers)
        self.missing_writer = False
        self.refresh_lock = Lock()
        self.load_balancing_strategy = LeastConnectedLoadBalancingStrategy(connection_pool=self)

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
            with self.acquire_direct(address) as cx:
                _, _, server_version = (cx.server.agent or "").partition("/")
                log.debug("[#%04X]  C: <ROUTING> query=%r", cx.local_port, self.routing_context or {})
                cx.run("CALL dbms.cluster.routing.getRoutingTable({context})",
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
                super(RoutingConnectionPool, self).deactivate(address)

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

    def acquire(self, access_mode=None):
        if access_mode is None:
            access_mode = WRITE_ACCESS
        if access_mode == READ_ACCESS:
            server_list = self.routing_table.readers
            server_selector = self.load_balancing_strategy.select_reader
        elif access_mode == WRITE_ACCESS:
            server_list = self.routing_table.writers
            server_selector = self.load_balancing_strategy.select_writer
        else:
            raise ValueError("Unsupported access mode {}".format(access_mode))

        self.ensure_routing_table_is_fresh(access_mode)
        while True:
            address = server_selector(server_list)
            if address is None:
                break
            try:
                connection = self.acquire_direct(address)  # should always be a resolved address
                connection.Error = ConnectionExpired
            except ServiceUnavailable:
                self.deactivate(address)
            else:
                return connection
        raise ConnectionExpired("Failed to obtain connection towards '%s' server." % access_mode)

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
        super(RoutingConnectionPool, self).deactivate(address)

    def remove_writer(self, address):
        """ Remove a writer address from the routing table, if present.
        """
        log.debug("[#0000]  C: <ROUTING> Removing writer %r", address)
        self.routing_table.writers.discard(address)
        log.debug("[#0000]  C: <ROUTING> table=%r", self.routing_table)
