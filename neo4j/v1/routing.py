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


from threading import Lock
from time import clock

from neo4j.addressing import SocketAddress, resolve
from neo4j.bolt import ConnectionPool, ServiceUnavailable, ProtocolError, DEFAULT_PORT, connect
from neo4j.compat.collections import MutableSet, OrderedDict
from neo4j.exceptions import CypherError
from neo4j.v1.api import Driver, READ_ACCESS, WRITE_ACCESS
from neo4j.v1.exceptions import SessionExpired
from neo4j.v1.security import SecurityPlan
from neo4j.v1.session import BoltSession


class RoundRobinSet(MutableSet):

    def __init__(self, elements=()):
        self._elements = OrderedDict.fromkeys(elements)
        self._current = None

    def __repr__(self):
        return "{%s}" % ", ".join(map(repr, self._elements))

    def __contains__(self, element):
        return element in self._elements

    def __next__(self):
        current = None
        if self._elements:
            if self._current is None:
                self._current = 0
            else:
                self._current = (self._current + 1) % len(self._elements)
            current = list(self._elements.keys())[self._current]
        return current

    def __iter__(self):
        return iter(self._elements)

    def __len__(self):
        return len(self._elements)

    def add(self, element):
        self._elements[element] = None

    def clear(self):
        self._elements.clear()

    def discard(self, element):
        try:
            del self._elements[element]
        except KeyError:
            pass

    def next(self):
        return self.__next__()

    def remove(self, element):
        try:
            del self._elements[element]
        except KeyError:
            raise ValueError(element)

    def update(self, elements=()):
        self._elements.update(OrderedDict.fromkeys(elements))

    def replace(self, elements=()):
        e = self._elements
        e.clear()
        e.update(OrderedDict.fromkeys(elements))


class RoutingTable(object):

    timer = clock

    @classmethod
    def parse_routing_info(cls, records):
        """ Parse the records returned from a getServers call and
        return a new RoutingTable instance.
        """
        if len(records) != 1:
            raise ProtocolError("Expected exactly one record")
        record = records[0]
        routers = []
        readers = []
        writers = []
        try:
            servers = record["servers"]
            for server in servers:
                role = server["role"]
                addresses = []
                for address in server["addresses"]:
                    addresses.extend(resolve(SocketAddress.parse(address, DEFAULT_PORT)))
                if role == "ROUTE":
                    routers.extend(addresses)
                elif role == "READ":
                    readers.extend(addresses)
                elif role == "WRITE":
                    writers.extend(addresses)
            ttl = record["ttl"]
        except (KeyError, TypeError):
            raise ProtocolError("Cannot parse routing info")
        else:
            return cls(routers, readers, writers, ttl)

    def __init__(self, routers=(), readers=(), writers=(), ttl=0):
        self.routers = RoundRobinSet(routers)
        self.readers = RoundRobinSet(readers)
        self.writers = RoundRobinSet(writers)
        self.last_updated_time = self.timer()
        self.ttl = ttl

    def is_fresh(self):
        """ Indicator for whether routing information is still usable.
        """
        expired = self.last_updated_time + self.ttl <= self.timer()
        return not expired and len(self.routers) > 1 and self.readers and self.writers

    def update(self, new_routing_table):
        """ Update the current routing table with new routing information
        from a replacement table.
        """
        self.routers.replace(new_routing_table.routers)
        self.readers.replace(new_routing_table.readers)
        self.writers.replace(new_routing_table.writers)
        self.last_updated_time = self.timer()
        self.ttl = new_routing_table.ttl


class RoutingConnectionPool(ConnectionPool):
    """ Connection pool with routing table.
    """

    routing_info_procedure = "dbms.cluster.routing.getServers"

    def __init__(self, connector, *routers):
        super(RoutingConnectionPool, self).__init__(connector)
        self.routing_table = RoutingTable(routers)
        self.refresh_lock = Lock()

    def fetch_routing_info(self, address):
        """ Fetch raw routing info from a given router address.

        :param address: router address
        :return: list of routing records or
                 None if no connection could be established
        :raise ServiceUnavailable: if the server does not support routing or
                                   if routing support is broken
        """
        try:
            with BoltSession(lambda _: self.acquire_direct(address)) as session:
                return list(session.run("CALL %s" % self.routing_info_procedure))
        except CypherError as error:
            if error.code == "Neo.ClientError.Procedure.ProcedureNotFound":
                raise ServiceUnavailable("Server {!r} does not support routing".format(address))
            else:
                raise ServiceUnavailable("Routing support broken on server {!r}".format(address))
        except ServiceUnavailable:
            self.remove(address)
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

        # Parse routing info and count the number of each type of server
        new_routing_table = RoutingTable.parse_routing_info(new_routing_info)
        num_routers = len(new_routing_table.routers)
        num_readers = len(new_routing_table.readers)
        num_writers = len(new_routing_table.writers)

        # No routers
        if num_routers == 0:
            raise ProtocolError("No routing servers returned from server %r" % (address,))

        # No readers
        if num_readers == 0:
            raise ProtocolError("No read servers returned from server %r" % (address,))

        # No writers
        if num_writers == 0:
            # No writers are available. This likely indicates a temporary state,
            # such as leader switching, so we should not signal an error.
            return None

        # At least one of each is fine, so return this table
        return new_routing_table

    def update_routing_table(self):
        """ Update the routing table from the first router able to provide
        valid routing information.
        """
        # copied because it can be modified
        copy_of_routers = list(self.routing_table.routers)
        for router in copy_of_routers:
            new_routing_table = self.fetch_routing_table(router)
            if new_routing_table is not None:
                self.routing_table.update(new_routing_table)
                return

        # None of the routers have been successful, so just fail
        raise ServiceUnavailable("Unable to retrieve routing information")

    def refresh_routing_table(self):
        """ Update the routing table if stale.

        This method performs two freshness checks, before and after acquiring
        the refresh lock. If the routing table is already fresh on entry, the
        method exits immediately; otherwise, the refresh lock is acquired and
        the second freshness check that follows determines whether an update
        is still required.

        This method is thread-safe.

        :return: `True` if an update was required, `False` otherwise.
        """
        if self.routing_table.is_fresh():
            return False
        with self.refresh_lock:
            if self.routing_table.is_fresh():
                return False
            self.update_routing_table()
            return True

    def acquire(self, access_mode=None):
        if access_mode is None:
            access_mode = WRITE_ACCESS
        if access_mode == READ_ACCESS:
            server_list = self.routing_table.readers
        elif access_mode == WRITE_ACCESS:
            server_list = self.routing_table.writers
        else:
            raise ValueError("Unsupported access mode {}".format(access_mode))
        while True:
            address = None
            while address is None:
                self.refresh_routing_table()
                address = next(server_list)
            try:
                connection = self.acquire_direct(address)  # should always be a resolved address
                connection.Error = SessionExpired
            except ServiceUnavailable:
                self.remove(address)
            else:
                return connection

    def remove(self, address):
        """ Remove an address from the connection pool, if present, closing
        all connections to that address. Also remove from the routing table.
        """
        # We use `discard` instead of `remove` here since the former
        # will not fail if the address has already been removed.
        self.routing_table.routers.discard(address)
        self.routing_table.readers.discard(address)
        self.routing_table.writers.discard(address)
        super(RoutingConnectionPool, self).remove(address)


class RoutingDriver(Driver):
    """ A :class:`.RoutingDriver` is created from a ``bolt+routing`` URI. The
    routing behaviour works in tandem with Neo4j's causal clustering feature
    by directing read and write behaviour to appropriate cluster members.
    """

    def __init__(self, uri, **config):
        self.initial_address = initial_address = SocketAddress.from_uri(uri, DEFAULT_PORT)
        self.security_plan = security_plan = SecurityPlan.build(**config)
        self.encrypted = security_plan.encrypted
        if not security_plan.routing_compatible:
            # this error message is case-specific as there is only one incompatible
            # scenario right now
            raise ValueError("TRUST_ON_FIRST_USE is not compatible with routing")

        def connector(a):
            return connect(a, security_plan.ssl_context, **config)

        pool = RoutingConnectionPool(connector, *resolve(initial_address))
        try:
            pool.update_routing_table()
        except:
            pool.close()
            raise
        else:
            Driver.__init__(self, pool, **config)

    def session(self, access_mode=None, bookmark=None):
        return BoltSession(self._pool.acquire, self._max_retry_time, access_mode=access_mode, bookmark=bookmark)
