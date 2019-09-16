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


from asyncio import run, Lock
from logging import getLogger
from random import choice

from neo4j.addressing import Address
from neo4j.aio.bolt import Bolt, BoltPool
from neo4j.aio.bolt.v3 import RoutingTable
from neo4j.errors import BoltError, Neo4jAvailabilityError


log = getLogger(__name__)


class Neo4jPool:
    """ Connection pool with routing table.
    """

    @classmethod
    async def open(cls, opener, *addresses, routing_context=None, max_size_per_host=100):
        # TODO: get initial routing table and construct
        obj = cls(opener, *addresses, routing_context=routing_context, max_size_per_host=max_size_per_host)
        await obj._ensure_routing_table_is_fresh()
        return obj

    def __init__(self, opener, *addresses, routing_context=None, max_size_per_host=100):
        self._pools = {}
        self._missing_writer = False
        self._refresh_lock = Lock()
        self._opener = opener
        self._routing_context = routing_context
        self._max_size_per_host = max_size_per_host
        self._initial_routers = addresses
        self._routing_table = RoutingTable(addresses)
        self._activate_new_pools_in(self._routing_table)

    def _activate_new_pools_in(self, routing_table):
        """ Add pools for addresses that exist in the given routing
        table but which don't already have pools.
        """
        for address in routing_table.servers():
            if address not in self._pools:
                self._pools[address] = BoltPool(self._opener, address,
                                                max_size=self._max_size_per_host)

    async def _deactivate_pools_not_in(self, routing_table):
        """ Deactivate any pools that aren't represented in the given
        routing table.
        """
        for address in self._pools:
            if address not in routing_table:
                await self._deactivate(address)

    async def _get_routing_table_from(self, *routers):
        """ Try to update routing tables with the given routers.

        :return: True if the routing table is successfully updated,
        otherwise False
        """
        log.debug("Attempting to update routing table from "
                  "{}".format(", ".join(map(repr, routers))))
        for router in routers:
            pool = self._pools[router]
            cx = await pool.acquire()
            try:
                new_routing_table = await cx.get_routing_table(self._routing_context)
            except BoltError:
                await self._deactivate(router)
            else:
                num_routers = len(new_routing_table.routers)
                num_readers = len(new_routing_table.readers)
                num_writers = len(new_routing_table.writers)

                # No writers are available. This likely indicates a temporary state,
                # such as leader switching, so we should not signal an error.
                # When no writers available, then we flag we are reading in absence of writer
                self._missing_writer = (num_writers == 0)

                # No routers
                if num_routers == 0:
                    continue

                # No readers
                if num_readers == 0:
                    continue

                log.debug("Successfully updated routing table from "
                          "{!r} ({!r})".format(router, self._routing_table))
                return new_routing_table
            finally:
                await pool.release(cx)
        return None

    async def _get_routing_table(self):
        """ Update the routing table from the first router able to provide
        valid routing information.
        """
        # copied because it can be modified
        existing_routers = list(self._routing_table.routers)

        has_tried_initial_routers = False
        if self._missing_writer:
            has_tried_initial_routers = True
            rt = await self._get_routing_table_from(self._initial_routers)
            if rt:
                return rt

        rt = await self._get_routing_table_from(*existing_routers)
        if rt:
            return rt

        if not has_tried_initial_routers and self._initial_routers not in existing_routers:
            rt = await self._get_routing_table_from(self._initial_routers)
            if rt:
                return rt

        # None of the routers have been successful, so just fail
        log.error("Unable to retrieve routing information")
        raise Neo4jAvailabilityError("Unable to retrieve routing information")

    async def _ensure_routing_table_is_fresh(self, readonly=False):
        """ Update the routing table if stale.

        This method performs two freshness checks, before and after acquiring
        the refresh lock. If the routing table is already fresh on entry, the
        method exits immediately; otherwise, the refresh lock is acquired and
        the second freshness check that follows determines whether an update
        is still required.
        """
        if self._routing_table.is_fresh(readonly=readonly):
            return
        async with self._refresh_lock:
            if self._routing_table.is_fresh(readonly=readonly):
                if readonly:
                    # if reader is fresh but writers are not, then
                    # we are reading in absence of writer
                    self._missing_writer = not self._routing_table.is_fresh(readonly=False)
            else:
                rt = await self._get_routing_table()
                self._activate_new_pools_in(rt)
                self._routing_table.update(rt)
                await self._deactivate_pools_not_in(rt)

    async def _select_pool(self, readonly=False):
        """ Selects the pool with the fewest in-use connections.
        """
        await self._ensure_routing_table_is_fresh(readonly=readonly)
        if readonly:
            addresses = self._routing_table.readers
        else:
            addresses = self._routing_table.writers
        pools = [pool for address, pool in self._pools.items() if address in addresses]
        pools_by_usage = {}
        for pool in pools:
            pools_by_usage.setdefault(pool.in_use, []).append(pool)
        if not pools_by_usage:
            raise Neo4jAvailabilityError("No {} service currently "
                                         "available".format("read" if readonly else "write"))
        return choice(pools_by_usage[min(pools_by_usage)])

    async def acquire(self, readonly=False, force_reset=False):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param readonly: true if a readonly connection is required,
            otherwise false
        :param force_reset:
        """
        while True:
            pool = await self._select_pool(readonly=readonly)
            try:
                cx = await pool.acquire(force_reset=force_reset)
            except BoltError:
                await self._deactivate(pool.address)
            else:
                if not readonly:
                    # If we're not acquiring a connection as
                    # readonly, then intercept NotALeader and
                    # ForbiddenOnReadOnlyDatabase errors to
                    # invalidate the routing table.
                    from neo4j.errors import (
                        NotALeader,
                        ForbiddenOnReadOnlyDatabase,
                    )

                    def handler(failure):
                        """ Invalidate the routing table before raising the failure.
                        """
                        log.debug("[#0000]  C: <ROUTING> Invalidating routing table")
                        self._routing_table.ttl = 0
                        raise failure

                    cx.set_failure_handler(NotALeader, handler)
                    cx.set_failure_handler(ForbiddenOnReadOnlyDatabase, handler)
                return cx

    async def release(self, connection, force_reset=False):
        """ Release a connection back into the pool.
        This method is thread safe.
        """
        for pool in self._pools.values():
            try:
                await pool.release(connection, force_reset=force_reset)
            except ValueError:
                pass
            else:
                # Unhook any custom error handling and exit.
                from neo4j.errors import (
                    NotALeader,
                    ForbiddenOnReadOnlyDatabase,
                )
                connection.del_failure_handler(NotALeader)
                connection.del_failure_handler(ForbiddenOnReadOnlyDatabase)
                break
        else:
            raise ValueError("Connection does not belong to this pool")

    async def _deactivate(self, address):
        """ Deactivate an address from the connection pool,
        if present, remove from the routing table and also closing
        all idle connections to that address.
        """
        log.debug("[#0000]  C: <ROUTING> Deactivating address %r", address)
        # We use `discard` instead of `remove` here since the former
        # will not fail if the address has already been removed.
        self._routing_table.routers.discard(address)
        self._routing_table.readers.discard(address)
        self._routing_table.writers.discard(address)
        log.debug("[#0000]  C: <ROUTING> table=%r", self._routing_table)
        try:
            pool = self._pools.pop(address)
        except KeyError:
            pass  # assume the address has already been removed
        else:
            pool.max_size = 0
            await pool.prune()

    async def close(self, force=False):
        """ Close all connections and empty the pool. If forced, in-use
        connections will be closed immediately; if not, they will
        remain open until released.
        """
        pools = dict(self._pools)
        self._pools.clear()
        for address, pool in pools.items():
            if force:
                await pool.close()
            else:
                pool.max_size = 0
                await pool.prune()


class Neo4j:

    # The default router address list to use if no addresses are specified.
    default_router_addresses = Address.parse_list(":7687 :17601 :17687")

    @classmethod
    async def open(cls, *addresses, auth=None, security=False, protocol_version=None, loop=None):
        opener = Bolt.opener(auth=auth,
                             security=security,
                             protocol_version=protocol_version,
                             loop=loop)
        router_addresses = Address.parse_list(" ".join(addresses), default_port=7687)
        return cls(opener, router_addresses)

    def __init__(self, opener, router_addresses):
        self._routers = Neo4jPool(opener, router_addresses or self.default_router_addresses)
        self._writers = Neo4jPool(opener)
        self._readers = Neo4jPool(opener)
        self._routing_table = None

    @property
    def routing_table(self):
        return self._routing_table

    async def update_routing_table(self):
        cx = await self._routers.acquire()
        try:
            result = await cx.run("CALL dbms.cluster.routing.getRoutingTable({context})", {"context": {}})
            record = await result.single()
            self._routing_table = RoutingTable.parse_routing_info([record])  # TODO: handle ValueError?
            return self._routing_table
        finally:
            self._routers.release(cx)


async def main():
    from neo4j.debug import watch; watch("neo4j")
    neo4j = await Neo4j.open(":17601 :17602 :17603", auth=("neo4j", "password"))
    await neo4j.update_routing_table()
    print(neo4j.routing_table)


if __name__ == "__main__":
    run(main())
