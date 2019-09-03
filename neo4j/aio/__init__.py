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


from asyncio import run

from neo4j.addressing import Address
from neo4j.aio.bolt import Bolt, BoltPool
from neo4j.bolt.routing import RoutingTable


class Neo4jPool:

    def __init__(self, opener, addresses=None):
        self._pools = [BoltPool(opener, address) for address in addresses or ()]

    async def acquire(self):
        candidates = {}
        for pool in self._pools:
            candidates[pool.in_use] = pool
        return await candidates[min(candidates.keys())].acquire()

    def release(self, cx):
        for pool in self._pools:
            if cx in pool:
                pool.release(cx)


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
            self._routing_table = RoutingTable.parse_routing_info([record])
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
