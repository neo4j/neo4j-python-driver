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


from collections.abc import MutableSet
from logging import getLogger
from time import perf_counter

from .addressing import Address


log = getLogger("neo4j")


class OrderedSet(MutableSet):

    def __init__(self, elements=()):
        # dicts keep insertion order starting with Python 3.7
        self._elements = dict.fromkeys(elements)
        self._current = None

    def __repr__(self):
        return "{%s}" % ", ".join(map(repr, self._elements))

    def __contains__(self, element):
        return element in self._elements

    def __iter__(self):
        return iter(self._elements)

    def __len__(self):
        return len(self._elements)

    def __getitem__(self, index):
        return list(self._elements.keys())[index]

    def add(self, element):
        self._elements[element] = None

    def clear(self):
        self._elements.clear()

    def discard(self, element):
        try:
            del self._elements[element]
        except KeyError:
            pass

    def remove(self, element):
        try:
            del self._elements[element]
        except KeyError:
            raise ValueError(element) from None

    def update(self, elements=()):
        self._elements.update(dict.fromkeys(elements))

    def replace(self, elements=()):
        e = self._elements
        e.clear()
        e.update(dict.fromkeys(elements))


class RoutingTable:

    @classmethod
    def parse_routing_info(cls, *, database, servers, ttl):
        """ Parse the records returned from the procedure call and
        return a new RoutingTable instance.
        """
        routers = []
        readers = []
        writers = []
        try:
            for server in servers:
                role = server["role"]
                addresses = []
                for address in server["addresses"]:
                    addresses.append(Address.parse(address, default_port=7687))
                if role == "ROUTE":
                    routers.extend(addresses)
                elif role == "READ":
                    readers.extend(addresses)
                elif role == "WRITE":
                    writers.extend(addresses)
        except (KeyError, TypeError) as exc:
            raise ValueError("Cannot parse routing info") from exc
        else:
            return cls(database=database, routers=routers, readers=readers, writers=writers, ttl=ttl)

    def __init__(self, *, database, routers=(), readers=(), writers=(), ttl=0):
        self.initial_routers = OrderedSet(routers)
        self.routers = OrderedSet(routers)
        self.readers = OrderedSet(readers)
        self.writers = OrderedSet(writers)
        self.initialized_without_writers = not self.writers
        self.last_updated_time = perf_counter()
        self.ttl = ttl
        self.database = database

    def __repr__(self):
        return "RoutingTable(database=%r routers=%r, readers=%r, writers=%r, last_updated_time=%r, ttl=%r)" % (
            self.database,
            self.routers,
            self.readers,
            self.writers,
            self.last_updated_time,
            self.ttl,
        )

    def __contains__(self, address):
        return address in self.routers or address in self.readers or address in self.writers

    def is_fresh(self, readonly=False):
        """ Indicator for whether routing information is still usable.
        """
        assert isinstance(readonly, bool)
        expired = self.last_updated_time + self.ttl <= perf_counter()
        if readonly:
            has_server_for_mode = bool(self.readers)
        else:
            has_server_for_mode = bool(self.writers)
        res = not expired and self.routers and has_server_for_mode
        log.debug("[#0000]  _: <ROUTING> checking table freshness "
                  "(readonly=%r): table expired=%r, "
                  "has_server_for_mode=%r, table routers=%r => %r",
                  readonly, expired, has_server_for_mode, self.routers, res)
        return res

    def should_be_purged_from_memory(self):
        """ Check if the routing table is stale and not used for a long time and should be removed from memory.

        :returns: Returns true if it is old and not used for a while.
        :rtype: bool
        """
        from ._conf import RoutingConfig
        perf_time = perf_counter()
        res = self.last_updated_time + self.ttl + RoutingConfig.routing_table_purge_delay <= perf_time
        log.debug("[#0000]  _: <ROUTING> purge check: "
                  "last_updated_time=%r, ttl=%r, perf_time=%r => %r",
                  self.last_updated_time, self.ttl, perf_time, res)
        return res

    def update(self, new_routing_table):
        """ Update the current routing table with new routing information
        from a replacement table.
        """
        self.routers.replace(new_routing_table.routers)
        self.readers.replace(new_routing_table.readers)
        self.writers.replace(new_routing_table.writers)
        self.initialized_without_writers = not self.writers
        self.last_updated_time = perf_counter()
        self.ttl = new_routing_table.ttl
        log.debug("[#0000]  _: <ROUTING> updated table=%r", self)

    def servers(self):
        return set(self.routers) | set(self.writers) | set(self.readers)
