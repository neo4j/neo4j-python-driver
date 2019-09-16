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


from collections import OrderedDict
from collections.abc import MutableSet
from logging import getLogger
from time import perf_counter

from neo4j import READ_ACCESS, WRITE_ACCESS, DEFAULT_PORT
from neo4j.addressing import Address


log = getLogger("neobolt")


class OrderedSet(MutableSet):

    def __init__(self, elements=()):
        self._elements = OrderedDict.fromkeys(elements)
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
            raise ValueError(element)

    def update(self, elements=()):
        self._elements.update(OrderedDict.fromkeys(elements))

    def replace(self, elements=()):
        e = self._elements
        e.clear()
        e.update(OrderedDict.fromkeys(elements))


class RoutingTable:

    timer = perf_counter

    @classmethod
    def parse_routing_info(cls, records):
        """ Parse the records returned from a getServers call and
        return a new RoutingTable instance.
        """
        if len(records) != 1:
            raise ValueError("Expected exactly one record")
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
                    addresses.append(Address.parse(address, default_port=DEFAULT_PORT))
                if role == "ROUTE":
                    routers.extend(addresses)
                elif role == "READ":
                    readers.extend(addresses)
                elif role == "WRITE":
                    writers.extend(addresses)
            ttl = record["ttl"]
        except (KeyError, TypeError):
            raise ValueError("Cannot parse routing info")
        else:
            return cls(routers, readers, writers, ttl)

    def __init__(self, routers=(), readers=(), writers=(), ttl=0):
        self.routers = OrderedSet(routers)
        self.readers = OrderedSet(readers)
        self.writers = OrderedSet(writers)
        self.last_updated_time = self.timer()
        self.ttl = ttl

    def __repr__(self):
        return "RoutingTable(routers=%r, readers=%r, writers=%r, last_updated_time=%r, ttl=%r)" % (
            self.routers,
            self.readers,
            self.writers,
            self.last_updated_time,
            self.ttl,
        )

    def is_fresh(self, access_mode):
        """ Indicator for whether routing information is still usable.
        """
        log.debug("[#0000]  C: <ROUTING> Checking table freshness for %r", access_mode)
        expired = self.last_updated_time + self.ttl <= self.timer()
        has_server_for_mode = bool(access_mode == READ_ACCESS and self.readers) or bool(access_mode == WRITE_ACCESS and self.writers)
        log.debug("[#0000]  C: <ROUTING> Table expired=%r", expired)
        log.debug("[#0000]  C: <ROUTING> Table routers=%r", self.routers)
        log.debug("[#0000]  C: <ROUTING> Table has_server_for_mode=%r", has_server_for_mode)
        return not expired and self.routers and has_server_for_mode

    def update(self, new_routing_table):
        """ Update the current routing table with new routing information
        from a replacement table.
        """
        self.routers.replace(new_routing_table.routers)
        self.readers.replace(new_routing_table.readers)
        self.writers.replace(new_routing_table.writers)
        self.last_updated_time = self.timer()
        self.ttl = new_routing_table.ttl
        log.debug("[#0000]  S: <ROUTING> table=%r", self)

    def servers(self):
        return set(self.routers) | set(self.writers) | set(self.readers)
