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

from unittest import TestCase

from neo4j.bolt import ProtocolError
from neo4j.bolt.connection import connect
from neo4j.v1.routing import RoundRobinSet, RoutingTable, RoutingConnectionPool
from neo4j.v1.security import basic_auth


VALID_ROUTING_RECORD = {
    "ttl": 300,
    "servers": [
        {"role": "ROUTE", "addresses": ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"]},
        {"role": "READ", "addresses": ["127.0.0.1:9004", "127.0.0.1:9005"]},
        {"role": "WRITE", "addresses": ["127.0.0.1:9006"]},
    ],
}

VALID_ROUTING_RECORD_WITH_EXTRA_ROLE = {
    "ttl": 300,
    "servers": [
        {"role": "ROUTE", "addresses": ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"]},
        {"role": "READ", "addresses": ["127.0.0.1:9004", "127.0.0.1:9005"]},
        {"role": "WRITE", "addresses": ["127.0.0.1:9006"]},
        {"role": "MAGIC", "addresses": ["127.0.0.1:9007"]},
    ],
}

INVALID_ROUTING_RECORD = {
    "X": 1,
}


def connector(address):
    return connect(address, auth=basic_auth("neotest", "neotest"))


class RoundRobinSetTestCase(TestCase):

    def test_should_repr_as_set(self):
        rrs = RoundRobinSet([1, 2, 3])
        assert repr(rrs) == "{1, 2, 3}"

    def test_should_contain_element(self):
        rrs = RoundRobinSet([1, 2, 3])
        assert 2 in rrs

    def test_should_not_contain_non_element(self):
        rrs = RoundRobinSet([1, 2, 3])
        assert 4 not in rrs

    def test_should_be_able_to_get_next_if_empty(self):
        rrs = RoundRobinSet([])
        assert next(rrs) is None

    def test_should_be_able_to_get_next_repeatedly(self):
        rrs = RoundRobinSet([1, 2, 3])
        assert next(rrs) == 1
        assert next(rrs) == 2
        assert next(rrs) == 3
        assert next(rrs) == 1

    def test_should_be_able_to_get_next_repeatedly_via_old_method(self):
        rrs = RoundRobinSet([1, 2, 3])
        assert rrs.next() == 1
        assert rrs.next() == 2
        assert rrs.next() == 3
        assert rrs.next() == 1

    def test_should_be_iterable(self):
        rrs = RoundRobinSet([1, 2, 3])
        assert list(iter(rrs)) == [1, 2, 3]

    def test_should_have_length(self):
        rrs = RoundRobinSet([1, 2, 3])
        assert len(rrs) == 3

    def test_should_be_able_to_add_new(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.add(4)
        assert list(rrs) == [1, 2, 3, 4]

    def test_should_be_able_to_add_existing(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.add(2)
        assert list(rrs) == [1, 2, 3]

    def test_should_be_able_to_clear(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.clear()
        assert list(rrs) == []

    def test_should_be_able_to_discard_existing(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.discard(2)
        assert list(rrs) == [1, 3]

    def test_should_be_able_to_discard_non_existing(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.discard(4)
        assert list(rrs) == [1, 2, 3]

    def test_should_be_able_to_remove_existing(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.remove(2)
        assert list(rrs) == [1, 3]

    def test_should_not_be_able_to_remove_non_existing(self):
        rrs = RoundRobinSet([1, 2, 3])
        with self.assertRaises(ValueError):
            rrs.remove(4)

    def test_should_be_able_to_update(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.update([3, 4, 5])
        assert list(rrs) == [1, 2, 3, 4, 5]

    def test_should_be_able_to_replace(self):
        rrs = RoundRobinSet([1, 2, 3])
        rrs.replace([3, 4, 5])
        assert list(rrs) == [3, 4, 5]


class RoutingTableConstructionTestCase(TestCase):

    def test_should_be_initially_stale(self):
        table = RoutingTable()
        assert not table.is_fresh()


class RoutingTableParseRoutingInfoTestCase(TestCase):

    def test_should_return_routing_table_on_valid_record(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
        assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
        assert table.writers == {('127.0.0.1', 9006)}
        assert table.ttl == 300

    def test_should_return_routing_table_on_valid_record_with_extra_role(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD_WITH_EXTRA_ROLE])
        assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
        assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
        assert table.writers == {('127.0.0.1', 9006)}
        assert table.ttl == 300

    def test_should_fail_on_invalid_record(self):
        with self.assertRaises(ProtocolError):
            _ = RoutingTable.parse_routing_info([INVALID_ROUTING_RECORD])

    def test_should_fail_on_zero_records(self):
        with self.assertRaises(ProtocolError):
            _ = RoutingTable.parse_routing_info([])

    def test_should_fail_on_multiple_records(self):
        with self.assertRaises(ProtocolError):
            _ = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD, VALID_ROUTING_RECORD])


class RoutingTableFreshnessTestCase(TestCase):

    def test_should_be_fresh_after_update(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        assert table.is_fresh()

    def test_should_become_stale_on_expiry(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        table.ttl = 0
        assert not table.is_fresh()

    def test_should_become_stale_if_no_readers(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        table.readers.clear()
        assert not table.is_fresh()

    def test_should_become_stale_if_no_writers(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        table.writers.clear()
        assert not table.is_fresh()


class RoutingTableUpdateTestCase(TestCase):

    def setUp(self):
        self.table = RoutingTable(
            [("192.168.1.1", 7687), ("192.168.1.2", 7687)], [("192.168.1.3", 7687)], [], 0)
        self.new_table = RoutingTable(
            [("127.0.0.1", 9001), ("127.0.0.1", 9002), ("127.0.0.1", 9003)],
            [("127.0.0.1", 9004), ("127.0.0.1", 9005)], [("127.0.0.1", 9006)], 300)

    def test_update_should_replace_routers(self):
        self.table.update(self.new_table)
        assert self.table.routers == {("127.0.0.1", 9001), ("127.0.0.1", 9002), ("127.0.0.1", 9003)}

    def test_update_should_replace_readers(self):
        self.table.update(self.new_table)
        assert self.table.readers == {("127.0.0.1", 9004), ("127.0.0.1", 9005)}

    def test_update_should_replace_writers(self):
        self.table.update(self.new_table)
        assert self.table.writers == {("127.0.0.1", 9006)}

    def test_update_should_replace_ttl(self):
        self.table.update(self.new_table)
        assert self.table.ttl == 300


class RoutingConnectionPoolConstructionTestCase(TestCase):

    def test_should_populate_initial_router(self):
        initial_router = ("127.0.0.1", 9001)
        router = ("127.0.0.1", 9002)
        with RoutingConnectionPool(connector, initial_router, router) as pool:
            assert pool.routing_table.routers == {("127.0.0.1", 9002)}
