#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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
from unittest import TestCase

from neo4j.bolt import ProtocolError
from neo4j.bolt.connection import connect
from neo4j.v1.routing import OrderedSet, RoutingTable, RoutingConnectionPool, LeastConnectedLoadBalancingStrategy, \
    RoundRobinLoadBalancingStrategy
from neo4j.v1.security import basic_auth
from neo4j.v1.api import READ_ACCESS, WRITE_ACCESS


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


def connector(address, error_handler):
    return connect(address, error_handler=error_handler, auth=basic_auth("neotest", "neotest"))


class OrderedSetTestCase(TestCase):
    def test_should_repr_as_set(self):
        s = OrderedSet([1, 2, 3])
        assert repr(s) == "{1, 2, 3}"

    def test_should_contain_element(self):
        s = OrderedSet([1, 2, 3])
        assert 2 in s

    def test_should_not_contain_non_element(self):
        s = OrderedSet([1, 2, 3])
        assert 4 not in s

    def test_should_be_able_to_get_item_if_empty(self):
        s = OrderedSet([])
        with self.assertRaises(IndexError):
            _ = s[0]

    def test_should_be_able_to_get_items_by_index(self):
        s = OrderedSet([1, 2, 3])
        self.assertEqual(s[0], 1)
        self.assertEqual(s[1], 2)
        self.assertEqual(s[2], 3)

    def test_should_be_iterable(self):
        s = OrderedSet([1, 2, 3])
        assert list(iter(s)) == [1, 2, 3]

    def test_should_have_length(self):
        s = OrderedSet([1, 2, 3])
        assert len(s) == 3

    def test_should_be_able_to_add_new(self):
        s = OrderedSet([1, 2, 3])
        s.add(4)
        assert list(s) == [1, 2, 3, 4]

    def test_should_be_able_to_add_existing(self):
        s = OrderedSet([1, 2, 3])
        s.add(2)
        assert list(s) == [1, 2, 3]

    def test_should_be_able_to_clear(self):
        s = OrderedSet([1, 2, 3])
        s.clear()
        assert list(s) == []

    def test_should_be_able_to_discard_existing(self):
        s = OrderedSet([1, 2, 3])
        s.discard(2)
        assert list(s) == [1, 3]

    def test_should_be_able_to_discard_non_existing(self):
        s = OrderedSet([1, 2, 3])
        s.discard(4)
        assert list(s) == [1, 2, 3]

    def test_should_be_able_to_remove_existing(self):
        s = OrderedSet([1, 2, 3])
        s.remove(2)
        assert list(s) == [1, 3]

    def test_should_not_be_able_to_remove_non_existing(self):
        s = OrderedSet([1, 2, 3])
        with self.assertRaises(ValueError):
            s.remove(4)

    def test_should_be_able_to_update(self):
        s = OrderedSet([1, 2, 3])
        s.update([3, 4, 5])
        assert list(s) == [1, 2, 3, 4, 5]

    def test_should_be_able_to_replace(self):
        s = OrderedSet([1, 2, 3])
        s.replace([3, 4, 5])
        assert list(s) == [3, 4, 5]


class RoutingTableConstructionTestCase(TestCase):
    def test_should_be_initially_stale(self):
        table = RoutingTable()
        assert not table.is_fresh(READ_ACCESS)
        assert not table.is_fresh(WRITE_ACCESS)


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


class RoutingTableServersTestCase(TestCase):
    def test_should_return_all_distinct_servers_in_routing_table(self):
        routing_table = {
            "ttl": 300,
            "servers": [
                {"role": "ROUTE", "addresses": ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"]},
                {"role": "READ", "addresses": ["127.0.0.1:9001", "127.0.0.1:9005"]},
                {"role": "WRITE", "addresses": ["127.0.0.1:9002"]},
            ],
        }
        table = RoutingTable.parse_routing_info([routing_table])
        assert table.servers() == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003), ('127.0.0.1', 9005)}


class RoutingTableFreshnessTestCase(TestCase):
    def test_should_be_fresh_after_update(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        assert table.is_fresh(READ_ACCESS)
        assert table.is_fresh(WRITE_ACCESS)

    def test_should_become_stale_on_expiry(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        table.ttl = 0
        assert not table.is_fresh(READ_ACCESS)
        assert not table.is_fresh(WRITE_ACCESS)

    def test_should_become_stale_if_no_readers(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        table.readers.clear()
        assert not table.is_fresh(READ_ACCESS)
        assert table.is_fresh(WRITE_ACCESS)

    def test_should_become_stale_if_no_writers(self):
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
        table.writers.clear()
        assert table.is_fresh(READ_ACCESS)
        assert not table.is_fresh(WRITE_ACCESS)


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
        with RoutingConnectionPool(connector, initial_router, {}, router) as pool:
            assert pool.routing_table.routers == {("127.0.0.1", 9002)}


class FakeConnectionPool(object):

    def __init__(self, addresses):
        self._addresses = addresses

    def in_use_connection_count(self, address):
        return self._addresses.get(address, 0)


class RoundRobinLoadBalancingStrategyTestCase(TestCase):

    def test_simple_reader_selection(self):
        strategy = RoundRobinLoadBalancingStrategy()
        self.assertEqual(strategy.select_reader(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "0.0.0.0")
        self.assertEqual(strategy.select_reader(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "1.1.1.1")
        self.assertEqual(strategy.select_reader(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "2.2.2.2")
        self.assertEqual(strategy.select_reader(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "0.0.0.0")

    def test_empty_reader_selection(self):
        strategy = RoundRobinLoadBalancingStrategy()
        self.assertIsNone(strategy.select_reader([]))

    def test_simple_writer_selection(self):
        strategy = RoundRobinLoadBalancingStrategy()
        self.assertEqual(strategy.select_writer(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "0.0.0.0")
        self.assertEqual(strategy.select_writer(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "1.1.1.1")
        self.assertEqual(strategy.select_writer(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "2.2.2.2")
        self.assertEqual(strategy.select_writer(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "0.0.0.0")

    def test_empty_writer_selection(self):
        strategy = RoundRobinLoadBalancingStrategy()
        self.assertIsNone(strategy.select_writer([]))


class LeastConnectedLoadBalancingStrategyTestCase(TestCase):

    def test_simple_reader_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("0.0.0.0", 2),
            ("1.1.1.1", 1),
            ("2.2.2.2", 0),
        ])))
        self.assertEqual(strategy.select_reader(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "2.2.2.2")

    def test_reader_selection_with_clash(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("0.0.0.0", 0),
            ("0.0.0.1", 0),
            ("1.1.1.1", 1),
        ])))
        self.assertEqual(strategy.select_reader(["0.0.0.0", "0.0.0.1", "1.1.1.1"]), "0.0.0.0")
        self.assertEqual(strategy.select_reader(["0.0.0.0", "0.0.0.1", "1.1.1.1"]), "0.0.0.1")

    def test_empty_reader_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
        ])))
        self.assertIsNone(strategy.select_reader([]))

    def test_not_in_pool_reader_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("1.1.1.1", 1),
            ("2.2.2.2", 2),
        ])))
        self.assertEqual(strategy.select_reader(["2.2.2.2", "3.3.3.3"]), "3.3.3.3")

    def test_partially_in_pool_reader_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("1.1.1.1", 1),
            ("2.2.2.2", 0),
        ])))
        self.assertEqual(strategy.select_reader(["2.2.2.2", "3.3.3.3"]), "2.2.2.2")
        self.assertEqual(strategy.select_reader(["2.2.2.2", "3.3.3.3"]), "3.3.3.3")

    def test_simple_writer_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("0.0.0.0", 2),
            ("1.1.1.1", 1),
            ("2.2.2.2", 0),
        ])))
        self.assertEqual(strategy.select_writer(["0.0.0.0", "1.1.1.1", "2.2.2.2"]), "2.2.2.2")

    def test_writer_selection_with_clash(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("0.0.0.0", 0),
            ("0.0.0.1", 0),
            ("1.1.1.1", 1),
        ])))
        self.assertEqual(strategy.select_writer(["0.0.0.0", "0.0.0.1", "1.1.1.1"]), "0.0.0.0")
        self.assertEqual(strategy.select_writer(["0.0.0.0", "0.0.0.1", "1.1.1.1"]), "0.0.0.1")

    def test_empty_writer_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
        ])))
        self.assertIsNone(strategy.select_writer([]))

    def test_not_in_pool_writer_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("1.1.1.1", 1),
            ("2.2.2.2", 2),
        ])))
        self.assertEqual(strategy.select_writer(["2.2.2.2", "3.3.3.3"]), "3.3.3.3")

    def test_partially_in_pool_writer_selection(self):
        strategy = LeastConnectedLoadBalancingStrategy(FakeConnectionPool(OrderedDict([
            ("1.1.1.1", 1),
            ("2.2.2.2", 0),
        ])))
        self.assertEqual(strategy.select_writer(["2.2.2.2", "3.3.3.3"]), "2.2.2.2")
        self.assertEqual(strategy.select_writer(["2.2.2.2", "3.3.3.3"]), "3.3.3.3")
