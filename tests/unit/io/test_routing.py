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


from unittest import TestCase

from neo4j.io import Bolt, Neo4jPool
from neo4j.routing import OrderedSet, RoutingTable


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


def opener(address, error_handler):
    return Bolt.open(address, error_handler=error_handler, auth=("neotest", "neotest"))


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
        assert not table.is_fresh(readonly=True)
        assert not table.is_fresh(readonly=False)


class RoutingTableParseRoutingInfoTestCase(TestCase):
    def test_should_return_routing_table_on_valid_record(self):
        table = RoutingTable.parse_routing_info(VALID_ROUTING_RECORD["servers"],
                                                VALID_ROUTING_RECORD["ttl"])
        assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
        assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
        assert table.writers == {('127.0.0.1', 9006)}
        assert table.ttl == 300

    def test_should_return_routing_table_on_valid_record_with_extra_role(self):
        table = RoutingTable.parse_routing_info(VALID_ROUTING_RECORD_WITH_EXTRA_ROLE["servers"],
                                                VALID_ROUTING_RECORD_WITH_EXTRA_ROLE["ttl"])
        assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
        assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
        assert table.writers == {('127.0.0.1', 9006)}
        assert table.ttl == 300


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
        table = RoutingTable.parse_routing_info(routing_table["servers"], routing_table["ttl"])
        assert table.servers() == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003), ('127.0.0.1', 9005)}


class RoutingTableFreshnessTestCase(TestCase):
    def test_should_be_fresh_after_update(self):
        table = RoutingTable.parse_routing_info(VALID_ROUTING_RECORD["servers"],
                                                VALID_ROUTING_RECORD["ttl"])
        assert table.is_fresh(readonly=True)
        assert table.is_fresh(readonly=False)

    def test_should_become_stale_on_expiry(self):
        table = RoutingTable.parse_routing_info(VALID_ROUTING_RECORD["servers"],
                                                VALID_ROUTING_RECORD["ttl"])
        table.ttl = 0
        assert not table.is_fresh(readonly=True)
        assert not table.is_fresh(readonly=False)

    def test_should_become_stale_if_no_readers(self):
        table = RoutingTable.parse_routing_info(VALID_ROUTING_RECORD["servers"],
                                                VALID_ROUTING_RECORD["ttl"])
        table.readers.clear()
        assert not table.is_fresh(readonly=True)
        assert table.is_fresh(readonly=False)

    def test_should_become_stale_if_no_writers(self):
        table = RoutingTable.parse_routing_info(VALID_ROUTING_RECORD["servers"],
                                                VALID_ROUTING_RECORD["ttl"])
        table.writers.clear()
        assert table.is_fresh(readonly=True)
        assert not table.is_fresh(readonly=False)


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
