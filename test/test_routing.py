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

from threading import Semaphore, Thread
from time import sleep
from unittest import TestCase

from mock import patch

from neo4j.bolt.connection import connect, ServiceUnavailable, ProtocolError
from neo4j.v1 import basic_auth
from neo4j.v1.routing import RoundRobinSet, RoutingTable, RoutingConnectionPool

from test.util import ServerTestCase, StubCluster


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


class RoutingTableParseAddressTestCase(ServerTestCase):

    def test_should_parse_ip_address_and_port(self):
        parsed = RoutingTable.parse_address("127.0.0.1:7687")
        assert parsed == ("127.0.0.1", 7687)

    def test_should_parse_host_name_and_port(self):
        parsed = RoutingTable.parse_address("localhost:7687")
        assert parsed == ("localhost", 7687)

    def test_should_fail_on_missing_port(self):
        with self.assertRaises(ValueError):
            _ = RoutingTable.parse_address("127.0.0.1")

    def test_should_fail_on_empty_port(self):
        with self.assertRaises(ValueError):
            _ = RoutingTable.parse_address("127.0.0.1:")

    def test_should_fail_on_non_numeric_port(self):
        with self.assertRaises(ValueError):
            _ = RoutingTable.parse_address("127.0.0.1:X")


class RoutingTableParseRoutingInfoTestCase(ServerTestCase):

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


class RoutingConnectionPoolConstructionTestCase(ServerTestCase):

    def test_should_populate_initial_router(self):
        with RoutingConnectionPool(connector, ("127.0.0.1", 9001)) as pool:
            assert pool.routing_table.routers == {("127.0.0.1", 9001)}


class RoutingConnectionPoolFetchRoutingInfoTestCase(ServerTestCase):

    def test_should_get_info_from_router(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector) as pool:
                result = pool.fetch_routing_info(address)
                assert len(result) == 1
                record = result[0]
                assert record["ttl"] == 300
                assert record["servers"] == [
                    {"role": "ROUTE", "addresses": ["127.0.0.1:9001", "127.0.0.1:9002",
                                                    "127.0.0.1:9003"]},
                    {"role": "READ", "addresses": ["127.0.0.1:9004", "127.0.0.1:9005"]},
                    {"role": "WRITE", "addresses": ["127.0.0.1:9006"]},
                ]

    def test_should_remove_router_if_cannot_connect(self):
        address = ("127.0.0.1", 9001)
        with RoutingConnectionPool(connector, address) as pool:
            assert address in pool.routing_table.routers
            _ = pool.fetch_routing_info(address)
            assert address not in pool.routing_table.routers

    def test_should_remove_router_if_connection_drops(self):
        with StubCluster({9001: "rude_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                assert address in pool.routing_table.routers
                _ = pool.fetch_routing_info(address)
                assert address not in pool.routing_table.routers

    def test_should_not_fail_if_cannot_connect_but_router_already_removed(self):
        address = ("127.0.0.1", 9001)
        with RoutingConnectionPool(connector) as pool:
            assert address not in pool.routing_table.routers
            _ = pool.fetch_routing_info(address)
            assert address not in pool.routing_table.routers

    def test_should_not_fail_if_connection_drops_but_router_already_removed(self):
        with StubCluster({9001: "rude_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector) as pool:
                assert address not in pool.routing_table.routers
                _ = pool.fetch_routing_info(address)
                assert address not in pool.routing_table.routers

    def test_should_return_none_if_cannot_connect(self):
        address = ("127.0.0.1", 9001)
        with RoutingConnectionPool(connector, address) as pool:
            result = pool.fetch_routing_info(address)
            assert result is None

    def test_should_return_none_if_connection_drops(self):
        with StubCluster({9001: "rude_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                result = pool.fetch_routing_info(address)
                assert result is None

    def test_should_fail_for_non_router(self):
        with StubCluster({9001: "non_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                with self.assertRaises(ServiceUnavailable):
                    _ = pool.fetch_routing_info(address)

    def test_should_fail_if_database_error(self):
        with StubCluster({9001: "broken_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                with self.assertRaises(ServiceUnavailable):
                    _ = pool.fetch_routing_info(address)


class RoutingConnectionPoolFetchRoutingTableTestCase(ServerTestCase):

    def test_should_get_table_from_router(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector) as pool:
                table = pool.fetch_routing_table(address)
                assert table.routers == {("127.0.0.1", 9001), ("127.0.0.1", 9002),
                                         ("127.0.0.1", 9003)}
                assert table.readers == {("127.0.0.1", 9004), ("127.0.0.1", 9005)}
                assert table.writers == {("127.0.0.1", 9006)}
                assert table.ttl == 300

    def test_null_info_should_return_null_table(self):
        address = ("127.0.0.1", 9001)
        with RoutingConnectionPool(connector) as pool:
            table = pool.fetch_routing_table(address)
            assert table is None

    def test_no_routers_should_raise_protocol_error(self):
        with StubCluster({9001: "router_no_routers.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector) as pool:
                with self.assertRaises(ProtocolError):
                    _ = pool.fetch_routing_table(address)

    def test_no_readers_should_raise_protocol_error(self):
        with StubCluster({9001: "router_no_readers.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector) as pool:
                with self.assertRaises(ProtocolError):
                    _ = pool.fetch_routing_table(address)

    def test_no_writers_and_one_router_should_raise_signal_service_unavailable(self):
        with StubCluster({9001: "router_no_writers_one_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector) as pool:
                with self.assertRaises(ServiceUnavailable):
                    _ = pool.fetch_routing_table(address)

    def test_no_writers_and_multiple_routers_should_return_null_table(self):
        with StubCluster({9001: "router_no_writers.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector) as pool:
                table = pool.fetch_routing_table(address)
                assert table is None


class RoutingConnectionPoolUpdateRoutingTableTestCase(ServerTestCase):

    scenarios = {
        (None,): ServiceUnavailable,
        (RoutingTable,): RoutingTable,
        (ServiceUnavailable,): ServiceUnavailable,
        (None, None): ServiceUnavailable,
        (None, RoutingTable): RoutingTable,
        (None, ServiceUnavailable): ServiceUnavailable,
        (None, None, None): ServiceUnavailable,
        (None, None, RoutingTable): RoutingTable,
        (None, None, ServiceUnavailable): ServiceUnavailable,
    }

    def test_update_with_no_routers_should_signal_service_unavailable(self):
        with RoutingConnectionPool(connector) as pool:
            with self.assertRaises(ServiceUnavailable):
                pool.update_routing_table()

    def test_update_scenarios(self):
        for server_outcomes, overall_outcome in self.scenarios.items():
            self._test_server_outcome(server_outcomes, overall_outcome)

    def _test_server_outcome(self, server_outcomes, overall_outcome):
        print("%r -> %r" % (server_outcomes, overall_outcome))
        servers = {}
        routers = []
        for port, outcome in enumerate(server_outcomes, 9001):
            if outcome is None:
                servers[port] = "router_no_writers.script"
            elif outcome is RoutingTable:
                servers[port] = "router.script"
            elif outcome is ServiceUnavailable:
                servers[port] = "non_router.script"
            else:
                assert False, "Unexpected server outcome %r" % outcome
            routers.append(("127.0.0.1", port))
        with StubCluster(servers):
            with RoutingConnectionPool(connector, *routers) as pool:
                if overall_outcome is RoutingTable:
                    pool.update_routing_table()
                    table = pool.routing_table
                    assert table.routers == {("127.0.0.1", 9001), ("127.0.0.1", 9002),
                                             ("127.0.0.1", 9003)}
                    assert table.readers == {("127.0.0.1", 9004), ("127.0.0.1", 9005)}
                    assert table.writers == {("127.0.0.1", 9006)}
                    assert table.ttl == 300
                elif overall_outcome is ServiceUnavailable:
                    with self.assertRaises(ServiceUnavailable):
                        pool.update_routing_table()
                else:
                    assert False, "Unexpected overall outcome %r" % overall_outcome


class RoutingConnectionPoolRefreshRoutingTableTestCase(ServerTestCase):

    def test_should_update_if_stale(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                first_updated_time = pool.routing_table.last_updated_time
                pool.routing_table.ttl = 0
                pool.refresh_routing_table()
                second_updated_time = pool.routing_table.last_updated_time
                assert second_updated_time != first_updated_time

    def test_should_not_update_if_fresh(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                pool.refresh_routing_table()
                first_updated_time = pool.routing_table.last_updated_time
                pool.refresh_routing_table()
                second_updated_time = pool.routing_table.last_updated_time
                assert second_updated_time == first_updated_time

    def test_concurrent_refreshes_should_not_block_if_fresh(self):
        address = ("127.0.0.1", 9001)
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])

        with RoutingConnectionPool(connector, address) as pool:
            semaphore = Semaphore()

            class Refresher(Thread):

                refreshed = None

                def run(self):
                    self.refreshed = pool.refresh_routing_table()

            class BlockingRefresher(Refresher):

                @classmethod
                def blocking_update(cls):
                    pool.routing_table.update(table)
                    semaphore.acquire()
                    semaphore.release()
                    return table

                def run(self):
                    with patch.object(RoutingConnectionPool, "update_routing_table",
                                      side_effect=self.blocking_update):
                        super(BlockingRefresher, self).run()

            first = BlockingRefresher()
            second = Refresher()

            assert not pool.routing_table.is_fresh()

            semaphore.acquire()
            first.start()
            second.start()
            sleep(1)
            assert not second.is_alive()  # second call should return immediately without blocking
            second.join()
            semaphore.release()
            first.join()

            assert first.refreshed
            assert not second.refreshed
            assert pool.routing_table.is_fresh()

    def test_concurrent_refreshes_should_block_if_stale(self):
        address = ("127.0.0.1", 9001)
        table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])

        with RoutingConnectionPool(connector, address) as pool:
            semaphore = Semaphore()

            class Refresher(Thread):

                refreshed = None

                def run(self):
                    self.refreshed = pool.refresh_routing_table()

            class BlockingRefresher(Refresher):

                @classmethod
                def blocking_update(cls):
                    semaphore.acquire()
                    semaphore.release()
                    pool.routing_table.update(table)
                    return table

                def run(self):
                    with patch.object(RoutingConnectionPool, "update_routing_table",
                                      side_effect=self.blocking_update):
                        super(BlockingRefresher, self).run()

            first = BlockingRefresher()
            second = Refresher()

            assert not pool.routing_table.is_fresh()

            semaphore.acquire()
            first.start()
            second.start()
            sleep(1)
            assert second.is_alive()  # second call should block
            semaphore.release()
            second.join()
            first.join()

            assert first.refreshed
            assert not second.refreshed
            assert pool.routing_table.is_fresh()


class RoutingConnectionPoolAcquireForReadTestCase(ServerTestCase):

    def test_should_refresh(self):
        with StubCluster({9001: "router.script", 9004: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire_for_read()
                assert pool.routing_table.is_fresh()

    def test_connected_to_reader(self):
        with StubCluster({9001: "router.script", 9004: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                assert not pool.routing_table.is_fresh()
                connection = pool.acquire_for_read()
                assert connection.server.address in pool.routing_table.readers

    def test_should_retry_if_first_reader_fails(self):
        with StubCluster({9001: "router.script",
                          9004: "fail_on_init.script",
                          9005: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire_for_read()
                assert ("127.0.0.1", 9004) not in pool.routing_table.readers
                assert ("127.0.0.1", 9005) in pool.routing_table.readers


class RoutingConnectionPoolAcquireForWriteTestCase(ServerTestCase):

    def test_should_refresh(self):
        with StubCluster({9001: "router.script", 9006: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire_for_write()
                assert pool.routing_table.is_fresh()

    def test_connected_to_writer(self):
        with StubCluster({9001: "router.script", 9006: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                assert not pool.routing_table.is_fresh()
                connection = pool.acquire_for_write()
                assert connection.server.address in pool.routing_table.writers

    def test_should_retry_if_first_writer_fails(self):
        with StubCluster({9001: "router_with_multiple_writers.script",
                          9006: "fail_on_init.script",
                          9007: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire_for_write()
                assert ("127.0.0.1", 9006) not in pool.routing_table.writers
                assert ("127.0.0.1", 9007) in pool.routing_table.writers


class RoutingConnectionPoolRemoveTestCase(ServerTestCase):

    def test_should_remove_router_from_routing_table_if_present(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9001)
                assert target in pool.routing_table.routers
                pool.remove(target)
                assert target not in pool.routing_table.routers

    def test_should_remove_reader_from_routing_table_if_present(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9004)
                assert target in pool.routing_table.readers
                pool.remove(target)
                assert target not in pool.routing_table.readers

    def test_should_remove_writer_from_routing_table_if_present(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9006)
                assert target in pool.routing_table.writers
                pool.remove(target)
                assert target not in pool.routing_table.writers

    def test_should_not_fail_if_absent(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9007)
                pool.remove(target)
