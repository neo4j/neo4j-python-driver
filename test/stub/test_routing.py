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


from neo4j.bolt import connect, ProtocolError, ServiceUnavailable
from neo4j.v1 import basic_auth, READ_ACCESS, WRITE_ACCESS, RoutingTable, RoutingConnectionPool

from test.stub.tools import StubCluster, StubTestCase

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

UNREACHABLE_ADDRESS = ("127.0.0.1", 8080)


def connector(address):
    return connect(address, auth=basic_auth("neotest", "neotest"))


def RoutingPool(*routers):
    return RoutingConnectionPool(connector, UNREACHABLE_ADDRESS, {}, *routers)


class RoutingConnectionPoolFetchRoutingInfoTestCase(StubTestCase):
    def test_should_get_info_from_router(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool() as pool:
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
        with RoutingPool(address) as pool:
            assert address in pool.routing_table.routers
            _ = pool.fetch_routing_info(address)
            assert address not in pool.routing_table.routers

    def test_should_remove_router_if_connection_drops(self):
        with StubCluster({9001: "rude_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                assert address in pool.routing_table.routers
                _ = pool.fetch_routing_info(address)
                assert address not in pool.routing_table.routers

    def test_should_not_fail_if_cannot_connect_but_router_already_removed(self):
        address = ("127.0.0.1", 9001)
        with RoutingPool() as pool:
            assert address not in pool.routing_table.routers
            _ = pool.fetch_routing_info(address)
            assert address not in pool.routing_table.routers

    def test_should_not_fail_if_connection_drops_but_router_already_removed(self):
        with StubCluster({9001: "rude_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool() as pool:
                assert address not in pool.routing_table.routers
                _ = pool.fetch_routing_info(address)
                assert address not in pool.routing_table.routers

    def test_should_return_none_if_cannot_connect(self):
        address = ("127.0.0.1", 9001)
        with RoutingPool(address) as pool:
            result = pool.fetch_routing_info(address)
            assert result is None

    def test_should_return_none_if_connection_drops(self):
        with StubCluster({9001: "rude_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                result = pool.fetch_routing_info(address)
                assert result is None

    def test_should_fail_for_non_router(self):
        with StubCluster({9001: "non_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                with self.assertRaises(ServiceUnavailable):
                    _ = pool.fetch_routing_info(address)

    def test_should_fail_if_database_error(self):
        with StubCluster({9001: "broken_router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                with self.assertRaises(ServiceUnavailable):
                    _ = pool.fetch_routing_info(address)

    def test_should_call_get_routing_tables_with_context(self):
        with StubCluster({9001: "get_routing_table_with_context.script"}):
            address = ("127.0.0.1", 9001)
            routing_context = {"name": "molly", "age": "1"}
            with RoutingConnectionPool(connector, UNREACHABLE_ADDRESS, routing_context) as pool:
                pool.fetch_routing_info(address)

    def test_should_call_get_routing_tables(self):
        with StubCluster({9001: "get_routing_table.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingConnectionPool(connector, UNREACHABLE_ADDRESS, {}) as pool:
                pool.fetch_routing_info(address)


class RoutingConnectionPoolFetchRoutingTableTestCase(StubTestCase):
    def test_should_get_table_from_router(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool() as pool:
                table = pool.fetch_routing_table(address)
                assert table.routers == {("127.0.0.1", 9001), ("127.0.0.1", 9002),
                                         ("127.0.0.1", 9003)}
                assert table.readers == {("127.0.0.1", 9004), ("127.0.0.1", 9005)}
                assert table.writers == {("127.0.0.1", 9006)}
                assert table.ttl == 300

    def test_null_info_should_return_null_table(self):
        address = ("127.0.0.1", 9001)
        with RoutingPool() as pool:
            table = pool.fetch_routing_table(address)
            assert table is None

    def test_no_routers_should_raise_protocol_error(self):
        with StubCluster({9001: "router_no_routers.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool() as pool:
                with self.assertRaises(ProtocolError):
                    _ = pool.fetch_routing_table(address)

    def test_no_readers_should_raise_protocol_error(self):
        with StubCluster({9001: "router_no_readers.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool() as pool:
                with self.assertRaises(ProtocolError):
                    _ = pool.fetch_routing_table(address)

    def test_no_writers_should_return_null_table(self):
        with StubCluster({9001: "router_no_writers.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool() as pool:
                table = pool.fetch_routing_table(address)
                assert table is None


class RoutingConnectionPoolUpdateRoutingTableTestCase(StubTestCase):
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

    def test_roll_back_to_initial_server_if_failed_update_with_existing_routers(self):
        with StubCluster({9001: "router.script"}):
            initial_address = ("127.0.0.1", 9001)  # roll back addresses
            routers = [("127.0.0.1", 9002), ("127.0.0.1", 9003)]  # not reachable servers
            with RoutingConnectionPool(connector, initial_address, {}, *routers) as pool:
                pool.update_routing_table()
                table = pool.routing_table
                assert table.routers == {("127.0.0.1", 9001), ("127.0.0.1", 9002),
                                         ("127.0.0.1", 9003)}
                assert table.readers == {("127.0.0.1", 9004), ("127.0.0.1", 9005)}
                assert table.writers == {("127.0.0.1", 9006)}
                assert table.ttl == 300

    def test_update_with_no_routers_should_signal_service_unavailable(self):
        with RoutingPool() as pool:
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
            with RoutingPool(*routers) as pool:
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


class RoutingConnectionPoolRefreshRoutingTableTestCase(StubTestCase):
    def test_should_update_if_stale(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                first_updated_time = pool.routing_table.last_updated_time
                pool.routing_table.ttl = 0
                pool.refresh_routing_table()
                second_updated_time = pool.routing_table.last_updated_time
                assert second_updated_time != first_updated_time

    def test_should_not_update_if_fresh(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                pool.refresh_routing_table()
                first_updated_time = pool.routing_table.last_updated_time
                pool.refresh_routing_table()
                second_updated_time = pool.routing_table.last_updated_time
                assert second_updated_time == first_updated_time

    # TODO: fix flaky test
    # def test_concurrent_refreshes_should_not_block_if_fresh(self):
    #     address = ("127.0.0.1", 9001)
    #     table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
    #
    #     with RoutingPool(address) as pool:
    #         semaphore = Semaphore()
    #
    #         class Refresher(Thread):
    #
    #             refreshed = None
    #
    #             def run(self):
    #                 self.refreshed = pool.refresh_routing_table()
    #
    #         class BlockingRefresher(Refresher):
    #
    #             @classmethod
    #             def blocking_update(cls):
    #                 pool.routing_table.update(table)
    #                 semaphore.acquire()
    #                 semaphore.release()
    #                 return table
    #
    #             def run(self):
    #                 with patch.object(RoutingConnectionPool, "update_routing_table",
    #                                   side_effect=self.blocking_update):
    #                     super(BlockingRefresher, self).run()
    #
    #         first = BlockingRefresher()
    #         second = Refresher()
    #
    #         assert not pool.routing_table.is_fresh()
    #
    #         semaphore.acquire()
    #         first.start()
    #         second.start()
    #         sleep(1)
    #         assert not second.is_alive()  # second call should return immediately without blocking
    #         second.join()
    #         semaphore.release()
    #         first.join()
    #
    #         assert first.refreshed
    #         assert not second.refreshed
    #         assert pool.routing_table.is_fresh()

    # TODO: fix flaky test
    # def test_concurrent_refreshes_should_block_if_stale(self):
    #     address = ("127.0.0.1", 9001)
    #     table = RoutingTable.parse_routing_info([VALID_ROUTING_RECORD])
    #
    #     with RoutingPool(address) as pool:
    #         semaphore = Semaphore()
    #
    #         class Refresher(Thread):
    #
    #             refreshed = None
    #
    #             def run(self):
    #                 self.refreshed = pool.refresh_routing_table()
    #
    #         class BlockingRefresher(Refresher):
    #
    #             @classmethod
    #             def blocking_update(cls):
    #                 semaphore.acquire()
    #                 semaphore.release()
    #                 pool.routing_table.update(table)
    #                 return table
    #
    #             def run(self):
    #                 with patch.object(RoutingConnectionPool, "update_routing_table",
    #                                   side_effect=self.blocking_update):
    #                     super(BlockingRefresher, self).run()
    #
    #         first = BlockingRefresher()
    #         second = Refresher()
    #
    #         assert not pool.routing_table.is_fresh()
    #
    #         semaphore.acquire()
    #         first.start()
    #         second.start()
    #         sleep(1)
    #         assert second.is_alive()  # second call should block
    #         semaphore.release()
    #         second.join()
    #         first.join()
    #
    #         assert first.refreshed
    #         assert not second.refreshed
    #         assert pool.routing_table.is_fresh()


class RoutingConnectionPoolAcquireForReadTestCase(StubTestCase):
    def test_should_refresh(self):
        with StubCluster({9001: "router.script", 9004: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire(access_mode=READ_ACCESS)
                assert pool.routing_table.is_fresh()

    def test_connected_to_reader(self):
        with StubCluster({9001: "router.script", 9004: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                assert not pool.routing_table.is_fresh()
                connection = pool.acquire(access_mode=READ_ACCESS)
                assert connection.server.address in pool.routing_table.readers

    def test_should_retry_if_first_reader_fails(self):
        with StubCluster({9001: "router.script",
                          9004: "fail_on_init.script",
                          9005: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire(access_mode=READ_ACCESS)
                assert ("127.0.0.1", 9004) not in pool.routing_table.readers
                assert ("127.0.0.1", 9005) in pool.routing_table.readers


class RoutingConnectionPoolAcquireForWriteTestCase(StubTestCase):
    def test_should_refresh(self):
        with StubCluster({9001: "router.script", 9006: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire(access_mode=WRITE_ACCESS)
                assert pool.routing_table.is_fresh()

    def test_connected_to_writer(self):
        with StubCluster({9001: "router.script", 9006: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                assert not pool.routing_table.is_fresh()
                connection = pool.acquire(access_mode=WRITE_ACCESS)
                assert connection.server.address in pool.routing_table.writers

    def test_should_retry_if_first_writer_fails(self):
        with StubCluster({9001: "router_with_multiple_writers.script",
                          9006: "fail_on_init.script",
                          9007: "empty.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                assert not pool.routing_table.is_fresh()
                _ = pool.acquire(access_mode=WRITE_ACCESS)
                assert ("127.0.0.1", 9006) not in pool.routing_table.writers
                assert ("127.0.0.1", 9007) in pool.routing_table.writers


class RoutingConnectionPoolRemoveTestCase(StubTestCase):
    def test_should_remove_router_from_routing_table_if_present(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9001)
                assert target in pool.routing_table.routers
                pool.remove(target)
                assert target not in pool.routing_table.routers

    def test_should_remove_reader_from_routing_table_if_present(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9004)
                assert target in pool.routing_table.readers
                pool.remove(target)
                assert target not in pool.routing_table.readers

    def test_should_remove_writer_from_routing_table_if_present(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9006)
                assert target in pool.routing_table.writers
                pool.remove(target)
                assert target not in pool.routing_table.writers

    def test_should_not_fail_if_absent(self):
        with StubCluster({9001: "router.script"}):
            address = ("127.0.0.1", 9001)
            with RoutingPool(address) as pool:
                pool.refresh_routing_table()
                target = ("127.0.0.1", 9007)
                pool.remove(target)
