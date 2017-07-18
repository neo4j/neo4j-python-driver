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


from neo4j.v1 import GraphDatabase, READ_ACCESS, WRITE_ACCESS, SessionExpired, \
    RoutingDriver, RoutingConnectionPool, LeastConnectedLoadBalancingStrategy, LOAD_BALANCING_STRATEGY_ROUND_ROBIN, \
    RoundRobinLoadBalancingStrategy
from neo4j.bolt import ProtocolError, ServiceUnavailable

from test.stub.tools import StubTestCase, StubCluster


class RoutingDriverTestCase(StubTestCase):

    def test_bolt_plus_routing_uri_constructs_routing_driver(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                assert isinstance(driver, RoutingDriver)

    def test_cannot_discover_servers_on_non_router(self):
        with StubCluster({9001: "non_router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with self.assertRaises(ServiceUnavailable):
                with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False):
                    pass

    def test_cannot_discover_servers_on_silent_router(self):
        with StubCluster({9001: "silent_router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with self.assertRaises(ProtocolError):
                with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False):
                    pass

    def test_should_discover_servers_on_driver_construction(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                table = driver._pool.routing_table
                assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002),
                                         ('127.0.0.1', 9003)}
                assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
                assert table.writers == {('127.0.0.1', 9006)}

    def test_should_be_able_to_read(self):
        with StubCluster({9001: "router.script", 9004: "return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9004)

    def test_should_be_able_to_write(self):
        with StubCluster({9001: "router.script", 9006: "create_a.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=WRITE_ACCESS) as session:
                    result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                    assert not list(result)
                    assert result.summary().server.address == ('127.0.0.1', 9006)

    def test_should_be_able_to_write_as_default(self):
        with StubCluster({9001: "router.script", 9006: "create_a.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                    assert not list(result)
                    assert result.summary().server.address == ('127.0.0.1', 9006)

    def test_routing_disconnect_on_run(self):
        with StubCluster({9001: "router.script", 9004: "disconnect_on_run.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with self.assertRaises(SessionExpired):
                    with driver.session(access_mode=READ_ACCESS) as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_routing_disconnect_on_pull_all(self):
        with StubCluster({9001: "router.script", 9004: "disconnect_on_pull_all.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with self.assertRaises(SessionExpired):
                    with driver.session(access_mode=READ_ACCESS) as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_should_disconnect_after_fetching_autocommit_result(self):
        with StubCluster({9001: "router.script", 9004: "return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    assert session._connection is not None
                    result.consume()
                    assert session._connection is None

    def test_should_disconnect_after_explicit_commit(self):
        with StubCluster({9001: "router.script", 9004: "return_1_twice_in_tx.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    with session.begin_transaction() as tx:
                        result = tx.run("RETURN $x", {"x": 1})
                        assert session._connection is not None
                        result.consume()
                        assert session._connection is not None
                        result = tx.run("RETURN $x", {"x": 1})
                        assert session._connection is not None
                        result.consume()
                        assert session._connection is not None
                    assert session._connection is None

    def test_should_reconnect_for_new_query(self):
        with StubCluster({9001: "router.script", 9004: "return_1_twice.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result_1 = session.run("RETURN $x", {"x": 1})
                    assert session._connection is not None
                    result_1.consume()
                    assert session._connection is None
                    result_2 = session.run("RETURN $x", {"x": 1})
                    assert session._connection is not None
                    result_2.consume()
                    assert session._connection is None

    def test_should_retain_connection_if_fetching_multiple_results(self):
        with StubCluster({9001: "router.script", 9004: "return_1_twice.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result_1 = session.run("RETURN $x", {"x": 1})
                    result_2 = session.run("RETURN $x", {"x": 1})
                    assert session._connection is not None
                    result_1.consume()
                    assert session._connection is not None
                    result_2.consume()
                    assert session._connection is None

    def test_two_sessions_can_share_a_connection(self):
        with StubCluster({9001: "router.script", 9004: "return_1_four_times.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                session_1 = driver.session(access_mode=READ_ACCESS)
                session_2 = driver.session(access_mode=READ_ACCESS)

                result_1a = session_1.run("RETURN $x", {"x": 1})
                c = session_1._connection
                result_1a.consume()

                result_2a = session_2.run("RETURN $x", {"x": 1})
                assert session_2._connection is c
                result_2a.consume()

                result_1b = session_1.run("RETURN $x", {"x": 1})
                assert session_1._connection is c
                result_1b.consume()

                result_2b = session_2.run("RETURN $x", {"x": 1})
                assert session_2._connection is c
                result_2b.consume()

                session_2.close()
                session_1.close()

    def test_should_call_get_routing_table_procedure(self):
        with StubCluster({9001: "get_routing_table.script", 9002: "return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9002)

    def test_should_call_get_routing_table_with_context(self):
        with StubCluster({9001: "get_routing_table_with_context.script", 9002: "return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001/?name=molly&age=1"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9002)

    def test_should_serve_read_when_missing_writer(self):
        with StubCluster({9001: "router_no_writers.script", 9005: "return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9005)

    def test_should_error_when_missing_reader(self):
        with StubCluster({9001: "router_no_readers.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with self.assertRaises(ProtocolError):
                GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False)

    def test_default_load_balancing_strategy_is_least_connected(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                self.assertIsInstance(driver, RoutingDriver)
                self.assertIsInstance(driver._pool, RoutingConnectionPool)
                self.assertIsInstance(driver._pool.load_balancing_strategy, LeastConnectedLoadBalancingStrategy)

    def test_can_select_round_robin_load_balancing_strategy(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False,
                                      load_balancing_strategy=LOAD_BALANCING_STRATEGY_ROUND_ROBIN) as driver:
                self.assertIsInstance(driver, RoutingDriver)
                self.assertIsInstance(driver._pool, RoutingConnectionPool)
                self.assertIsInstance(driver._pool.load_balancing_strategy, RoundRobinLoadBalancingStrategy)

    def test_no_other_load_balancing_strategies_are_available(self):
        with StubCluster({9001: "router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with self.assertRaises(ValueError):
                with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False, load_balancing_strategy=-1):
                    pass
