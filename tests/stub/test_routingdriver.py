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


from neo4j import GraphDatabase, READ_ACCESS, WRITE_ACCESS, RoutingDriver
from neo4j.errors import BoltRoutingError
from neo4j.blocking import SessionExpired
from neo4j.exceptions import ServiceUnavailable, ClientError, TransientError

from tests.stub.conftest import StubTestCase, StubCluster


class RoutingDriverTestCase(StubTestCase):

    def test_bolt_plus_routing_uri_constructs_routing_driver(self):
        with StubCluster({9001: "v3/router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                assert isinstance(driver, RoutingDriver)

    def test_cannot_discover_servers_on_non_router(self):
        with StubCluster({9001: "v3/non_router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with self.assertRaises(ServiceUnavailable):
                with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False):
                    pass

    def test_cannot_discover_servers_on_silent_router(self):
        with StubCluster({9001: "v3/silent_router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with self.assertRaises(BoltRoutingError):
                with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False):
                    pass

    def test_should_discover_servers_on_driver_construction(self):
        with StubCluster({9001: "v3/router.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                table = driver._pool.routing_table
                assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002),
                                         ('127.0.0.1', 9003)}
                assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
                assert table.writers == {('127.0.0.1', 9006)}

    def test_should_be_able_to_read(self):
        with StubCluster({9001: "v3/router.script", 9004: "v3/return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9004)

    def test_should_be_able_to_write(self):
        with StubCluster({9001: "v3/router.script", 9006: "v3/create_a.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=WRITE_ACCESS) as session:
                    result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                    assert not list(result)
                    assert result.summary().server.address == ('127.0.0.1', 9006)

    def test_should_be_able_to_write_as_default(self):
        with StubCluster({9001: "v3/router.script", 9006: "v3/create_a.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                    assert not list(result)
                    assert result.summary().server.address == ('127.0.0.1', 9006)

    def test_routing_disconnect_on_run(self):
        with StubCluster({9001: "v3/router.script", 9004: "v3/disconnect_on_run.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with self.assertRaises(SessionExpired):
                    with driver.session(access_mode=READ_ACCESS) as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_routing_disconnect_on_pull_all(self):
        with StubCluster({9001: "v3/router.script", 9004: "v3/disconnect_on_pull_all.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with self.assertRaises(SessionExpired):
                    with driver.session(access_mode=READ_ACCESS) as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_should_disconnect_after_fetching_autocommit_result(self):
        with StubCluster({9001: "v3/router.script", 9004: "v3/return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    assert session._connection is not None
                    result.consume()
                    assert session._connection is None

    def test_should_disconnect_after_explicit_commit(self):
        with StubCluster({9001: "v3/router.script", 9004: "v3/return_1_twice_in_tx.script"}):
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
        with StubCluster({9001: "v3/router.script", 9004: "v3/return_1_twice.script"}):
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
        with StubCluster({9001: "v3/router.script", 9004: "v3/return_1_twice.script"}):
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
        with StubCluster({9001: "v3/router.script", 9004: "v3/return_1_four_times.script"}):
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
        with StubCluster({9001: "v3/get_routing_table.script", 9002: "v3/return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9002)

    def test_should_call_get_routing_table_with_context(self):
        with StubCluster({9001: "v3/get_routing_table_with_context.script", 9002: "v3/return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001/?name=molly&age=1"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9002)

    def test_should_serve_read_when_missing_writer(self):
        with StubCluster({9001: "v3/router_no_writers.script", 9005: "v3/return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert result.summary().server.address == ('127.0.0.1', 9005)

    def test_should_error_when_missing_reader(self):
        with StubCluster({9001: "v3/router_no_readers.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with self.assertRaises(BoltRoutingError):
                GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False)

    def test_forgets_address_on_not_a_leader_error(self):
        with StubCluster({9001: "v3/router.script", 9006: "v3/not_a_leader.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=WRITE_ACCESS) as session:
                    with self.assertRaises(ClientError):
                        _ = session.run("CREATE (n {name:'Bob'})")

                    pool = driver._pool
                    table = pool.routing_table

                    # address might still have connections in the pool, failed instance just can't serve writes
                    assert ('127.0.0.1', 9006) in pool.connections
                    assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                    assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
                    # writer 127.0.0.1:9006 should've been forgotten because of an error
                    assert len(table.writers) == 0

    def test_forgets_address_on_forbidden_on_read_only_database_error(self):
        with StubCluster({9001: "v3/router.script", 9006: "v3/forbidden_on_read_only_database.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=WRITE_ACCESS) as session:
                    with self.assertRaises(ClientError):
                        _ = session.run("CREATE (n {name:'Bob'})")

                    pool = driver._pool
                    table = pool.routing_table

                    # address might still have connections in the pool, failed instance just can't serve writes
                    assert ('127.0.0.1', 9006) in pool.connections
                    assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                    assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
                    # writer 127.0.0.1:9006 should've been forgotten because of an error
                    assert len(table.writers) == 0

    def test_forgets_address_on_service_unavailable_error(self):
        with StubCluster({9001: "v3/router.script", 9004: "v3/rude_reader.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    with self.assertRaises(SessionExpired):
                        _ = session.run("RETURN 1")

                    pool = driver._pool
                    table = pool.routing_table

                    # address should have connections in the pool but be inactive, it has failed
                    assert ('127.0.0.1', 9004) in pool.connections
                    conns = pool.connections[('127.0.0.1', 9004)]
                    conn = conns[0]
                    assert conn._closed == True
                    assert conn.in_use == True
                    assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                    # reader 127.0.0.1:9004 should've been forgotten because of an error
                    assert table.readers == {('127.0.0.1', 9005)}
                    assert table.writers == {('127.0.0.1', 9006)}

                assert conn.in_use == False

    def test_forgets_address_on_database_unavailable_error(self):
        with StubCluster({9001: "v3/router.script", 9004: "v3/database_unavailable.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(access_mode=READ_ACCESS) as session:
                    with self.assertRaises(TransientError) as raised:
                        _ = session.run("RETURN 1")
                    assert raised.exception.title == "DatabaseUnavailable"

                    pool = driver._pool
                    table = pool.routing_table

                    # address should not have connections in the pool, it has failed
                    assert ('127.0.0.1', 9004) not in pool.connections
                    assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                    # reader 127.0.0.1:9004 should've been forgotten because of an raised
                    assert table.readers == {('127.0.0.1', 9005)}
                    assert table.writers == {('127.0.0.1', 9006)}
