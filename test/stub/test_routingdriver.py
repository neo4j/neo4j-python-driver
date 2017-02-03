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


from neo4j.v1 import ServiceUnavailable, GraphDatabase
from neo4j.v1 import RoutingDriver, ProtocolError, READ_ACCESS, WRITE_ACCESS, SessionExpired

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
                table = driver.pool.routing_table
                assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002),
                                         ('127.0.0.1', 9003)}
                assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
                assert table.writers == {('127.0.0.1', 9006)}

    def test_should_be_able_to_read(self):
        with StubCluster({9001: "router.script", 9004: "return_1.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(READ_ACCESS) as session:
                    result = session.run("RETURN $x", {"x": 1})
                    for record in result:
                        assert record["x"] == 1
                    assert session.connection.server.address == ('127.0.0.1', 9004)

    def test_should_be_able_to_write(self):
        with StubCluster({9001: "router.script", 9006: "create_a.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(WRITE_ACCESS) as session:
                    result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                    assert not list(result)
                    assert session.connection.server.address == ('127.0.0.1', 9006)

    def test_should_be_able_to_write_as_default(self):
        with StubCluster({9001: "router.script", 9006: "create_a.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                    assert not list(result)
                    assert session.connection.server.address == ('127.0.0.1', 9006)

    def test_routing_disconnect_on_run(self):
        with StubCluster({9001: "router.script", 9004: "disconnect_on_run.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(READ_ACCESS) as session:
                    with self.assertRaises(SessionExpired):
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_routing_disconnect_on_pull_all(self):
        with StubCluster({9001: "router.script", 9004: "disconnect_on_pull_all.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session(READ_ACCESS) as session:
                    with self.assertRaises(SessionExpired):
                        session.run("RETURN $x", {"x": 1}).consume()
