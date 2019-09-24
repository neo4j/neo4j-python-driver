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


from neo4j.exceptions import ServiceUnavailable

from neo4j import GraphDatabase, BoltDriver

from tests.stub.conftest import StubTestCase, StubCluster


class BoltDriverTestCase(StubTestCase):

    def test_bolt_uri_constructs_bolt_driver(self):
        with StubCluster({9001: "v3/empty.script"}):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                assert isinstance(driver, BoltDriver)

    def test_direct_disconnect_on_run(self):
        with StubCluster({9001: "v3/disconnect_on_run.script"}):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                with self.assertRaises(ServiceUnavailable):
                    with driver.session() as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_direct_disconnect_on_pull_all(self):
        with StubCluster({9001: "v3/disconnect_on_pull_all.script"}):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                with self.assertRaises(ServiceUnavailable):
                    with driver.session() as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_direct_session_close_after_server_close(self):
        with StubCluster({9001: "v3/disconnect_after_init.script"}):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, max_retry_time=0,
                                      user_agent="test") as driver:
                with driver.session() as session:
                    with self.assertRaises(ServiceUnavailable):
                        session.write_transaction(lambda tx: tx.run("CREATE (a:Item)"))
