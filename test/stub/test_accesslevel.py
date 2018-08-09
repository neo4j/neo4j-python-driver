#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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


from neo4j.v1 import GraphDatabase, CypherError, TransientError

from test.stub.tools import StubTestCase, StubCluster


class AccessLevelTestCase(StubTestCase):

    def test_read_transaction(self):
        with StubCluster({9001: "router.script", 9004: "return_1_in_tx.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work(tx):
                        total = 0
                        for record in tx.run("RETURN 1"):
                            total += record[0]
                        return total

                    value = session.read_transaction(unit_of_work)
                    assert value == 1

    def test_write_transaction(self):
        with StubCluster({9001: "router.script", 9006: "return_1_in_tx.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work(tx):
                        total = 0
                        for record in tx.run("RETURN 1"):
                            total += record[0]
                        return total

                    value = session.write_transaction(unit_of_work)
                    assert value == 1

    def test_read_transaction_with_error(self):
        with StubCluster({9001: "router.script", 9004: "error_in_tx.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work(tx):
                        tx.run("X")

                    with self.assertRaises(CypherError):
                        _ = session.read_transaction(unit_of_work)

    def test_write_transaction_with_error(self):
        with StubCluster({9001: "router.script", 9006: "error_in_tx.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work(tx):
                        tx.run("X")

                    with self.assertRaises(CypherError):
                        _ = session.write_transaction(unit_of_work)

    def test_two_subsequent_read_transactions(self):
        with StubCluster({9001: "router.script", 9004: "return_1_in_tx_twice.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work(tx):
                        total = 0
                        for record in tx.run("RETURN 1"):
                            total += record[0]
                        return total

                    value = session.read_transaction(unit_of_work)
                    assert value == 1
                    value = session.read_transaction(unit_of_work)
                    assert value == 1

    def test_two_subsequent_write_transactions(self):
        with StubCluster({9001: "router.script", 9006: "return_1_in_tx_twice.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work(tx):
                        total = 0
                        for record in tx.run("RETURN 1"):
                            total += record[0]
                        return total

                    value = session.write_transaction(unit_of_work)
                    assert value == 1
                    value = session.write_transaction(unit_of_work)
                    assert value == 1

    def test_read_tx_then_write_tx(self):
        with StubCluster({9001: "router.script", 9004: "return_1_in_tx.script", 9006: "return_2_in_tx.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work_1(tx):
                        total = 0
                        for record in tx.run("RETURN 1"):
                            total += record[0]
                        return total

                    def unit_of_work_2(tx):
                        total = 0
                        for record in tx.run("RETURN 2"):
                            total += record[0]
                        return total

                    value = session.read_transaction(unit_of_work_1)
                    assert session.last_bookmark() == "bookmark:1"
                    assert value == 1
                    value = session.write_transaction(unit_of_work_2)
                    assert session.last_bookmark() == "bookmark:2"
                    assert value == 2

    def test_write_tx_then_read_tx(self):
        with StubCluster({9001: "router.script", 9004: "return_2_in_tx.script", 9006: "return_1_in_tx.script"}):
            uri = "bolt+routing://localhost:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:

                    def unit_of_work_1(tx):
                        total = 0
                        for record in tx.run("RETURN 1"):
                            total += record[0]
                        return total

                    def unit_of_work_2(tx):
                        total = 0
                        for record in tx.run("RETURN 2"):
                            total += record[0]
                        return total

                    value = session.write_transaction(unit_of_work_1)
                    assert value == 1
                    value = session.read_transaction(unit_of_work_2)
                    assert value == 2

    def test_no_retry_read_on_user_canceled_tx(self):
        with StubCluster({9001: "router.script", 9004: "user_canceled_tx.script.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    def unit_of_work(tx):
                        tx.run("RETURN 1")

                    with self.assertRaises(TransientError):
                        _ = session.read_transaction(unit_of_work)

    def test_no_retry_write_on_user_canceled_tx(self):
        with StubCluster({9001: "router.script", 9006: "user_canceled_tx.script.script"}):
            uri = "bolt+routing://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, encrypted=False) as driver:
                with driver.session() as session:
                    def unit_of_work(tx):
                        tx.run("RETURN 1")

                    with self.assertRaises(TransientError):
                        _ = session.write_transaction(unit_of_work)
