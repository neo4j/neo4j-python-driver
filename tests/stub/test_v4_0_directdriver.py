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


from neo4j.exceptions import (
    ServiceUnavailable,
    SessionExpired,
)

from neo4j import GraphDatabase, BoltDriver

from tests.stub.conftest import StubTestCase, StubCluster
from logging import getLogger

log = getLogger("neo4j")


class BoltDriverTestCase(StubTestCase):

    def test_bolt_uri_constructs_bolt_driver(self):
        # from logging import getLogger, StreamHandler, DEBUG
        # handler = StreamHandler()
        # handler.setLevel(DEBUG)
        # getLogger("neo4j").addHandler(handler)

        with StubCluster("v4/empty.script"):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                assert isinstance(driver, BoltDriver)

    def test_direct_disconnect_on_run(self):
        with StubCluster("v4/disconnect_on_run.script"):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                with self.assertRaises(SessionExpired):
                    with driver.session() as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_direct_pull_all_bolt_3_syntax_auto_commit(self):
        with StubCluster("v4/pull_all_bolt_3_syntax.script"):
            uri = "bolt://127.0.0.1:9001"
            # Is this behaviour correct for the auto commit ?
            with self.assertRaises(ServiceUnavailable):
                with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                    with driver.session() as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_direct_pull_all_bolt_3_syntax_transaction_function(self):
        with StubCluster("v4/pull_all_bolt_3_syntax.script"):
            uri = "bolt://127.0.0.1:9001"
            # Is this behaviour correct for the transaction function ?
            with self.assertRaises(ServiceUnavailable):
                with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                    with driver.session() as session:
                        session.read_transaction(lambda tx: tx.run("RETURN $x", {"x": 1})).consume()

    def test_direct_disconnect_on_pull_all_new_bolt_4_syntax(self):
        with StubCluster("v4/disconnect_on_pull_all_new_bolt_4_syntax.script"):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                with self.assertRaises(SessionExpired):
                    with driver.session() as session:
                        session.run("RETURN $x", {"x": 1}).consume()

    def test_direct_pull_all_new_bolt_4_syntax(self):
        with StubCluster("v4/pull_all_new_bolt_4_syntax.script"):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
                with driver.session() as session:
                    session.run("UNWIND $list AS x RETURN x", {"list": list(range(4))}).consume()

    # def test_direct_disconnect_on_pull_n_new_bolt_4_syntax(self):
    #     # Message mismatch (expected <PULL {"n": 100}>, received <PULL {"n": -1}>)
    #     with StubCluster("v4/disconnect_on_pull_n_new_bolt_4_syntax.script"):
    #         uri = "bolt://127.0.0.1:9001"
    #         with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
    #             with self.assertRaises(SessionExpired):
    #                 with driver.session() as session:
    #                     session.run("UNWIND $list AS x RETURN x", {"list": list(range(4))}).consume()

    # Message mismatch (expected <PULL {"n": 100}>, received <PULL {"n": -1}>)
    # def test_direct_pull_n_new_bolt_4_syntax(self):
    #     with StubCluster("v4/pull_n_new_bolt_4_syntax.script"):
    #         uri = "bolt://127.0.0.1:9001"
    #         with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
    #             with driver.session() as session:
    #                 session.run("UNWIND $list AS x RETURN x", {"list": list(range(4))}).consume()

    def test_direct_session_close_after_server_close(self):
        with StubCluster("v4/disconnect_after_init.script"):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, max_retry_time=0,
                                      acquire_timeout=3, connect_timeout=20, user_agent="test") as driver:
                with driver.session() as session:
                    with self.assertRaises(ServiceUnavailable):
                        session.write_transaction(lambda tx: tx.run("CREATE (a:Item)"), timeout=100)

    # def test_direct_session_database_name(self):
    #     with StubCluster("v4/disconnect_after_init.script"):
    #         uri = "bolt://127.0.0.1:9001"
    #         with GraphDatabase.driver(uri, auth=self.auth_token) as driver:
    #             with driver.session() as session:
    #                 with self.assertRaises(ServiceUnavailable):
    #                     session.write_transaction(lambda tx: tx.run("CREATE (a:Item)"), timeout=100)


    # MaxConnectionLifetime = 60 * 60 seconds # max_age, max_connection_lifetime
    # MaxConnectionPoolSize = 100 connections # max_size, max_connection_pool_size
    # ConnectionAcquisitionTimeout = 60 seconds # acquire_timeout
    # ConnectionTimeout = 30 seconds # connect_timeout
    # MaxRetryTime = 30 seconds # max_retry_time
    # KeepAlive = True # keep_alive

    # class IOPool:
    #     """ A collection of connections to one or more server addresses.
    #     """
    #
    #     _default_acquire_timeout = 60  # seconds
    #
    #     _default_max_size = 100


    # fetch_size
    #
    # Specify how many records to fetch in each batch for this session.
    # This config will overrides the default value set on {@link Config#fetchSize()}.
    # This config is only valid when the driver is used with servers that support Bolt V4 (Server version 4.0 and later).
    #
    # Bolt V4 enables pulling records in batches to allow client to take control of data population and apply back pressure to server.
    # This config specifies the default fetch size for all query runs using {@link Session} and {@link AsyncSession}.
    # By default, the value is set to {@code 1000}.
    # Use {@code -1} to disables back pressure and config client to pull all records at once after each run.