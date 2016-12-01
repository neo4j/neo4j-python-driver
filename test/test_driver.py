#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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


from socket import socket
from ssl import SSLSocket
from unittest import skipUnless

from neo4j.v1 import ServiceUnavailable, ProtocolError, READ_ACCESS, WRITE_ACCESS, \
    TRUST_ON_FIRST_USE, TRUST_CUSTOM_CA_SIGNED_CERTIFICATES, GraphDatabase, basic_auth, \
    SSL_AVAILABLE, SessionExpired, DirectDriver
from test.util import ServerTestCase

BOLT_URI = "bolt://localhost:7687"
BOLT_ROUTING_URI = "bolt+routing://localhost:7687"
AUTH_TOKEN = basic_auth("neotest", "neotest")


class DriverTestCase(ServerTestCase):

    def test_driver_with_block(self):
        with GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN, encrypted=False) as driver:
            assert isinstance(driver, DirectDriver)

    def test_must_use_valid_url_scheme(self):
        with self.assertRaises(ProtocolError):
            GraphDatabase.driver("x://xxx", auth=AUTH_TOKEN)

    def test_connections_are_reused(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN)
        session_1 = driver.session()
        connection_1 = session_1.connection
        session_1.close()
        session_2 = driver.session()
        connection_2 = session_2.connection
        session_2.close()
        assert connection_1 is connection_2

    def test_connections_are_not_shared_between_sessions(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN)
        session_1 = driver.session()
        session_2 = driver.session()
        try:
            assert session_1.connection is not session_2.connection
        finally:
            session_1.close()
            session_2.close()

    def test_fail_nicely_when_connecting_to_http_port(self):
        driver = GraphDatabase.driver("bolt://localhost:7474", auth=AUTH_TOKEN, encrypted=False)
        with self.assertRaises(ServiceUnavailable) as context:
            driver.session()


class DirectDriverTestCase(ServerTestCase):

    def tearDown(self):
        self.await_all_servers()

    def test_direct_disconnect_on_run(self):
        self.start_stub_server(9001, "disconnect_on_run.script")
        uri = "bolt://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        try:
            with driver.session() as session:
                with self.assertRaises(ServiceUnavailable):
                    session.run("RETURN $x", {"x": 1}).consume()
        finally:
            driver.close()

    def test_direct_disconnect_on_pull_all(self):
        self.start_stub_server(9001, "disconnect_on_pull_all.script")
        uri = "bolt://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        try:
            with driver.session() as session:
                with self.assertRaises(ServiceUnavailable):
                    session.run("RETURN $x", {"x": 1}).consume()
        finally:
            driver.close()


class RoutingDriverTestCase(ServerTestCase):

    def tearDown(self):
        self.await_all_servers()

    def test_cannot_discover_servers_on_non_router(self):
        self.start_stub_server(9001, "non_router.script")
        uri = "bolt+routing://127.0.0.1:9001"
        with self.assertRaises(ServiceUnavailable):
            GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)

    def test_cannot_discover_servers_on_silent_router(self):
        self.start_stub_server(9001, "silent_router.script")
        uri = "bolt+routing://127.0.0.1:9001"
        with self.assertRaises(ServiceUnavailable):
            GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)

    def test_should_discover_servers_on_driver_construction(self):
        self.start_stub_server(9001, "router.script")
        uri = "bolt+routing://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        router = driver.router
        assert router.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
        assert router.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
        assert router.writers == {('127.0.0.1', 9006)}

    def test_should_be_able_to_read(self):
        self.start_stub_server(9001, "router.script")
        self.start_stub_server(9004, "return_1.script")
        uri = "bolt+routing://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        try:
            with driver.session(READ_ACCESS) as session:
                result = session.run("RETURN $x", {"x": 1})
                for record in result:
                    assert record["x"] == 1
                assert session.connection.address == ('127.0.0.1', 9004)
        finally:
            driver.close()

    def test_should_be_able_to_write(self):
        self.start_stub_server(9001, "router.script")
        self.start_stub_server(9006, "create_a.script")
        uri = "bolt+routing://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        try:
            with driver.session(WRITE_ACCESS) as session:
                result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                assert not list(result)
                assert session.connection.address == ('127.0.0.1', 9006)
        finally:
            driver.close()

    def test_should_be_able_to_write_as_default(self):
        self.start_stub_server(9001, "router.script")
        self.start_stub_server(9006, "create_a.script")
        uri = "bolt+routing://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        try:
            with driver.session() as session:
                result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                assert not list(result)
                assert session.connection.address == ('127.0.0.1', 9006)
        finally:
            driver.close()

    def test_routing_disconnect_on_run(self):
        self.start_stub_server(9001, "router.script")
        self.start_stub_server(9004, "disconnect_on_run.script")
        uri = "bolt+routing://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        try:
            with driver.session(READ_ACCESS) as session:
                with self.assertRaises(SessionExpired):
                    session.run("RETURN $x", {"x": 1}).consume()
        finally:
            driver.close()

    def test_routing_disconnect_on_pull_all(self):
        self.start_stub_server(9001, "router.script")
        self.start_stub_server(9004, "disconnect_on_pull_all.script")
        uri = "bolt+routing://127.0.0.1:9001"
        driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "password"), encrypted=False)
        try:
            with driver.session(READ_ACCESS) as session:
                with self.assertRaises(SessionExpired):
                    session.run("RETURN $x", {"x": 1}).consume()
        finally:
            driver.close()


class SecurityTestCase(ServerTestCase):

    def test_insecure_session_uses_normal_socket(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN, encrypted=False)
        with driver.session() as session:
            connection = session.connection
            assert isinstance(connection.channel.socket, socket)
            assert connection.der_encoded_server_certificate is None

    @skipUnless(SSL_AVAILABLE, "Bolt over TLS is not supported by this version of Python")
    def test_tofu_session_uses_secure_socket(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN, encrypted=True, trust=TRUST_ON_FIRST_USE)
        with driver.session() as session:
            connection = session.connection
            assert isinstance(connection.channel.socket, SSLSocket)
            assert connection.der_encoded_server_certificate is not None

    @skipUnless(SSL_AVAILABLE, "Bolt over TLS is not supported by this version of Python")
    def test_tofu_session_trusts_certificate_after_first_use(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN, encrypted=True, trust=TRUST_ON_FIRST_USE)
        with driver.session() as session:
            connection = session.connection
            certificate = connection.der_encoded_server_certificate
        with driver.session() as session:
            connection = session.connection
            assert connection.der_encoded_server_certificate == certificate

    def test_routing_driver_not_compatible_with_tofu(self):
        with self.assertRaises(ValueError):
            GraphDatabase.driver(BOLT_ROUTING_URI, auth=AUTH_TOKEN, trust=TRUST_ON_FIRST_USE)

    def test_custom_ca_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN,
                                 trust=TRUST_CUSTOM_CA_SIGNED_CERTIFICATES)
