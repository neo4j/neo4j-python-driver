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


from socket import socket

from neo4j.v1 import GraphDatabase, TRUST_CUSTOM_CA_SIGNED_CERTIFICATES
from neo4j.exceptions import AuthError

from test.integration.tools import IntegrationTestCase


class SecurityTestCase(IntegrationTestCase):

    def test_insecure_session_uses_normal_socket(self):
        with GraphDatabase.driver(self.bolt_uri, auth=self.auth_token, encrypted=False) as driver:
            with driver.session() as session:
                result = session.run("RETURN 1")
                connection = session._connection
                assert isinstance(connection.socket, socket)
                assert connection.der_encoded_server_certificate is None
                result.consume()

    def test_custom_ca_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            _ = GraphDatabase.driver(self.bolt_uri, auth=self.auth_token,
                                     trust=TRUST_CUSTOM_CA_SIGNED_CERTIFICATES)

    def test_should_fail_on_incorrect_password(self):
        with self.assertRaises(AuthError):
            with GraphDatabase.driver(self.bolt_uri, auth=("neo4j", "wrong-password")) as driver:
                with driver.session() as session:
                    _ = session.run("RETURN 1")
