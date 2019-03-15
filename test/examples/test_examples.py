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


from contextlib import contextmanager
from io import StringIO
import sys
from unittest import TestCase

from test.integration.tools import IntegrationTestCase


# Python2 doesn't have contextlib.redirect_stdout()
#   http://eli.thegreenplace.net/2015/redirecting-all-kinds-of-stdout-in-python/
@contextmanager
def stdout_redirector(stream):
    old_stdout = sys.stdout
    sys.stdout = stream
    try:
        yield
    finally:
        sys.stdout = old_stdout


class ExamplesTest(IntegrationTestCase):

    def setUp(self):
        self.clean()

    def test_autocommit_transaction_example(self):
        from test.examples.autocommit_transaction_example import AutocommitTransactionExample

        example = AutocommitTransactionExample(self.bolt_uri, self.user, self.password)
        example.add_person('Alice')

        self.assertTrue(self.person_count('Alice') > 0)

    def test_config_connection_pool_example(self):
        from test.examples.config_connection_pool_example import ConfigConnectionPoolExample
        example = ConfigConnectionPoolExample(self.bolt_uri, self.user, self.password)
        self.assertTrue(example.can_connect())

    def test_connection_timeout_example(self):
        from test.examples.config_connection_timeout_example import ConfigConnectionTimeoutExample
        example = ConfigConnectionTimeoutExample(self.bolt_uri, self.user, self.password)
        self.assertTrue(example.can_connect())

    def test_load_balancing_strategy_example(self):
        from test.examples.config_load_balancing_strategy_example import ConfigLoadBalancingStrategyExample
        example = ConfigLoadBalancingStrategyExample(self.bolt_uri, self.user, self.password)
        self.assertTrue(example.can_connect())

    def test_max_retry_time_example(self):
        from test.examples.config_max_retry_time_example import ConfigMaxRetryTimeExample
        example = ConfigMaxRetryTimeExample(self.bolt_uri, self.user, self.password)
        self.assertTrue(example.can_connect())

    def test_basic_auth_example(self):
        from test.examples.auth_example import BasicAuthExample

        example = BasicAuthExample(self.bolt_uri, self.user, self.password)

        self.assertTrue(example.can_connect())

    def test_custom_auth_example(self):
        from test.examples.custom_auth_example import CustomAuthExample

        example = CustomAuthExample(self.bolt_uri, self.user, self.password, None, "basic", **{"key":"value"})

        self.assertTrue(example.can_connect())

    def test_config_unencrypted_example(self):
        from test.examples.config_unencrypted_example import ConfigUnencryptedExample

        example = ConfigUnencryptedExample(self.bolt_uri, self.user, self.password)

        self.assertIsInstance(example, ConfigUnencryptedExample)

    def test_cypher_error_example(self):
        from test.examples.cypher_error_example import CypherErrorExample

        f = StringIO()
        with stdout_redirector(f):
            example = CypherErrorExample(self.bolt_uri, self.user, self.password)
            try:
                example.get_employee_number('Alice')
            except:
                pass

        self.assertTrue(f.getvalue().startswith(
            """Invalid input 'L': expected 't/T' (line 1, column 3 (offset: 2))
"SELECT * FROM Employees WHERE name = $name"
   ^"""))

    def test_driver_lifecycle_example(self):
        from test.examples.driver_lifecycle_example import DriverLifecycleExample

        example = DriverLifecycleExample(self.bolt_uri, self.user, self.password)
        example.close()

        self.assertIsInstance(example, DriverLifecycleExample)

    def test_hello_world_example(self):
        from test.examples.hello_world_example import HelloWorldExample

        f = StringIO()
        with stdout_redirector(f):
            example = HelloWorldExample(self.bolt_uri, self.user, self.password)
            example.print_greeting("hello, world")
            example.close()

        self.assertTrue(f.getvalue().startswith("hello, world, from node"))
        
    def test_read_write_transaction_example(self):
        from test.examples.read_write_transaction_example import ReadWriteTransactionExample

        example = ReadWriteTransactionExample(self.bolt_uri, self.user, self.password)
        node_count = example.add_person('Alice')

        self.assertTrue(node_count > 0)

    def test_result_consume_example(self):
        from test.examples.result_consume_example import ResultConsumeExample

        self.write("CREATE (a:Person {name: 'Alice'})")
        self.write("CREATE (a:Person {name: 'Bob'})")
        example = ResultConsumeExample(self.bolt_uri, self.user, self.password)
        people = list(example.get_people())

        self.assertEqual(['Alice', 'Bob'], people)

    def test_result_retain_example(self):
        from test.examples.result_retain_example import ResultRetainExample

        self.write("CREATE (a:Person {name: 'Alice'})")
        self.write("CREATE (a:Person {name: 'Bob'})")
        example = ResultRetainExample(self.bolt_uri, self.user, self.password)
        example.add_employees('Acme')
        employee_count = self.read("MATCH (emp:Person)-[:WORKS_FOR]->(com:Company) WHERE com.name = 'Acme' RETURN count(emp)").single()[0]

        self.assertEqual(employee_count, 2)

    def test_session_example(self):
        from test.examples.session_example import SessionExample

        example = SessionExample(self.bolt_uri, self.user, self.password)
        example.add_person("Alice")

        self.assertIsInstance(example, SessionExample)
        self.assertEqual(self.person_count("Alice"), 1)

    def test_transaction_function_example(self):
        from test.examples.transaction_function_example import TransactionFunctionExample

        example = TransactionFunctionExample(self.bolt_uri, self.user, self.password)
        example.add_person("Alice")

        self.assertEqual(self.person_count("Alice"), 1)

    def read(self, statement):
        from neo4j import GraphDatabase
        with GraphDatabase.driver(self.bolt_uri, auth=self.auth_token) as driver:
            with driver.session() as session:
                return session.read_transaction(lambda tx: tx.run(statement))

    def write(self, statement):
        from neo4j import GraphDatabase
        with GraphDatabase.driver(self.bolt_uri, auth=self.auth_token) as driver:
            with driver.session() as session:
                return session.write_transaction(lambda tx: tx.run(statement))

    def clean(self):
        self.write("MATCH (a) DETACH DELETE a")
        
    def person_count(self, name):
        from neo4j import GraphDatabase
        with GraphDatabase.driver(self.bolt_uri, auth=self.auth_token) as driver:
            with driver.session() as session:
                record_list = list(session.run("MATCH (a:Person {name: $name}) RETURN count(a)", {"name": name}))
                return len(record_list)


class ServiceUnavailableTest(IntegrationTestCase):

    def test_service_unavailable_example(self):
        from test.examples.service_unavailable_example import ServiceUnavailableExample

        example = ServiceUnavailableExample(self.bolt_uri, self.user, self.password)
        self._stop_server()

        self.assertTrue(example.add_item())
