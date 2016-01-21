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


from unittest import TestCase

# tag::minimal-example-import[]
from neo4j.v1 import GraphDatabase
# end::minimal-example-import[]


class FreshDatabaseTestCase(TestCase):

    def setUp(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        session.run("MATCH (n) DETACH DELETE n")
        session.close()


class MinimalWorkingExampleTestCase(FreshDatabaseTestCase):

    def test_minimal_working_example(self):
        # tag::minimal-example[]
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()

        session.run("CREATE (neo:Person {name:'Neo', age:23})")

        cursor = session.run("MATCH (p:Person) WHERE p.name = 'Neo' RETURN p.age")
        while cursor.next():
            print("Neo is %d years old." % cursor["p.age"])

        session.close()
        # end::minimal-example[]


class ExamplesTestCase(FreshDatabaseTestCase):

    def test_construct_driver(self):
        # tag::construct-driver[]
        driver = GraphDatabase.driver("bolt://localhost")
        # end::construct-driver[]
        return driver

    def test_configuration(self):
        # tag::configuration[]
        driver = GraphDatabase.driver("bolt://localhost", max_pool_size=10)
        # end::configuration[]
        return driver

    def test_tls_require_encryption(self):
        # tag::tls-require-encryption[]
        # TODO: Unfortunately, this feature is not yet implemented for Python
        pass
        # end::tls-require-encryption[]

    def test_tls_trust_on_first_use(self):
        # tag::tls-trust-on-first-use[]
        # TODO: Unfortunately, this feature is not yet implemented for Python
        pass
        # end::tls-trust-on-first-use[]

    def test_tls_signed(self):
        # tag::tls-signed[]
        # TODO: Unfortunately, this feature is not yet implemented for Python
        pass
        # end::tls-signed[]

    def test_statement(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::statement[]
        session.run("CREATE (person:Person {name: {name}})", {"name": "Neo"}).close()
        # end::statement[]
        session.close()

    def test_statement_without_parameters(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::statement-without-parameters[]
        session.run("CREATE (person:Person {name: 'Neo'})").close()
        # end::statement-without-parameters[]
        session.close()

    def test_result_cursor(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::result-cursor[]
        search_term = "hammer"
        cursor = session.run("MATCH (tool:Tool) WHERE tool.name CONTAINS {term} "
                             "RETURN tool.name", {"term": search_term})
        print("List of tools called %r:" % search_term)
        while cursor.next():
            print(cursor["tool.name"])
        # end::result-cursor[]
        session.close()

    def test_cursor_nesting(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::retain-result-query[]
        cursor = session.run("MATCH (person:Person) WHERE person.dept = {dept} "
                             "RETURN id(person) AS minion", {"dept": "IT"})
        while cursor.next():
            session.run("MATCH (person) WHERE id(person) = {id} "
                        "MATCH (boss:Person) WHERE boss.name = {boss} "
                        "CREATE (person)-[:REPORTS_TO]->(boss)", {"id": cursor["minion"], "boss": "Bob"})
        # end::retain-result-query[]
        session.close()

    def test_result_retention(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::retain-result-process[]
        cursor = session.run("MATCH (person:Person) WHERE person.dept = {dept} "
                             "RETURN id(person) AS minion", {"dept": "IT"})
        minion_records = list(cursor.stream())

        for record in minion_records:
            session.run("MATCH (person) WHERE id(person) = {id} "
                        "MATCH (boss:Person) WHERE boss.name = {boss} "
                        "CREATE (person)-[:REPORTS_TO]->(boss)", {"id": record["minion"], "boss": "Bob"})
        # end::retain-result-process[]
        session.close()

    def test_transaction_commit(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::transaction-commit[]
        tx = session.begin_transaction()
        tx.run("CREATE (p:Person {name: 'The One'})")
        tx.commit()
        # end::transaction-commit[]
        cursor = session.run("MATCH (p:Person {name: 'The One'}) RETURN count(p)")
        assert cursor.next()
        assert cursor["count(p)"] == 1
        assert cursor.at_end()
        session.close()

    def test_transaction_rollback(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::transaction-rollback[]
        tx = session.begin_transaction()
        tx.run("CREATE (p:Person {name: 'The One'})")
        tx.rollback()
        # end::transaction-rollback[]
        cursor = session.run("MATCH (p:Person {name: 'The One'}) RETURN count(p)")
        assert cursor.next()
        assert cursor["count(p)"] == 0
        assert cursor.at_end()
        session.close()

    def test_result_summary_query_profile(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::result-summary-query-profile[]
        cursor = session.run("PROFILE MATCH (p:Person {name: {name}}) "
                             "RETURN id(p)", {"name": "The One"})
        summary = cursor.summarize()
        print(summary.statement_type)
        print(summary.profile)
        # end::result-summary-query-profile[]
        session.close()

    def test_result_summary_notifications(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        # tag::result-summary-notifications[]
        summary = session.run("EXPLAIN MATCH (a), (b) RETURN a,b").summarize()
        for notification in summary.notifications:
            print(notification)
        # end::result-summary-notifications[]
        session.close()
