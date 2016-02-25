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


from test.util import ServerTestCase

# tag::minimal-example-import[]
from neo4j.v1 import GraphDatabase, basic_auth
# end::minimal-example-import[]


class FreshDatabaseTestCase(ServerTestCase):

    def setUp(self):
        ServerTestCase.setUp(self)
        session = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j")).session()
        session.run("MATCH (n) DETACH DELETE n")
        session.close()


class MinimalWorkingExampleTestCase(FreshDatabaseTestCase):

    def test_minimal_working_example(self):
        # tag::minimal-example[]
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()

        session.run("CREATE (neo:Person {name:'Neo', age:23})")

        result = session.run("MATCH (p:Person) WHERE p.name = 'Neo' RETURN p.age")
        while result.next():
            print("Neo is %d years old." % result["p.age"])

        session.close()
        # end::minimal-example[]


class ExamplesTestCase(FreshDatabaseTestCase):

    def test_construct_driver(self):
        # tag::construct-driver[]
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
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
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::statement[]
        session.run("CREATE (person:Person {name: {name}})", {"name": "Neo"}).close()
        # end::statement[]
        session.close()

    def test_statement_without_parameters(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::statement-without-parameters[]
        session.run("CREATE (person:Person {name: 'Neo'})").close()
        # end::statement-without-parameters[]
        session.close()

    def test_result_cursor(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::result-cursor[]
        search_term = "hammer"
        result = session.run("MATCH (tool:Tool) WHERE tool.name CONTAINS {term} "
                             "RETURN tool.name", {"term": search_term})
        print("List of tools called %r:" % search_term)
        while result.next():
            print(result["tool.name"])
        # end::result-cursor[]
        session.close()

    def test_cursor_nesting(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::retain-result-query[]
        result = session.run("MATCH (person:Person) WHERE person.dept = {dept} "
                             "RETURN id(person) AS minion", {"dept": "IT"})
        while result.next():
            session.run("MATCH (person) WHERE id(person) = {id} "
                        "MATCH (boss:Person) WHERE boss.name = {boss} "
                        "CREATE (person)-[:REPORTS_TO]->(boss)", {"id": result["minion"], "boss": "Bob"})
        # end::retain-result-query[]
        session.close()

    def test_result_retention(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::retain-result-process[]
        result = session.run("MATCH (person:Person) WHERE person.dept = {dept} "
                             "RETURN id(person) AS minion", {"dept": "IT"})
        minion_records = list(result.stream())

        for record in minion_records:
            session.run("MATCH (person) WHERE id(person) = {id} "
                        "MATCH (boss:Person) WHERE boss.name = {boss} "
                        "CREATE (person)-[:REPORTS_TO]->(boss)", {"id": record["minion"], "boss": "Bob"})
        # end::retain-result-process[]
        session.close()

    def test_transaction_commit(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::transaction-commit[]
        tx = session.begin_transaction()
        tx.run("CREATE (p:Person {name: 'The One'})")
        tx.commit()
        # end::transaction-commit[]
        result = session.run("MATCH (p:Person {name: 'The One'}) RETURN count(p)")
        assert result.next()
        assert result["count(p)"] == 1
        assert result.at_end
        session.close()

    def test_transaction_rollback(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::transaction-rollback[]
        tx = session.begin_transaction()
        tx.run("CREATE (p:Person {name: 'The One'})")
        tx.rollback()
        # end::transaction-rollback[]
        result = session.run("MATCH (p:Person {name: 'The One'}) RETURN count(p)")
        assert result.next()
        assert result["count(p)"] == 0
        assert result.at_end
        session.close()

    def test_result_summary_query_profile(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::result-summary-query-profile[]
        result = session.run("PROFILE MATCH (p:Person {name: {name}}) "
                             "RETURN id(p)", {"name": "The One"})
        while result.next():
            pass  # skip the records to get to the summary
        print(result.summary.statement_type)
        print(result.summary.profile)
        # end::result-summary-query-profile[]
        session.close()

    def test_result_summary_notifications(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
        session = driver.session()
        # tag::result-summary-notifications[]
        result = session.run("EXPLAIN MATCH (a), (b) RETURN a,b")
        while result.next():
            pass  # skip the records to get to the summary
        for notification in result.summary.notifications:
            print(notification)
        # end::result-summary-notifications[]
        session.close()
