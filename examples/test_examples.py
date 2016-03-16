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


from unittest import skip

from neo4j.v1 import TRUST_ON_FIRST_USE, TRUST_SIGNED_CERTIFICATES
from test.util import ServerTestCase

# Do not change the contents of this tagged section without good reason*
# tag::minimal-example-import[]
from neo4j.v1 import GraphDatabase, basic_auth
# end::minimal-example-import[]
# (* "good reason" is defined as knowing what you are doing)


auth_token = basic_auth("neo4j", "password")


class FreshDatabaseTestCase(ServerTestCase):

    def setUp(self):
        ServerTestCase.setUp(self)
        session = GraphDatabase.driver("bolt://localhost", auth=auth_token).session()
        session.run("MATCH (n) DETACH DELETE n")
        session.close()


class MinimalWorkingExampleTestCase(FreshDatabaseTestCase):

    def test_minimal_working_example(self):
        # tag::minimal-example[]
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
        session = driver.session()

        session.run("CREATE (a:Person {name:'Arthur', title:'King'})", )

        result = session.run("MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title")
        for record in result:
            print("%s %s" % (record["title"], record["name"]))

        session.close()
        # end::minimal-example[]


class ExamplesTestCase(FreshDatabaseTestCase):

    def test_construct_driver(self):
        # tag::construct-driver[]
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
        # end::construct-driver[]
        return driver

    def test_configuration(self):
        # tag::configuration[]
        driver = GraphDatabase.driver("bolt://localhost", max_pool_size=10)
        # end::configuration[]
        return driver

    def test_tls_require_encryption(self):
        # tag::tls-require-encryption[]
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"), encrypted=True)
        # end::tls-require-encryption[]

    def test_tls_trust_on_first_use(self):
        # tag::tls-trust-on-first-use[]
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"), encrypted=True, trust=TRUST_ON_FIRST_USE)
        # end::tls-trust-on-first-use[]
        assert driver

    @skip("testing verified certificates not yet supported ")
    def test_tls_signed(self):
        # tag::tls-signed[]
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"), encrypted=True, trust=TRUST_SIGNED_CERTIFICATES)
        # end::tls-signed[]
        assert driver

    def test_statement(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::statement[]
        result = session.run("CREATE (person:Person {name: {name}})", {"name": "Arthur"})
        # end::statement[]
        result.consume()
        session.close()

    def test_statement_without_parameters(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::statement-without-parameters[]
        result = session.run("CREATE (person:Person {name: 'Arthur'})")
        # end::statement-without-parameters[]
        result.consume()
        session.close()

    def test_result_traversal(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::result-traversal[]
        search_term = "sword"
        result = session.run("MATCH (weapon:Weapon) WHERE weapon.name CONTAINS {term} "
                             "RETURN weapon.name", {"term": search_term})
        print("List of weapons called %r:" % search_term)
        for record in result:
            print(record["weapon.name"])
        # end::result-traversal[]
        session.close()

    def test_result_retention(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        # tag::retain-result[]
        session = driver.session()
        result = session.run("MATCH (knight:Person:Knight) WHERE knight.castle = {castle} "
                             "RETURN knight.name AS name", {"castle": "Camelot"})
        retained_result = list(result)
        session.close()
        for record in retained_result:
            print("%s is a knight of Camelot" % record["name"])
        # end::retain-result[]
        assert isinstance(retained_result, list)

    def test_nested_statements(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::nested-statements[]
        result = session.run("MATCH (knight:Person:Knight) WHERE knight.castle = {castle} "
                             "RETURN id(knight) AS knight_id", {"castle": "Camelot"})
        for record in result:
            session.run("MATCH (knight) WHERE id(knight) = {id} "
                        "MATCH (king:Person) WHERE king.name = {king} "
                        "CREATE (knight)-[:DEFENDS]->(king)", {"id": record["knight_id"], "king": "Arthur"})
        # end::nested-statements[]
        session.close()

    def test_transaction_commit(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::transaction-commit[]
        tx = session.begin_transaction()
        tx.run("CREATE (:Person {name: 'Guinevere'})")
        tx.commit()
        # end::transaction-commit[]
        result = session.run("MATCH (p:Person {name: 'Guinevere'}) RETURN count(p)")
        record = next(result)
        assert record["count(p)"] == 1
        session.close()

    def test_transaction_rollback(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::transaction-rollback[]
        tx = session.begin_transaction()
        tx.run("CREATE (:Person {name: 'Merlin'})")
        tx.rollback()
        # end::transaction-rollback[]
        result = session.run("MATCH (p:Person {name: 'Merlin'}) RETURN count(p)")
        record = next(result)
        assert record["count(p)"] == 0
        session.close()

    def test_result_summary_query_profile(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::result-summary-query-profile[]
        result = session.run("PROFILE MATCH (p:Person {name: {name}}) "
                             "RETURN id(p)", {"name": "Arthur"})
        summary = result.consume()
        print(summary.statement_type)
        print(summary.profile)
        # end::result-summary-query-profile[]
        session.close()

    def test_result_summary_notifications(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
        session = driver.session()
        # tag::result-summary-notifications[]
        result = session.run("EXPLAIN MATCH (king), (queen) RETURN king, queen")
        summary = result.consume()
        for notification in summary.notifications:
            print(notification)
        # end::result-summary-notifications[]
        session.close()
