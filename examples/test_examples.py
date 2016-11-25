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


from unittest import skip, skipUnless

from neo4j.v1 import TRUST_ON_FIRST_USE, TRUST_SIGNED_CERTIFICATES, SSL_AVAILABLE
from neo4j.v1.exceptions import CypherError
from test.util import ServerTestCase

# Do not change the contents of this tagged section without good reason*
# tag::minimal-example-import[]
from neo4j.v1 import GraphDatabase, basic_auth
# end::minimal-example-import[]
# (* "good reason" is defined as knowing what you are doing)


auth_token = basic_auth("neo4j", "neo4j")


class FreshDatabaseTestCase(ServerTestCase):

    def setUp(self):
        ServerTestCase.setUp(self)
        with GraphDatabase.driver("bolt://localhost:7687", auth=auth_token).session() as session:
            with session.begin_transaction() as tx:
                tx.run("MATCH (n) DETACH DELETE n")
                tx.success = True


class MinimalWorkingExampleTestCase(FreshDatabaseTestCase):

    def test_minimal_working_example(self):
        # tag::minimal-example[]
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))

        with driver.session() as session:

            with session.begin_transaction() as tx:
                tx.run("CREATE (a:Person {name: {name}, title: {title}})",
                       {"name": "Arthur", "title": "King"})
                tx.success = True

            with session.begin_transaction() as tx:
                result = tx.run("MATCH (a:Person) WHERE a.name = {name} "
                                "RETURN a.name AS name, a.title AS title",
                                {"name": "Arthur"})
                for record in result:
                    print("%s %s" % (record["title"], record["name"]))

        # TODO: driver.close()
        # end::minimal-example[]


class ExamplesTestCase(FreshDatabaseTestCase):

    def test_construct_driver(self):
        # tag::construct-driver[]
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))
        # end::construct-driver[]
        return driver

    def test_configuration(self):
        # tag::configuration[]
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"), max_pool_size=10)
        # end::configuration[]
        return driver

    @skipUnless(SSL_AVAILABLE, "Bolt over TLS is not supported by this version of Python")
    def test_tls_require_encryption(self):
        # tag::tls-require-encryption[]
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=True)
        # end::tls-require-encryption[]

    @skipUnless(SSL_AVAILABLE, "Bolt over TLS is not supported by this version of Python")
    def test_tls_trust_on_first_use(self):
        # tag::tls-trust-on-first-use[]
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=True, trust=TRUST_ON_FIRST_USE)
        # end::tls-trust-on-first-use[]
        assert driver

    @skip("testing verified certificates not yet supported ")
    def test_tls_signed(self):
        # tag::tls-signed[]
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=True, trust=TRUST_SIGNED_CERTIFICATES)
        # end::tls-signed[]
        assert driver

    @skipUnless(SSL_AVAILABLE, "Bolt over TLS is not supported by this version of Python")
    def test_connect_with_auth_disabled(self):
        # tag::connect-with-auth-disabled[]
        driver = GraphDatabase.driver("bolt://localhost:7687", encrypted=True)
        # end::connect-with-auth-disabled[]
        assert driver

    def test_statement(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                # tag::statement[]
                result = transaction.run("CREATE (person:Person {name: {name}})", {"name": "Arthur"})
                transaction.success = True
                # end::statement[]
                result.consume()
        # TODO driver.close()

    def test_statement_without_parameters(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                # tag::statement-without-parameters[]
                result = transaction.run("CREATE (person:Person {name: 'Arthur'})")
                transaction.success = True
                # end::statement-without-parameters[]
                result.consume()
        # TODO driver.close()

    def test_result_traversal(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                # tag::result-traversal[]
                search_term = "Sword"
                result = transaction.run("MATCH (weapon:Weapon) WHERE weapon.name CONTAINS {term} "
                                     "RETURN weapon.name", {"term": search_term})
                print("List of weapons called %r:" % search_term)
                for record in result:
                    print(record["weapon.name"])
                # end::result-traversal[]
        # TODO driver.close()

    def test_access_record(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                # tag::access-record[]
                search_term = "Arthur"
                result = transaction.run("MATCH (weapon:Weapon) WHERE weapon.owner CONTAINS {term} "
                                     "RETURN weapon.name, weapon.material, weapon.size",
                                         {"term": search_term})
                print("List of weapons owned by %r:" % search_term)
                for record in result:
                    print(", ".join("%s: %s" % (key, record[key]) for key in record.keys()))
                # end::access-record[]
        # driver.close()

    def test_result_retention(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        # tag::retain-result[]
        with driver.session() as session:

            with session.begin_transaction() as tx:
                result = tx.run("MATCH (knight:Person:Knight) WHERE knight.castle = {castle} "
                                     "RETURN knight.name AS name", {"castle": "Camelot"})
                retained_result = list(result)

                for record in retained_result:
                    print("%s is a knight of Camelot" % record["name"])
                # end::retain-result[]
                assert isinstance(retained_result, list)
        # TODO driver.close()

    def test_nested_statements(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            # tag::nested-statements[]
            with session.begin_transaction() as transaction:
                result = transaction.run("MATCH (knight:Person:Knight) WHERE knight.castle = {castle} "
                                     "RETURN id(knight) AS knight_id", {"castle": "Camelot"})
                for record in result:
                    with session.begin_transaction() as tx:
                        tx.run("MATCH (knight) WHERE id(knight) = {id} "
                                    "MATCH (king:Person) WHERE king.name = {king} "
                                    "CREATE (knight)-[:DEFENDS]->(king)",
                               {"id": record["knight_id"], "king": "Arthur"})
                        tx.success = True
            # end::nested-statements[]
        # TODO driver.close()

    def test_transaction_commit(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            # tag::transaction-commit[]
            with session.begin_transaction() as tx:
                tx.run("CREATE (:Person {name: 'Guinevere'})")
                tx.success = True
            # end::transaction-commit[]
            with session.begin_transaction() as tx:
                result = tx.run("MATCH (p:Person {name: 'Guinevere'}) RETURN count(p)")
                record = next(iter(result))
                assert record["count(p)"] == 1
        # TODO driver.close()

    def test_transaction_rollback(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            # tag::transaction-rollback[]
            with session.begin_transaction() as tx:
                tx.run("CREATE (:Person {name: 'Merlin'})")
                tx.success = False
            # end::transaction-rollback[]
            with session.begin_transaction() as tx:
                result = tx.run("MATCH (p:Person {name: 'Merlin'}) RETURN count(p)")
                record = next(iter(result))
                assert record["count(p)"] == 0
        # TODO driver.close()

    def test_result_summary_query_profile(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                # tag::result-summary-query-profile[]
                result = transaction.run("PROFILE MATCH (p:Person {name: {name}}) "
                                     "RETURN id(p)", {"name": "Arthur"})
                summary = result.consume()
                print(summary.statement_type)
                print(summary.profile)
                # end::result-summary-query-profile[]
        # TODO driver.close()

    def test_result_summary_notifications(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                # tag::result-summary-notifications[]
                result = transaction.run("EXPLAIN MATCH (king), (queen) RETURN king, queen")
                summary = result.consume()
                for notification in summary.notifications:
                    print(notification)
                # end::result-summary-notifications[]
        # TODO driver.close()

    def test_handle_cypher_error(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth_token)
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                with self.assertRaises(RuntimeError):
                    # tag::handle-cypher-error[]
                    try:
                        transaction.run("This will cause a syntax error").consume()
                    except CypherError:
                        raise RuntimeError("Something really bad has happened!")
                    finally:
                        session.close()
                    # end::handle-cypher-error[]
