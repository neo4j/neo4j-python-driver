#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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

from neo4j.v1 import GraphDatabase


class ExamplesTestCase(TestCase):

    def setUp(self):
        session = GraphDatabase.driver("bolt://localhost").session()
        session.run("MATCH (n) DETACH DELETE n")
        session.close()

    def test_minimum_snippet(self):
        #tag::minimum-snippet[]
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        session.run("CREATE (neo:Person {name:'Neo', age:23})")

        for record in session.run("MATCH (p:Person) WHERE p.name = 'Neo' RETURN p.age"):
            print("Neo is {0} years old.".format(record["p.age"]))
        session.close()
        #end::minimum-snippet[]

    def test_statement_with_parameters(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        #tag::statement[]
        result = session.run("CREATE (p:Person { name: {name} })", {"name": "The One"})
        ones_created = result.summary.statistics.nodes_created
        print("There were {0} the ones created.".format(ones_created))
        #end::statement[]
        assert ones_created == 1
        session.close()

    def test_statement_without_parameters(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        #tag::statement-without-parameters[]
        result = session.run("CREATE (p:Person { name: 'The One' })")
        ones_created = result.summary.statistics.nodes_created
        print("There were {0} the ones created.".format(ones_created))
        #end::statement-without-parameters[]
        assert ones_created == 1
        session.close()

    def test_commit_a_transaction(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        #tag::transaction-commit[]
        tx = session.begin_transaction()
        tx.run("CREATE (p:Person { name: 'The One' })")
        tx.commit()
        #end::transaction-commit[]
        res = session.run("MATCH (p:Person { name: 'The One' }) RETURN count(p)")
        assert res[0]["count(p)"] == 1
        session.close()

    def test_rollback_a_transaction(self):
        driver = GraphDatabase.driver("bolt://localhost")
        session = driver.session()
        #tag::transaction-rollback[]
        tx = session.begin_transaction()
        tx.run("CREATE (p:Person { name: 'The One' })")
        tx.rollback()
        #end::transaction-rollback[]
        res = session.run("MATCH (p:Person { name: 'The One' }) RETURN count(p)")
        assert res[0]["count(p)"] == 0
        session.close()

    def test_require_encryption(self):
        #tag::tls-require-encryption[]
        #Unfortunately, this feature is not yet implemented for Python
        pass
        #end::tls-require-encryption[]

    def test_trust_on_first_use(self):
        #tag::tls-trust-on-first-use[]
        #Unfortunately, this feature is not yet implemented for Python
        pass
        #end::tls-trust-on-first-use[]

    def test_signed_certificate(self):
        #tag::tls-signed[]
        #Unfortunately, this feature is not yet implemented for Python
        pass
        #end::tls-signed[]
