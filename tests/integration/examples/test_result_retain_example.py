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


# tag::result-retain-import[]
# end::result-retain-import[]


class ResultRetainExample:

    def __init__(self, driver):
        self.driver = driver

    # tag::result-retain[]
    def add_employees(self, company_name):
        with self.driver.session() as session:
            employees = 0
            persons = session.read_transaction(self.match_person_nodes)

            for person in persons:
                employees += session.write_transaction(self.add_employee_to_company, person, company_name)

            return employees

    @staticmethod
    def add_employee_to_company(tx, person, company_name):
        tx.run("MATCH (emp:Person {name: $person_name}) "
               "MERGE (com:Company {name: $company_name}) "
               "MERGE (emp)-[:WORKS_FOR]->(com)",
               person_name=person["name"], company_name=company_name)
        return 1

    @staticmethod
    def match_person_nodes(tx):
        return list(tx.run("MATCH (a:Person) RETURN a.name AS name"))
    # end::result-retain[]


def test(bolt_driver):
    eg = ResultRetainExample(bolt_driver)
    with eg.driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _").data()
        session.run("CREATE (a:Person {name: 'Alice'})").data()
        session.run("CREATE (a:Person {name: 'Bob'})").data()
        assert eg.add_employees('Acme') == 2
        n = session.run("MATCH (emp:Person)-[:WORKS_FOR]->(com:Company) "
                        "WHERE com.name = 'Acme' RETURN count(emp)").single().value()
        assert n == 2
