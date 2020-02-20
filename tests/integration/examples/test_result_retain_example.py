#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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
        self.session = driver.session()

    def close(self):
        self.session.close()

    def delete_all(self):
        self.session.run("MATCH (_) DETACH DELETE _").consume()

    def add_person(self, name):
        return self.session.run("CREATE (a:Person {name: $name}) "
                                "RETURN a", name=name).single().value()

    # tag::result-retain[]
    def add_employees(self, company_name):
        employees = 0
        persons = self.session.read_transaction(self.match_person_nodes)

        for person in persons:
            employees += self.session.write_transaction(self.add_employee_to_company,
                                                        person, company_name)

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

    def count_employees(self, company_name):
        return self.session.run("MATCH (emp:Person)-[:WORKS_FOR]->(com:Company) "
                                "WHERE com.name = $company_name "
                                "RETURN count(emp)", company_name=company_name).single().value()


def test_example(driver):
    eg = ResultRetainExample(driver)
    eg.delete_all()
    eg.add_person("Alice")
    eg.add_person("Bob")
    assert eg.add_employees("Acme") == 2
    assert eg.count_employees("Acme") == 2
    eg.close()
