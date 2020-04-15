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


# python -m pytest tests/integration/examples/test_result_retain_example.py -s -v

def result_retain_example(driver):
    with driver.session() as session:
        session.run("CREATE (a:Person {name: $name}) RETURN a", name="Alice").single().value()
        session.run("CREATE (a:Person {name: $name}) RETURN a", name="Bob").single().value()

    # tag::result-retain[]
    def add_employee_to_company(tx, person, company_name):
        tx.run("MATCH (emp:Person {name: $person_name}) "
               "MERGE (com:Company {name: $company_name}) "
               "MERGE (emp)-[:WORKS_FOR]->(com)",
               person_name=person["name"], company_name=company_name)
        return 1

    def match_person_nodes(tx):
        return list(tx.run("MATCH (a:Person) RETURN a.name AS name"))

    def add_employees(company_name):
        employees = 0
        with driver.session() as session:
            persons = session.read_transaction(match_person_nodes)

            for person in persons:
                employees += session.write_transaction(add_employee_to_company, person, company_name)

        return employees
    # end::result-retain[]

    employees = add_employees(company_name="Neo4j")

    def count_employees(company_name):
        with driver.session() as session:
            head_count = session.run(
                "MATCH (emp:Person)-[:WORKS_FOR]->(com:Company) "
                "WHERE com.name = $company_name "
                "RETURN count(emp)",
                company_name=company_name).single().value()

        return head_count

    head_count = count_employees(company_name="Neo4j")

    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _").consume()

    return employees, head_count


def test_example(driver):
    employees, head_count = result_retain_example(driver)
    assert employees == 2
    assert head_count == 2
