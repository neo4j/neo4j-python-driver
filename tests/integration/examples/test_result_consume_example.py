#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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


# tag::result-consume-import[]
# end::result-consume-import[]

# python -m pytest tests/integration/examples/test_result_consume_example.py -s -v

def result_consume_example(driver):
    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _").consume()

    with driver.session() as session:
        session.run("CREATE (a:Person {name: $name}) RETURN a", name="Alice").single().value()
        session.run("CREATE (a:Person {name: $name}) RETURN a", name="Bob").single().value()

    # tag::result-consume[]
    def match_person_nodes(tx):
        result = tx.run("MATCH (a:Person) RETURN a.name ORDER BY a.name")
        return [record["a.name"] for record in result]

    with driver.session() as session:
        people = session.read_transaction(match_person_nodes)
    # end::result-consume[]

    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _").consume()

    return people


def test_example(driver):
    people = result_consume_example(driver)
    assert people == ['Alice', 'Bob']
