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


# tag::result-consume-import[]
# end::result-consume-import[]


class ResultConsumeExample:

    def __init__(self, driver):
        self.driver = driver

    # tag::result-consume[]
    def get_people(self):
        with self.driver.session() as session:
            return session.read_transaction(self.match_person_nodes)

    @staticmethod
    def match_person_nodes(tx):
        result = tx.run("MATCH (a:Person) RETURN a.name ORDER BY a.name")
        return [record["a.name"] for record in result]
    # end::result-consume[]


def test(driver):
    eg = ResultConsumeExample(driver)
    with eg.driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _")
        session.run("CREATE (a:Person {name: 'Alice'})")
        session.run("CREATE (a:Person {name: 'Bob'})")
    people = list(eg.get_people())
    assert people == ['Alice', 'Bob']
