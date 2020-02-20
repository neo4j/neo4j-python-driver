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


# tag::session-import[]
# end::session-import[]


class SessionExample:

    def __init__(self, driver):
        self.driver = driver

    # tag::session[]
    def add_person(self, name):
        with self.driver.session() as session:
            session.run("CREATE (a:Person {name: $name})", name=name)
    # end::session[]


def test_example(driver):
    eg = SessionExample(driver)
    with eg.driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _")
        eg.add_person("Alice")
        n = session.run("MATCH (a:Person) RETURN count(a)").single().value()
        assert n == 1
