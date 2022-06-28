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


# tag::session-import[]
# end::session-import[]

# tag::session-import[]
from neo4j.api import READ_ACCESS
# end::session-import[]

# python -m pytest tests/integration/examples/test_session_example.py -s -v


def session_example(driver):
    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _")

    # tag::session[]
    def add_person(name):
        with driver.session() as session:
            session.run("CREATE (a:Person {name: $name})", name=name)
    # end::session[]

    add_person("Alice")
    add_person("Bob")

    with driver.session() as session:
        persons = session.run("MATCH (a:Person) RETURN count(a)").single().value()

    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _")

    return persons


def test_example(driver):
    persons = session_example(driver)
    assert persons == 2


def test_session_database_config_example(driver):
    # tag::session-config-database[]
    with driver.session(
            database="<the database name>",
            default_access_mode=READ_ACCESS) as session:
        # end::session-config-database[]
        session.last_bookmark()
