# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# tag::session-import[]
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
        persons = (
            session.run("MATCH (a:Person) RETURN count(a)").single().value()
        )

    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _")

    return persons


def test_example(driver):
    persons = session_example(driver)
    assert persons == 2
