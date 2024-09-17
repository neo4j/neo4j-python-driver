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


# tag::result-consume-import[]
# end::result-consume-import[]


def result_consume_example(driver):
    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _").consume()

    with driver.session() as session:
        for name in ("Alice", "Bob"):
            res = session.run(
                "CREATE (a:Person {name: $name}) RETURN a", name=name
            )
            res.consume()

    # tag::result-consume[]
    def match_person_nodes(tx):
        result = tx.run("MATCH (a:Person) RETURN a.name ORDER BY a.name")
        return [record["a.name"] for record in result]

    with driver.session() as session:
        people = session.execute_read(match_person_nodes)
    # end::result-consume[]

    with driver.session() as session:
        session.run("MATCH (_) DETACH DELETE _").consume()

    return people


def test_example(driver):
    people = result_consume_example(driver)
    assert people == ["Alice", "Bob"]
