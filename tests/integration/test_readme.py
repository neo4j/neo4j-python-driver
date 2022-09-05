# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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


from pathlib import Path


# python -m pytest tests/integration/test_readme.py -s -v


def test_should_run_readme(uri, auth):
    names = set()
    print = names.add

    # === START: README ===
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver("neo4j://localhost:7687",
                                  auth=("neo4j", "password"))
    # === END: README ===
    driver.close()
    driver = GraphDatabase.driver(uri, auth=auth)
    # === START: README ===

    def add_friend(tx, name, friend_name):
        tx.run("MERGE (a:Person {name: $name}) "
               "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
               name=name, friend_name=friend_name)

    def print_friends(tx, name):
        query = ("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                 "RETURN friend.name ORDER BY friend.name")
        for record in tx.run(query, name=name):
            print(record["friend.name"])

    with driver.session() as session:
        # === END: README ===
        session.run("MATCH (a) DETACH DELETE a")
        # === START: README ===
        session.execute_write(add_friend, "Arthur", "Guinevere")
        session.execute_write(add_friend, "Arthur", "Lancelot")
        session.execute_write(add_friend, "Arthur", "Merlin")
        session.execute_read(print_friends, "Arthur")
        # === END: README ===
        session.run("MATCH (a) DETACH DELETE a")
        # === START: README ===

    driver.close()
    # === END: README ===
    assert names == {"Guinevere", "Lancelot", "Merlin"}


def test_readme_contains_example():
    test_path = Path(__file__)
    readme_path = test_path.parents[2] / "README.rst"

    with test_path.open("r") as fd:
        test_content = fd.read()
    with readme_path.open("r") as fd:
        readme_content = fd.read()

    stripped_test_content = ""

    adding = False
    for line in test_content.splitlines(keepends=True):
        if line.strip() == "# === START: README ===":
            adding = True
            continue
        elif line.strip() == "# === END: README ===":
            adding = False
            continue
        if adding:
            stripped_test_content += line

    assert stripped_test_content in readme_content
