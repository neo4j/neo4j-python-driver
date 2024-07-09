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


import re
from pathlib import Path


# python -m pytest tests/integration/test_readme.py -s -v


def test_should_run_readme(uri, auth):
    names = set()
    print = names.add

    # === START: README ===
    from neo4j import GraphDatabase, RoutingControl  # isort:skip


    URI = "neo4j://localhost:7687"
    AUTH = ("neo4j", "password")


    def add_friend(driver, name, friend_name):
        driver.execute_query(
            "MERGE (a:Person {name: $name}) "
            "MERGE (friend:Person {name: $friend_name}) "
            "MERGE (a)-[:KNOWS]->(friend)",
            name=name, friend_name=friend_name, database_="neo4j",
        )


    def print_friends(driver, name):
        records, _, _ = driver.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
            "RETURN friend.name ORDER BY friend.name",
            name=name, database_="neo4j", routing_=RoutingControl.READ,
        )
        for record in records:
            print(record["friend.name"])


    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        # === END: README ===
        pass
    with GraphDatabase.driver(uri, auth=auth) as driver:
        driver.execute_query("MATCH (a) DETACH DELETE a")
        # === START: README ===
        add_friend(driver, "Arthur", "Guinevere")
        add_friend(driver, "Arthur", "Lancelot")
        add_friend(driver, "Arthur", "Merlin")
        print_friends(driver, "Arthur")
        # === END: README ===
        driver.execute_query("MATCH (a) DETACH DELETE a")

    assert names == {"Guinevere", "Lancelot", "Merlin"}


def test_readme_contains_example():
    test_path = Path(__file__)
    readme_path = test_path.parents[2] / "README.md"

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
            line = re.sub(r"\s+# isort:skip\s+$", "\n", line)
            if line.startswith("    "):
                line = line[4:]
            else:
                assert line == "\n"
            stripped_test_content += line

    assert stripped_test_content in readme_content
