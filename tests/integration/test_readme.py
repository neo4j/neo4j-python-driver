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


import pytest

from neo4j._exceptions import BoltHandshakeError
from neo4j.exceptions import ServiceUnavailable


# python -m pytest tests/integration/test_readme.py -s -v


def test_should_run_readme(uri, auth):
    names = set()
    print = names.add

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=auth)

    def add_friend(tx, name, friend_name):
        tx.run("MERGE (a:Person {name: $name}) "
               "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
               name=name, friend_name=friend_name)

    def print_friends(tx, name):
        for record in tx.run(
                "MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                "RETURN friend.name ORDER BY friend.name", name=name):
            print(record["friend.name"])

    with driver.session() as session:
        session.run("MATCH (a) DETACH DELETE a")

        session.write_transaction(add_friend, "Arthur", "Guinevere")
        session.write_transaction(add_friend, "Arthur", "Lancelot")
        session.write_transaction(add_friend, "Arthur", "Merlin")
        session.read_transaction(print_friends, "Arthur")

        session.run("MATCH (a) DETACH DELETE a")

    driver.close()
    assert names == {"Guinevere", "Lancelot", "Merlin"}
