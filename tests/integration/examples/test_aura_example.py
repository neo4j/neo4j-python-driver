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


import pytest

from contextlib import redirect_stdout
from io import StringIO

# tag::aura-import[]
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
# end::aura-import[]

from neo4j._exceptions import BoltHandshakeError


# python -m pytest tests/integration/examples/test_hello_world_example.py -s -v

# tag::aura[]
class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    # Don't forget to close the driver connection when you are finished with it
    def close(self):
        self.driver.close()

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(self._create_and_return_friendship,
                                               person1_name, person2_name)
            for row in result:
                print(f"Created friendship between: {row['p1']}, {row['p2']}")

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = """
        MERGE (p1:Person { name: $person1_name })
        MERGE (p2:Person { name: $person2_name })
        MERGE (p1)-[:KNOWS]->(p2)
        RETURN p1, p2
        """
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print(f"Found person: {row}")

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = """
        MATCH (p:Person)
        WHERE p.name = $person_name
        RETURN p.name AS name
        """
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


if __name__ == "__main__":
    # Aura requires you to use the "bolt+routing" protocol, and process your queries using an encrypted connection
    # (You may need to replace your connection details, username and password)
    app = App("bolt://localhost:7687", "neo4j", "password")
    app.create_friendship("Alice", "David")
    app.find_person("Alice")
    app.close()


# end::aura[]

# tag::hello-world-output[]
# hello, world, from node 1234
# end::hello-world-output[]


def test_hello_world_example(uri, auth):
    try:
        s = StringIO()
        with redirect_stdout(s):
            example = App(uri, auth[0], auth[1])
            example.create_friendship("hello, world")
            example.close()

        assert s.getvalue().startswith("hello, world, from node ")
    except ServiceUnavailable as error:
        if isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
