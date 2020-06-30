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

from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

from neo4j._exceptions import BoltHandshakeError


# python -m pytest tests/integration/examples/test_aura_example.py -s -v

class App:

    def __init__(self, uri, user, password):
        # Aura queries use an encrypted connection
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=True)

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(self._create_and_return_friendship,
                                               person1_name, person2_name)
            for row in result:
                print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = """
        CREATE (p1:Person { name: $person1_name })
        CREATE (p2:Person { name: $person2_name })
        CREATE (p1)-[:KNOWS]->(p2)
        RETURN p1, p2
        """
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

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
    # Aura uses the "bolt+routing" protocol
    bolt_url = "%%BOLT_URL_PLACEHOLDER%%"
    user = "<Username for Neo4j Aura database>"
    password = "<Password for Neo4j Aura database>"
    app = App(bolt_url, user, password)
    app.create_friendship("Alice", "David")
    app.find_person("Alice")
    app.close()


def test_aura_example(uri, auth):
    try:
        s = StringIO()
        with redirect_stdout(s):
            app = App(uri, auth[0], auth[1])
            app.create_friendship("Alice", "David")
            app.find_person("Alice")
            app.close()

        assert s.getvalue().startswith("Found person: Alice")
    except ServiceUnavailable as error:
        if isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
