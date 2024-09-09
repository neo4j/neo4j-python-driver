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


import pytest

from neo4j._exceptions import BoltHandshakeError
from neo4j.exceptions import ServiceUnavailable


# isort: off
# tag::pass-bookmarks-import[]
from neo4j import (
    Bookmarks,
    GraphDatabase,
)
# end::pass-bookmarks-import[]
# isort: on


# tag::pass-bookmarks[]
class BookmarksExample:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    # Create a person node.
    @classmethod
    def create_person(cls, tx, name):
        tx.run("CREATE (:Person {name: $name})", name=name)

    # Create an employment relationship to a pre-existing company node.
    # This relies on the person first having been created.
    @classmethod
    def employ(cls, tx, person_name, company_name):
        tx.run(
            "MATCH (person:Person {name: $person_name}) "
            "MATCH (company:Company {name: $company_name}) "
            "CREATE (person)-[:WORKS_FOR]->(company)",
            person_name=person_name,
            company_name=company_name,
        )

    # Create a friendship between two people.
    @classmethod
    def create_friendship(cls, tx, name_a, name_b):
        tx.run(
            "MATCH (a:Person {name: $name_a}) "
            "MATCH (b:Person {name: $name_b}) "
            "MERGE (a)-[:KNOWS]->(b)",
            name_a=name_a,
            name_b=name_b,
        )

    # Match and display all friendships.
    @classmethod
    def print_friendships(cls, tx):
        result = tx.run("MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name")
        for record in result:
            print("{} knows {}".format(record["a.name"], record["b.name"]))

    def main(self):
        saved_bookmarks = Bookmarks()  # To collect the session bookmarks

        # Create the first person and employment relationship.
        with self.driver.session() as session_a:
            session_a.execute_write(self.create_person, "Alice")
            session_a.execute_write(self.employ, "Alice", "Wayne Enterprises")
            saved_bookmarks += session_a.last_bookmarks()

        # Create the second person and employment relationship.
        with self.driver.session() as session_b:
            session_b.execute_write(self.create_person, "Bob")
            session_b.execute_write(self.employ, "Bob", "LexCorp")
            saved_bookmarks += session_a.last_bookmarks()

        # Create a friendship between the two people created above.
        with self.driver.session(bookmarks=saved_bookmarks) as session_c:
            session_c.execute_write(self.create_friendship, "Alice", "Bob")
            session_c.execute_read(self.print_friendships)


# end::pass-bookmarks[]


def test(uri, auth):
    eg = BookmarksExample(uri, auth)
    try:
        with eg.driver.session() as session:
            session.run("MATCH (_) DETACH DELETE _")
        eg.main()
        with eg.driver.session() as session:
            session.run("MATCH (_) DETACH DELETE _")
    except ServiceUnavailable as error:
        if isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
    finally:
        eg.close()
