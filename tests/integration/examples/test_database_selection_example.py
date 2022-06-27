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


from contextlib import redirect_stdout
from io import StringIO

from ...conftest import (
    mark_requires_edition,
    mark_requires_min_bolt_version,
)


# isort: off
# tag::database-selection-import[]
from neo4j import (
    GraphDatabase,
    READ_ACCESS,
)
# end::database-selection-import[]
# isort: on


# python -m pytest tests/integration/examples/test_database_selection_example.py -s -v

class DatabaseSelectionExample:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        with self.driver.session(database="system") as session:
            session.run("DROP DATABASE example IF EXISTS WAIT").consume()
            session.run("CREATE DATABASE example WAIT").consume()

    def close(self):
        with self.driver.session(database="system") as session:
            session.run("DROP DATABASE example WAIT").consume()
        self.driver.close()

    def run_example_code(self):

        driver = self.driver
        # tag::database-selection[]
        with driver.session(database="example") as session:
            session.run("CREATE (a:Greeting {message: 'Hello, Example-Database'}) RETURN a").consume()

        with driver.session(database="example", default_access_mode=READ_ACCESS) as session:
            message = session.run("MATCH (a:Greeting) RETURN a.message as msg").single().get("msg")
            print(message)
        # end::database-selection[]


@mark_requires_min_bolt_version("4")
@mark_requires_edition("enterprise")
def test_database_selection_example(uri, auth):
    s = StringIO()
    with redirect_stdout(s):
        example = DatabaseSelectionExample(uri, auth[0], auth[1])
        example.run_example_code()
        example.close()
    assert s.getvalue().startswith("Hello, Example-Database")
