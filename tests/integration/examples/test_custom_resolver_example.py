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
# tag::custom-resolver-import[]
from neo4j import (
    GraphDatabase,
    WRITE_ACCESS,
)
# end::custom-resolver-import[]
# isort: on


# tag::custom-resolver[]
def create_driver(uri, user, password):
    def resolver(address):
        host, port = address
        if host == "x.example.com":
            yield "a.example.com", port
            yield "b.example.com", port
            yield "c.example.com", port
        else:
            yield host, port

    return GraphDatabase.driver(uri, auth=(user, password), resolver=resolver)


def add_person(name):
    driver = create_driver(
        "neo4j://x.example.com", user="neo4j", password="password"
    )
    try:
        with driver.session(default_access_mode=WRITE_ACCESS) as session:
            session.run("CREATE (a:Person {name: $name})", {"name", name})
    finally:
        driver.close()


# end::custom-resolver[]


def test_example(uri, auth):
    try:
        add_person("testing_resolver")
    except ServiceUnavailable as error:
        if isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
    except ValueError as error:
        if error.args[0] != "Cannot resolve address a.example.com:7687":
            raise
