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

from . import DriverSetupExample


# isort: off
# tag::basic-auth-import[]
from neo4j import GraphDatabase
# end::basic-auth-import[]
# isort: on


# python -m pytest tests/integration/examples/test_basic_auth_example.py -s -v


class BasicAuthExample(DriverSetupExample):
    # tag::basic-auth[]
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    # end::basic-auth[]


def test_example(uri, auth):
    try:
        BasicAuthExample.test(uri, user=auth[0], password=auth[1])
    except ServiceUnavailable as error:
        if isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
