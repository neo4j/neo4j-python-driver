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


import neo4j
from tests.integration.examples import DriverSetupExample


# isort: off
# tag::bearer-auth-import[]
from neo4j import (
    bearer_auth,
    GraphDatabase,
)
# end::bearer-auth-import[]
# isort: on


# python -m pytest tests/integration/examples/test_bearer_auth_example.py -s -v

class BearerAuthExample(DriverSetupExample):

    # tag::bearer-auth[]
    def __init__(self, uri, token):
        self.driver = GraphDatabase.driver(uri, auth=bearer_auth(token))
    # end::bearer-auth[]


def test_example(uri, mocker):
    # Currently, there is no way of running the test against a server with SSO
    # setup.
    mocker.patch("neo4j.GraphDatabase.bolt_driver")
    mocker.patch("neo4j.GraphDatabase.neo4j_driver")

    token = "myToken"
    BearerAuthExample(uri, token)
    calls = (neo4j.GraphDatabase.bolt_driver.call_args_list
             + neo4j.GraphDatabase.neo4j_driver.call_args_list)
    assert len(calls) == 1
    args_, kwargs = calls[0]
    auth = kwargs.get("auth")
    assert isinstance(auth, neo4j.Auth)
    assert auth.scheme == "bearer"
    assert not hasattr(auth, "principal")
    assert auth.credentials == token
    assert not hasattr(auth, "parameters")
