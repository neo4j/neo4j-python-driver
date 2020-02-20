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

# tag::custom-resolver-import[]
from neo4j import GraphDatabase
# end::custom-resolver-import[]

from neo4j._exceptions import BoltHandshakeError
from tests.integration.examples import DriverSetupExample


# python -m pytest tests/integration/examples/test_custom_resolver_example.py -s -v


class CustomResolverExample(DriverSetupExample):

    # tag::custom-resolver[]
    def __init__(self, uri, auth):

        def resolve(address):
            host, port = address
            if host == "x.example.com":
                yield "a.example.com", port
                yield "b.example.com", port
                yield "c.example.com", port
            else:
                yield host, port

        self.driver = GraphDatabase.driver(uri, auth=auth, resolver=resolve)
    # end::custom-resolver[]


def test_example(uri, auth):
    try:
        CustomResolverExample.test(uri, auth)
    except BoltHandshakeError as error:
        pytest.skip(error.args[0])
