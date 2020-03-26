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

from neo4j import GraphDatabase
from neo4j.exceptions import (
    ServiceUnavailable,
)
from neo4j._exceptions import (
    BoltHandshakeError,
)

# python -m pytest tests/integration/test_neo4j_driver.py -s -v


def test_neo4j_uri(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_uri
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
            with driver.session() as session:
                value = session.run("RETURN 1").single().value()
                assert value == 1
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
