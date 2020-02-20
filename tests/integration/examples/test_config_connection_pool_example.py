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

from neo4j._exceptions import BoltHandshakeError

# tag::config-connection-pool-import[]
from neo4j import GraphDatabase
# end::config-connection-pool-import[]

from tests.integration.examples import DriverSetupExample


# python -m pytest tests/integration/examples/test_config_connection_pool_example.py -s -v

class ConfigConnectionPoolExample(DriverSetupExample):

    # tag::config-connection-pool[]
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth,
                                           max_age=30 * 60,
                                           max_size=50,
                                           acquire_timeout=2 * 60)
    # end::config-connection-pool[]


def test(uri, auth):
    try:
        ConfigConnectionPoolExample.test(uri, auth)
    except BoltHandshakeError as error:
        pytest.skip(error.args[0])
