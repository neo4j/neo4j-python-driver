#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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

from neo4j import (
    GraphDatabase,
    Neo4jDriver,
    DEFAULT_DATABASE,
)
from tests.stub.conftest import StubCluster

# python -m pytest tests/stub/test_multi_database.py -s -v


@pytest.mark.parametrize(
    "test_script, test_database",
    [
        ("v3/dbms_cluster_routing_get_routing_table_system.script", DEFAULT_DATABASE),
        ("v4x0/dbms_routing_get_routing_table_system_default.script", DEFAULT_DATABASE),
        ("v4x0/dbms_routing_get_routing_table_system_neo4j.script", "neo4j"),
    ]
)
def test_dbms_cluster_routing_get_routing_table(driver_info, test_script, test_database):
    # python -m pytest tests/stub/test_multi_database.py -s -v -k test_dbms_cluster_routing_get_routing_table

    test_config = {
        "user_agent": "test",
        "database": test_database,
    }

    with StubCluster(test_script):
        uri = "neo4j://localhost:9001"
        driver = GraphDatabase.driver(uri, auth=driver_info["auth_token"], **test_config)
        assert isinstance(driver, Neo4jDriver)
        driver.close()
