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

from neo4j import (
    GraphDatabase,
    Neo4jDriver,
    Version,
)
from neo4j.exceptions import (
    ServiceUnavailable,
    ConfigurationError,
    ClientError,
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


def test_supports_multi_db(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_supports_multi_db
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=auth)
        assert isinstance(driver, Neo4jDriver)
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])

    with driver.session() as session:
        result = session.run("RETURN 1")
        value = result.single().value()   # Consumes the result
        summary = result.summary()
        server_info = summary.server

    result = driver.supports_multi_db()
    driver.close()

    if server_info.version_info() >= Version(4, 0, 0) and server_info.protocol_version >= Version(4, 0):
        assert result is True
        assert summary.database == "neo4j"  # This is the default database name if not set explicitly on the Neo4j Server
        assert summary.query_type == "r"
    else:
        assert result is False
        assert summary.database is None
        assert summary.query_type == "r"


def test_test_multi_db_specify_database(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_test_multi_db_specify_database
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth, database="test_database") as driver:
            with driver.session() as session:
                result = session.run("RETURN 1")
                assert next(result) == 1
                summary = result.summary()
                assert summary.database == "test_database"
    except ServiceUnavailable as error:
        if isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
    except ConfigurationError as error:
        assert "Database name parameter for selecting database is not supported in Bolt Protocol Version(3, 0)." in error.args[0]
    except ClientError as error:
        # FAILURE {'code': 'Neo.ClientError.Database.DatabaseNotFound' - This message is sent from the server
        assert error.args[0] == "Unable to get a routing table for database 'test_database' because this database does not exist"
