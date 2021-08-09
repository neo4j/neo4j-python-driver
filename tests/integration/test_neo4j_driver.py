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
    Version,
    READ_ACCESS,
    ResultSummary,
    ServerInfo,
)
from neo4j.exceptions import (
    ServiceUnavailable,
    ConfigurationError,
    ClientError,
)
from neo4j._exceptions import (
    BoltHandshakeError,
)
from neo4j.conf import (
    RoutingConfig,
)
from neo4j.io._bolt3 import Bolt3

# python -m pytest tests/integration/test_neo4j_driver.py -s -v


def test_neo4j_multi_database_test_routing_table_creates_new_if_deleted(neo4j_uri, auth, target, requires_bolt_4x):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_test_routing_table_creates_new_if_deleted
    with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
        with driver.session(database="system") as session:
            result = session.run("DROP DATABASE test IF EXISTS")
            result.consume()
            result = session.run("SHOW DATABASES")
            databases = set()
            for record in result:
                databases.add(record.get("name"))
            assert databases == {"system", "neo4j"}

            result = session.run("CREATE DATABASE test")
            result.consume()
            result = session.run("SHOW DATABASES")
            for record in result:
                databases.add(record.get("name"))
            assert databases == {"system", "neo4j", "test"}
        with driver.session(database="test") as session:
            result = session.run("RETURN 1 AS x")
            result.consume()
        del driver._pool.routing_tables["test"]
        with driver.session(database="test") as session:
            result = session.run("RETURN 1 AS x")
            result.consume()
        with driver.session(database="system") as session:
            result = session.run("DROP DATABASE test IF EXISTS")
            result.consume()


def test_neo4j_multi_database_test_routing_table_updates_if_stale(neo4j_uri, auth, target, requires_bolt_4x):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_test_routing_table_updates_if_stale
    with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
        with driver.session(database="system") as session:
            result = session.run("DROP DATABASE test IF EXISTS")
            result.consume()
            result = session.run("SHOW DATABASES")
            databases = set()
            for record in result:
                databases.add(record.get("name"))
            assert databases == {"system", "neo4j"}

            result = session.run("CREATE DATABASE test")
            result.consume()
            result = session.run("SHOW DATABASES")
            for record in result:
                databases.add(record.get("name"))
            assert databases == {"system", "neo4j", "test"}
        with driver.session(database="test") as session:
            result = session.run("RETURN 1 AS x")
            result.consume()
        driver._pool.routing_tables["test"].ttl = 0
        old_value = driver._pool.routing_tables["test"].last_updated_time
        with driver.session(database="test") as session:
            result = session.run("RETURN 1 AS x")
            result.consume()
        with driver.session(database="system") as session:
            result = session.run("DROP DATABASE test IF EXISTS")
            result.consume()
        assert driver._pool.routing_tables["test"].last_updated_time > old_value


def test_neo4j_multi_database_test_routing_table_removes_aged(neo4j_uri, auth, target, requires_bolt_4x):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_test_routing_table_removes_aged
    with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
        with driver.session(database="system") as session:
            result = session.run("DROP DATABASE testa IF EXISTS")
            result.consume()
            result = session.run("DROP DATABASE testb IF EXISTS")
            result.consume()
            result = session.run("SHOW DATABASES")
            databases = set()
            for record in result:
                databases.add(record.get("name"))
            assert databases == {"system", "neo4j"}

            result = session.run("CREATE DATABASE testa")
            result.consume()
            result = session.run("CREATE DATABASE testb")
            result.consume()
            result = session.run("SHOW DATABASES")
            for record in result:
                databases.add(record.get("name"))
            assert databases == {"system", "neo4j", "testa", "testb"}
        with driver.session(database="testa") as session:
            result = session.run("RETURN 1 AS x")
            result.consume()
        with driver.session(database="testb") as session:
            result = session.run("RETURN 1 AS x")
            result.consume()
        driver._pool.routing_tables["testa"].ttl = 0
        driver._pool.routing_tables["testb"].ttl = -1 * RoutingConfig.routing_table_purge_delay
        old_value = driver._pool.routing_tables["testa"].last_updated_time
        with driver.session(database="testa") as session:
            # This will refresh the routing table for "testa" and the refresh will trigger a cleanup of aged routing tables
            result = session.run("RETURN 1 AS x")
            result.consume()
        with driver.session(database="system") as session:
            result = session.run("DROP DATABASE testa IF EXISTS")
            result.consume()
            result = session.run("DROP DATABASE testb IF EXISTS")
            result.consume()
        assert driver._pool.routing_tables["testa"].last_updated_time > old_value
        assert "testb" not in driver._pool.routing_tables


def test_neo4j_driver_fetch_size_config_autocommit_normal_case(neo4j_uri, auth):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_driver_fetch_size_config_autocommit_normal_case
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth, user_agent="test") as driver:
            assert isinstance(driver, Neo4jDriver)
            with driver.session(fetch_size=2, default_access_mode=READ_ACCESS) as session:
                expected = []
                result = session.run("UNWIND [1,2,3,4] AS x RETURN x")
                for record in result:
                    expected.append(record["x"])

        assert expected == [1, 2, 3, 4]
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])


def test_neo4j_driver_fetch_size_config_autocommit_consume_case(neo4j_uri, auth):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_driver_fetch_size_config_autocommit_consume_case
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth, user_agent="test") as driver:
            assert isinstance(driver, Neo4jDriver)
            with driver.session(fetch_size=2, default_access_mode=READ_ACCESS) as session:
                result = session.run("UNWIND [1,2,3,4] AS x RETURN x")
                result_summary_consume = result.consume()

        assert isinstance(result_summary_consume, ResultSummary)
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])


def test_neo4j_driver_fetch_size_config_explicit_transaction(neo4j_uri, auth):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_driver_fetch_size_config_explicit_transaction
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth, user_agent="test") as driver:
            assert isinstance(driver, Neo4jDriver)
            with driver.session(fetch_size=2, default_access_mode=READ_ACCESS) as session:
                expected = []
                tx = session.begin_transaction()
                result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                for record in result:
                    expected.append(record["x"])
                tx.commit()

        assert expected == [1, 2, 3, 4]
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
