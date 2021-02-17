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
        _ = result.single().value()   # Consumes the result
        summary = result.consume()
        server_info = summary.server

    assert isinstance(summary, ResultSummary)
    assert isinstance(server_info, ServerInfo)
    assert server_info.version_info() is not None
    assert isinstance(server_info.protocol_version, Version)

    result = driver.supports_multi_db()
    driver.close()

    if server_info.protocol_version == Bolt3.PROTOCOL_VERSION:
        assert result is False
        assert summary.database is None
        assert summary.query_type == "r"
    else:
        assert result is True
        assert server_info.version_info() >= Version(4, 0)
        assert server_info.protocol_version >= Version(4, 0)
        assert summary.database == "neo4j"  # This is the default database name if not set explicitly on the Neo4j Server
        assert summary.query_type == "r"


def test_test_multi_db_specify_database(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_test_multi_db_specify_database
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth, database="test_database") as driver:
            with driver.session() as session:
                result = session.run("RETURN 1")
                assert next(result) == 1
                summary = result.consume()
                assert summary.database == "test_database"
    except ServiceUnavailable as error:
        if isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
    except ConfigurationError as error:
        assert "Database name parameter for selecting database is not supported in Bolt Protocol Version(3, 0)." in error.args[0]
    except ClientError as error:
        # FAILURE {'code': 'Neo.ClientError.Database.DatabaseNotFound' - This message is sent from the server
        assert error.args[0] == "Unable to get a routing table for database 'test_database' because this database does not exist"


def test_neo4j_multi_database_support_create(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_support_create
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
            with driver.session(database="system") as session:
                session.run("DROP DATABASE test IF EXISTS").consume()
                result = session.run("SHOW DATABASES")
                databases = set()
                for record in result:
                    databases.add(record.get("name"))
                assert "system" in databases
                assert "neo4j" in databases

                session.run("CREATE DATABASE test").consume()
                result = session.run("SHOW DATABASES")
                for record in result:
                    databases.add(record.get("name"))
                assert "system" in databases
                assert "neo4j" in databases
                assert "test" in databases
            with driver.session(database="system") as session:
                session.run("DROP DATABASE test IF EXISTS").consume()
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])


def test_neo4j_multi_database_support_different(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_support_different
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
            with driver.session() as session:
                # Test that default database is empty
                session.run("MATCH (n) DETACH DELETE n").consume()
                result = session.run("MATCH (p:Person) RETURN p")
                names = set()
                for ix in result:
                    names.add(ix["p"].get("name"))
                assert names == set()  # THIS FAILS?
            with driver.session(database="system") as session:
                session.run("DROP DATABASE testa IF EXISTS").consume()
                session.run("DROP DATABASE testb IF EXISTS").consume()
            with driver.session(database="system") as session:
                result = session.run("SHOW DATABASES")
                databases = set()
                for record in result:
                    databases.add(record.get("name"))
                assert databases == {"system", "neo4j"}
                result = session.run("CREATE DATABASE testa")
                result.consume()
                result = session.run("CREATE DATABASE testb")
                result.consume()
            with driver.session(database="testa") as session:
                result = session.run('CREATE (p:Person {name: "ALICE"})')
                result.consume()
            with driver.session(database="testb") as session:
                result = session.run('CREATE (p:Person {name: "BOB"})')
                result.consume()
            with driver.session() as session:
                # Test that default database is still empty
                result = session.run("MATCH (p:Person) RETURN p")
                names = set()
                for ix in result:
                    names.add(ix["p"].get("name"))
                assert names == set()  # THIS FAILS?
            with driver.session(database="testa") as session:
                result = session.run("MATCH (p:Person) RETURN p")
                names = set()
                for ix in result:
                    names.add(ix["p"].get("name"))
                assert names == {"ALICE", }
            with driver.session(database="testb") as session:
                result = session.run("MATCH (p:Person) RETURN p")
                names = set()
                for ix in result:
                    names.add(ix["p"].get("name"))
                assert names == {"BOB", }
            with driver.session(database="system") as session:
                session.run("DROP DATABASE testa IF EXISTS").consume()
            with driver.session(database="system") as session:
                session.run("DROP DATABASE testb IF EXISTS").consume()
            with driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n").consume()
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])


def test_neo4j_multi_database_test_routing_table_creates_new_if_deleted(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_test_routing_table_creates_new_if_deleted
    try:
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
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])


def test_neo4j_multi_database_test_routing_table_updates_if_stale(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_test_routing_table_updates_if_stale
    try:
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
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])


def test_neo4j_multi_database_test_routing_table_removes_aged(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_neo4j_driver.py -s -v -k test_neo4j_multi_database_test_routing_table_removes_aged
    try:
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
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])


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
