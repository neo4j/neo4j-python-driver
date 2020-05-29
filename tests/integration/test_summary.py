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
    ResultSummary,
    SummaryCounters,
    GraphDatabase,
)
from neo4j.exceptions import (
    ServiceUnavailable,
)
from neo4j._exceptions import (
    BoltHandshakeError,
)


def test_can_obtain_summary_after_consuming_result(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_can_obtain_summary_after_consuming_result

    result = session.run("CREATE (n) RETURN n")
    summary = result.consume()
    assert summary.query == "CREATE (n) RETURN n"
    assert summary.parameters == {}
    assert summary.query_type == "rw"
    assert summary.counters.nodes_created == 1


def test_no_plan_info(session):
    result = session.run("CREATE (n) RETURN n")
    summary = result.consume()
    assert summary.plan is None
    assert summary.profile is None


def test_can_obtain_plan_info(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_can_obtain_plan_info
    result = session.run("EXPLAIN CREATE (n) RETURN n")
    summary = result.consume()
    assert isinstance(summary.plan, dict)


def test_can_obtain_profile_info(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_can_obtain_profile_info
    result = session.run("PROFILE CREATE (n) RETURN n")
    summary = result.consume()
    assert isinstance(summary.profile, dict)


def test_no_notification_info(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_no_notification_info
    result = session.run("CREATE (n) RETURN n")
    summary = result.consume()
    assert summary.notifications is None


def test_can_obtain_notification_info(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_can_obtain_notification_info
    result = session.run("EXPLAIN MATCH (n), (m) RETURN n, m")
    summary = result.consume()
    assert isinstance(summary, ResultSummary)

    notifications = summary.notifications
    assert isinstance(notifications, list)
    assert len(notifications) == 1
    assert isinstance(notifications[0], dict)


def test_contains_time_information(session):
    summary = session.run("UNWIND range(1,1000) AS n RETURN n AS number").consume()

    assert isinstance(summary.result_available_after, int)
    assert isinstance(summary.result_consumed_after, int)

    with pytest.raises(AttributeError) as ex:
        assert isinstance(summary.t_first, int)

    with pytest.raises(AttributeError) as ex:
        assert isinstance(summary.t_last, int)


def test_protocol_version_information(session):
    summary = session.run("UNWIND range(1,100) AS n RETURN n AS number").consume()

    assert isinstance(summary.server.protocol_version, tuple)
    assert isinstance(summary.server.protocol_version[0], int)
    assert isinstance(summary.server.protocol_version[1], int)


def test_summary_counters_case_1(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_summary_counters_case_1

    result = session.run("RETURN $number AS x", number=3)
    summary = result.consume()

    assert summary.query == "RETURN $number AS x"
    assert summary.parameters == {"number": 3}

    assert isinstance(summary.query_type, str)

    counters = summary.counters

    assert isinstance(counters, SummaryCounters)
    assert counters.nodes_created == 0
    assert counters.nodes_deleted == 0
    assert counters.relationships_created == 0
    assert counters.relationships_deleted == 0
    assert counters.properties_set == 0
    assert counters.labels_added == 0
    assert counters.labels_removed == 0
    assert counters.indexes_added == 0
    assert counters.indexes_removed == 0
    assert counters.constraints_added == 0
    assert counters.constraints_removed == 0
    assert counters.contains_updates is False

    assert counters.system_updates == 0
    assert counters.contains_system_updates is False


def test_summary_counters_case_2(neo4j_uri, auth, target):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_summary_counters_case_2
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:

            with driver.session(database="system") as session:
                session.run("DROP DATABASE test IF EXISTS").consume()

                # SHOW DATABASES

                result = session.run("SHOW DATABASES")
                databases = set()
                for record in result:
                    databases.add(record.get("name"))
                assert "system" in databases
                assert "neo4j" in databases

                summary = result.consume()

                assert summary.query == "SHOW DATABASES"
                assert summary.parameters == {}

                assert isinstance(summary.query_type, str)

                counters = summary.counters

                assert isinstance(counters, SummaryCounters)
                assert counters.nodes_created == 0
                assert counters.nodes_deleted == 0
                assert counters.relationships_created == 0
                assert counters.relationships_deleted == 0
                assert counters.properties_set == 0
                assert counters.labels_added == 0
                assert counters.labels_removed == 0
                assert counters.indexes_added == 0
                assert counters.indexes_removed == 0
                assert counters.constraints_added == 0
                assert counters.constraints_removed == 0
                assert counters.contains_updates is False

                assert counters.system_updates == 0
                assert counters.contains_system_updates is False

                # CREATE DATABASE test

                summary = session.run("CREATE DATABASE test").consume()

                assert summary.query == "CREATE DATABASE test"
                assert summary.parameters == {}

                assert isinstance(summary.query_type, str)

                counters = summary.counters

                assert isinstance(counters, SummaryCounters)
                assert counters.nodes_created == 0
                assert counters.nodes_deleted == 0
                assert counters.relationships_created == 0
                assert counters.relationships_deleted == 0
                assert counters.properties_set == 0
                assert counters.labels_added == 0
                assert counters.labels_removed == 0
                assert counters.indexes_added == 0
                assert counters.indexes_removed == 0
                assert counters.constraints_added == 0
                assert counters.constraints_removed == 0
                assert counters.contains_updates is False

                assert counters.system_updates == 1
                assert counters.contains_system_updates is True

            with driver.session(database="test") as session:
                summary = session.run("CREATE (n)").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.nodes_created == 1

            with driver.session(database="test") as session:
                summary = session.run("MATCH (n) DELETE (n)").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.nodes_deleted == 1

            with driver.session(database="test") as session:
                summary = session.run("CREATE ()-[:KNOWS]->()").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.relationships_created == 1

            with driver.session(database="test") as session:
                summary = session.run("MATCH ()-[r:KNOWS]->() DELETE r").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.relationships_deleted == 1

            with driver.session(database="test") as session:
                summary = session.run("CREATE (n:ALabel)").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.labels_added == 1

            with driver.session(database="test") as session:
                summary = session.run("MATCH (n:ALabel) REMOVE n:ALabel").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.labels_removed == 1

            with driver.session(database="test") as session:
                summary = session.run("CREATE (n {magic: 42})").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.properties_set == 1

            with driver.session(database="test") as session:
                summary = session.run("CREATE INDEX ON :ALabel(prop)").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.indexes_added == 1

            with driver.session(database="test") as session:
                summary = session.run("DROP INDEX ON :ALabel(prop)").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.indexes_removed == 1

            with driver.session(database="test") as session:
                summary = session.run("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.constraints_added == 1

            with driver.session(database="test") as session:
                summary = session.run("DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE").consume()
                assert summary.counters.contains_updates is True
                assert summary.counters.contains_system_updates is False
                assert summary.counters.constraints_removed == 1

            with driver.session(database="system") as session:
                session.run("DROP DATABASE test IF EXISTS").consume()
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            # This is because a single instance Neo4j 3.5 does not have dbms.routing.cluster.getRoutingTable() call
            pytest.skip(error.args[0])
        elif isinstance(error.__cause__, BoltHandshakeError):
            pytest.skip(error.args[0])
