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
    Plan,
    ProfiledPlan,
    Notification,
    Position,
)


def get_operator_type(op):
    # Fabric will suffix with db name, remove this to handle fabric on/off
    op = op.split("@")
    return op[0]


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
    result = session.run("EXPLAIN CREATE (n) RETURN n")
    summary = result.consume()
    plan = summary.plan
    assert get_operator_type(plan.operator_type) == "ProduceResults"
    assert plan.identifiers == ["n"]
    assert len(plan.children) == 1


def test_can_obtain_profile_info(session):
    result = session.run("PROFILE CREATE (n) RETURN n")
    summary = result.consume()
    profile = summary.profile
    assert profile.db_hits == 0
    assert profile.rows == 1
    assert get_operator_type(profile.operator_type) == "ProduceResults"
    assert profile.identifiers == ["n"]
    assert len(profile.children) == 1


def test_no_notification_info(session):
    result = session.run("CREATE (n) RETURN n")
    summary = result.consume()
    notifications = summary.notifications
    assert notifications == []


def test_can_obtain_notification_info(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_can_obtain_notification_info
    result = session.run("EXPLAIN MATCH (n), (m) RETURN n, m")
    summary = result.consume()
    assert isinstance(summary, ResultSummary)

    notifications = summary.notifications
    assert isinstance(notifications, list)
    assert len(notifications) == 1

    notification = notifications[0]
    assert isinstance(notification, Notification)
    assert notification.code.startswith("Neo.ClientNotification")           # "Neo.ClientNotification.Statement.CartesianProductWarning"
    assert isinstance(notification.title, str)                              # "This query builds a cartesian product between disconnected patterns."
    assert isinstance(notification.severity, str)                           # "WARNING"
    assert isinstance(notification.description, str)
    assert isinstance(notification.position, Position)


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


def test_summary_counters(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_summary_counters

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
