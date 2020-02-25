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


def test_can_obtain_summary_after_consuming_result(session):
    # python -m pytest tests/integration/test_summary.py -s -v -k test_can_obtain_summary_after_consuming_result

    result = session.run("CREATE (n) RETURN n")
    summary = result.summary()
    assert summary.query == "CREATE (n) RETURN n"
    assert summary.parameters == {}
    assert summary.query_type == "rw"
    assert summary.counters.nodes_created == 1


def test_no_plan_info(session):
    result = session.run("CREATE (n) RETURN n")
    summary = result.summary()
    assert summary.plan is None
    assert summary.profile is None


def test_can_obtain_plan_info(session):
    result = session.run("EXPLAIN CREATE (n) RETURN n")
    summary = result.summary()
    plan = summary.plan
    assert plan.operator_type == "ProduceResults"
    assert plan.identifiers == ["n"]
    assert len(plan.children) == 1


def test_can_obtain_profile_info(session):
    result = session.run("PROFILE CREATE (n) RETURN n")
    summary = result.summary()
    profile = summary.profile
    assert profile.db_hits == 0
    assert profile.rows == 1
    assert profile.operator_type == "ProduceResults"
    assert profile.identifiers == ["n"]
    assert len(profile.children) == 1


def test_no_notification_info(session):
    result = session.run("CREATE (n) RETURN n")
    summary = result.summary()
    notifications = summary.notifications
    assert notifications == []


def test_can_obtain_notification_info(session):
    result = session.run("EXPLAIN MATCH (n), (m) RETURN n, m")
    summary = result.summary()
    notifications = summary.notifications

    assert len(notifications) == 1
    notification = notifications[0]
    assert notification.code == "Neo.ClientNotification.Statement.CartesianProductWarning"
    assert notification.title == "This query builds a cartesian product between " \
                                 "disconnected patterns."
    assert notification.severity == "WARNING"
    assert notification.description == "If a part of a query contains multiple " \
                                       "disconnected patterns, this will build a " \
                                       "cartesian product between all those parts. This " \
                                       "may produce a large amount of data and slow down " \
                                       "query processing. While occasionally intended, " \
                                       "it may often be possible to reformulate the " \
                                       "query that avoids the use of this cross product, " \
                                       "perhaps by adding a relationship between the " \
                                       "different parts or by using OPTIONAL MATCH " \
                                       "(identifier is: (m))"
    position = notification.position
    assert position


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

    assert isinstance(summary.protocol_version, tuple)
    assert isinstance(summary.protocol_version[0], int)
    assert isinstance(summary.protocol_version[1], int)


