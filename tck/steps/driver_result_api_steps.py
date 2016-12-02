#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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

from behave import *

from neo4j.v1.summary import STATEMENT_TYPE_READ_ONLY, STATEMENT_TYPE_READ_WRITE, \
                             STATEMENT_TYPE_WRITE_ONLY, STATEMENT_TYPE_SCHEMA_WRITE
from tck.resultparser import parse_values

use_step_matcher("re")


@step("the `Statement Result` is consumed a `Result Summary` is returned")
def step_impl(context):
    context.summaries = [x.consume() for x in context.results]
    assert context.summaries[0] is not None


@then("the `Statement Result` is closed")
def step_impl(context):
    for result in context.results:
        assert result.connection is None


@step("I request a `Statement` from the `Result Summary`")
def step_impl(context):
    context.statements = []
    for summary in context.summaries:
        context.statements.append(summary.statement)


@then("requesting the `Statement` as text should give: (?P<expected>.+)")
def step_impl(context, expected):
    for statement in context.statements:
        assert statement == expected


@step("requesting the `Statement` parameter should give: (?P<expected>.+)")
def step_impl(context, expected):
    for summary in context.summaries:
        assert summary.parameters == parse_values(expected)


@step("requesting `Counters` from `Result Summary` should give")
def step_impl(context):
    for summary in context.summaries:
        for row in context.table:
            assert getattr(summary.counters, row[0].replace(" ","_")) == parse_values(row[1])


@step("requesting the `Statement Type` should give (?P<expected>.+)")
def step_impl(context, expected):
    for summary in context.summaries:
        if expected == "read only":
            statement_type = STATEMENT_TYPE_READ_ONLY
        elif expected == "read write":
            statement_type = STATEMENT_TYPE_READ_WRITE
        elif expected == "write only":
            statement_type = STATEMENT_TYPE_WRITE_ONLY
        elif expected == "schema write":
            statement_type = STATEMENT_TYPE_SCHEMA_WRITE
        else:
            raise ValueError("Not recognisable statement type: %s" % expected)
        assert summary.statement_type == statement_type


@step("the `Result Summary` has a `Plan`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.plan is not None


@step("the `Result Summary` has a `Profile`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.profile is not None


@step("the `Result Summary` does not have a `Plan`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.plan is None


@step("the `Result Summary` does not have a `Profile`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.profile is None


@step("requesting the `(?P<plan_type>.+)` it contains")
def step_impl(context, plan_type):
    for summary in context.summaries:
        if plan_type == "Plan":
            plan = summary.plan
        elif plan_type == "Profile":
            plan = summary.profile
        else:
            raise ValueError("Expected 'plan' or 'profile'. Got: %s" % plan_type)
        for row in context.table:
            attr = row[0].replace(" ", "_")
            if attr == 'records':
                attr = 'rows'
            assert getattr(plan, attr) == parse_values(row[1])


@step("the `(?P<plan_type>.+)` also contains method calls for")
def step_impl(context, plan_type):
    for summary in context.summaries:
        if plan_type == "Plan":
            plan = summary.plan
        elif plan_type == "Profile":
            plan = summary.profile
        else:
            raise ValueError("Expected 'plan' or 'profile'. Got: %s" % plan_type)
        for row in context.table:
            assert getattr(plan, row[0].replace(" ", "_")) is not None


@step("the `Result Summary` `Notifications` is empty")
def step_impl(context):
    for summary in context.summaries:
        assert len(summary.notifications) == 0


@step("the `Result Summary` `Notifications` has one notification with")
def step_impl(context):

    for summary in context.summaries:
        assert len(summary.notifications) == 1
        notification = summary.notifications[0]
        for row in context.table:
            if row[0] == 'position':
                position = getattr(notification, row[0].replace(" ","_"))
                expected_position = parse_values(row[1])
                for position_key, value in expected_position.items():
                    assert value == getattr(position, position_key.replace(" ", "_"))
            else:
                assert getattr(notification, row[0].replace(" ","_")) == parse_values(row[1])



