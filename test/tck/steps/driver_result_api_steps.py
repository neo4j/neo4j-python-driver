from behave import *

from neo4j.v1 import ResultSummary, STATEMENT_TYPE_READ_ONLY, STATEMENT_TYPE_READ_WRITE, STATEMENT_TYPE_WRITE_ONLY, \
    STATEMENT_TYPE_SCHEMA_WRITE

from test.tck.resultparser import parse_values

use_step_matcher("re")


@step("the `Result Cursor` is summarized")
def step_impl(context):
    context.summaries = []
    for rc in context.rcs:
        context.summaries.append(rc.summarize())


@then("the `Result Cursor` is fully consumed")
def step_impl(context):
    for rc in context.rcs:
        assert rc.at_end()
        assert rc.record() is None


@then("a `Result Summary` is returned")
def step_impl(context):
    for summary in context.summaries:
        assert isinstance(summary, ResultSummary)


@step("I request a `statement` from the `Result Summary`")
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


@step("requesting `update statistics` from it should give")
def step_impl(context):
    for summary in context.summaries:
        for row in context.table:
            assert getattr(summary.statistics, row[0].replace(" ","_")) == parse_values(row[1])


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


@step("the summary has a `plan`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.plan is not None


@step("the summary has a `profile`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.profile is not None


@step("the summary does not have a `plan`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.plan is None


@step("the summary does not have a `profile`")
def step_impl(context):
    for summary in context.summaries:
        assert summary.profile is None


@step("requesting the `(?P<plan_type>.+)` it contains")
def step_impl(context, plan_type):
    for summary in context.summaries:
        if plan_type == "plan":
            plan = summary.plan
        elif plan_type == "profile":
            plan = summary.profile
        else:
            raise ValueError("Expected 'plan' or 'profile'. Got: %s" % plan_type)
        for row in context.table:
            assert getattr(plan, row[0].replace(" ", "_")) == parse_values(row[1])


@step("the `(?P<plan_type>.+)` also contains method calls for")
def step_impl(context, plan_type):
    for summary in context.summaries:
        if plan_type == "plan":
            plan = summary.plan
        elif plan_type == "profile":
            plan = summary.profile
        else:
            raise ValueError("Expected 'plan' or 'profile'. Got: %s" % plan_type)
        for row in context.table:
            assert getattr(plan, row[0].replace(" ", "_")) is not None


@step("the summaries `notifications` is empty list")
def step_impl(context):
    for summary in context.summaries:
        assert len(summary.notifications) == 0


@step("the summaries `notifications` has one notification with")
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



