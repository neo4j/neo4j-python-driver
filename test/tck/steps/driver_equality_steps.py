from collections import deque

from behave import *

from neo4j.v1 import Path, Relationship
from test.tck.tck_util import send_string

use_step_matcher("re")


@step("`(?P<key>.+)` is single value result of: (?P<statement>.+)")
def step_impl(context, key, statement):
    runner = send_string(statement)
    records = list(runner.result)
    assert len(records) == 1
    assert len(records[0]) == 1
    context.values[key] = records[0][0]


@step("saved values should all equal")
def step_impl(context):
    values = list(context.values.values())
    assert len(values) > 1
    first_val = values.pop()
    for item in values:
        assert item == first_val


@step("none of the saved values should be equal")
def step_impl(context):
    values = list(context.values.values())
    assert len(values) > 1
    first_val = values.pop()
    for item in values:
        assert item != first_val
