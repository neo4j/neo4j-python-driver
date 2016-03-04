from collections import deque

from behave import *

from neo4j.v1 import Path, Relationship
from test.tck.tck_util import send_string

use_step_matcher("re")


@step("`(?P<key>.+)` is single value result of: (?P<statement>.+)")
def step_impl(context, key, statement):
    records = list(send_string(statement).stream())
    assert len(records) == 1
    assert len(records[0]) == 1
    context.values[key] = records[0][0]


@step("`(?P<key1>.+)` is a copy of `(?P<key2>.+)` path with flipped relationship direction")
def step_impl(context, key1, key2):
    path = context.values[key2]
    nodes = path.nodes
    new_relationships = []
    for r in path.relationships:
        start = r.end
        end = r.start
        tmp_r = Relationship(start, end, r.type, r.properties)
        tmp_r.identity = r.identity
        new_relationships.append(tmp_r)
    entities = [nodes[0]]
    for i in range(1,len(nodes)):
        entities.append(new_relationships[i-1])
        entities.append(nodes[i])

    context.values[key1] = Path(*entities)


@step("saved values should all equal")
def step_impl(context):
    values = context.values.values()
    assert len(values) > 1
    first_val = values.pop()
    for item in values:
        assert item == first_val


@step("none of the saved values should be equal")
def step_impl(context):
    values = context.values.values()
    assert len(values) > 1
    first_val = values.pop()
    for item in values:
        assert item != first_val
