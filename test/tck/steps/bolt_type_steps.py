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
import json

from neo4j.v1 import GraphDatabase
from test.tck import tck_util

use_step_matcher("re")


@given("A running database")
def step_impl(context):
    tck_util.send_string("RETURN 1")


@given("a value (?P<input>.+) of type (?P<bolt_type>.+)")
def step_impl(context, input, bolt_type):
    context.expected = tck_util.get_bolt_value(bolt_type, input)


@given("a value  of type (?P<bolt_type>.+)")
def step_impl(context, bolt_type):
    context.expected = tck_util.get_bolt_value(bolt_type, u' ')


@given("a list value (?P<input>.+) of type (?P<bolt_type>.+)")
def step_impl(context, input, bolt_type):
    context.expected = tck_util.get_list_from_feature_file(input, bolt_type)


@given("an empty list L")
def step_impl(context):
    context.L = []


@given("an empty map M")
def step_impl(context):
    context.M = {}


@given("a String of size (?P<size>\d+)")
def step_impl(context, size):
    context.expected = tck_util.get_random_string(int(size))


@given("a List of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = tck_util.get_list_of_random_type(int(size), type)


@given("a Map of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = tck_util.get_dict_of_random_type(int(size), type)


@step("adding a table of lists to the list L")
def step_impl(context):
    for row in context.table:
        context.L.append(tck_util.get_list_from_feature_file(row[1], row[0]))


@step("adding a table of values to the list L")
def step_impl(context):
    for row in context.table:
        context.L.append(tck_util.get_bolt_value(row[0], row[1]))


@step("adding a table of values to the map M")
def step_impl(context):
    for row in context.table:
        context.M['a%d' % len(context.M)] = tck_util.get_bolt_value(row[0], row[1])


@step("adding map M to list L")
def step_impl(context):
    context.L.append(context.M)


@when("adding a table of lists to the map M")
def step_impl(context):
    for row in context.table:
        context.M['a%d' % len(context.M)] = tck_util.get_list_from_feature_file(row[1], row[0])


@step("adding a copy of map M to map M")
def step_impl(context):
    context.M['a%d' % len(context.M)] = context.M.copy()


@when("the driver asks the server to echo this value back")
def step_impl(context):
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypher_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@when("the driver asks the server to echo this list back")
def step_impl(context):
    context.expected = context.L
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypher_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@when("the driver asks the server to echo this map back")
def step_impl(context):
    context.expected = context.M
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypher_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@then("the result returned from the server should be a single record with a single value")
def step_impl(context):
    assert context.results
    for result in context.results.values():
        assert len(result) == 1
        assert len(result[0]) == 1


@step("the value given in the result should be the same as what was sent")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results.values():
        result_value = result[0].values()[0]
        assert result_value == context.expected


@given("A driver containing a session pool of size (?P<size>\d+)")
def step_impl(context, size):
    context.driver = GraphDatabase.driver("bolt://localhost", max_pool_size=1)


@when("acquiring a session from the driver")
def step_impl(context):
    context.session = context.driver.session()


@step('with the session running the Cypher statement "(?P<statement>.+)"')
def step_impl(context, statement):
    context.cursor = context.session.run(statement)


@step("pulling the result records")
def step_impl(context):
    context.cursor.consume()


@then("acquiring a session from the driver should not be possible")
def step_impl(context):
    try:
        context.session = context.driver.session()
    except:
        assert True
    else:
        assert False


@then("acquiring a session from the driver should be possible")
def step_impl(context):
    _ = context.driver.session()
    assert True


@given("init: (?P<statement>.+);")
def step_impl(context, statement):
    tck_util.send_string(statement)


@when("running: (?P<statement>.+);")
def step_impl(context, statement):
    context.results = {"as_string": tck_util.send_string(statement)}


@given("an empty database")
def step_impl(context):
    tck_util.send_string("MATCH (n) DETACH DELETE n")


@then("result should be a path p containing")
def step_impl(context):
    result = context.results["as_string"]
    given = tck_util.result_to_set(result)
    expected = tck_util.text_path_to_set(context.table)
    if given != expected:
        raise Exception("Path does not match given: %s expected: %s" % (given, expected))


@then("result should be integer\(s\)")
def step_impl(context):
    result = context.results["as_string"]
    given = tck_util.result_to_set(result)
    expected = tck_util.text_int_to_set(context.table)
    if given != expected:
        raise Exception("Integers does not match given: %s expected: %s" % (given, expected))


@then("result should be node\(s\)")
def step_impl(context):
    result = context.results["as_string"]
    given = tck_util.result_to_set(result)
    expected = tck_util.text_node_to_set(context.table)
    if given != expected:
        raise Exception("Nodes does not match given: %s expected: %s" % (given, expected))


@then("result should be string\(s\)")
def step_impl(context):
    result = context.results["as_string"]
    given = tck_util.result_to_set(result)
    expected = tck_util.text_string_to_set(context.table)
    if given != expected:
        raise Exception("Strings does not match given: %s expected: %s" % (given, expected))


@then("result should be relationship\(s\)")
def step_impl(context):
    result = context.results["as_string"]
    given = tck_util.result_to_set(result)
    expected = tck_util.text_relationship_to_set(context.table)
    if given != expected:
        raise Exception("Relationship does not match given: %s expected: %s" % (given, expected))


@then("result should be empty")
def step_impl(context):
    assert context.results
    for result in context.results.values():
        assert len(result) == 0


@then('result should be node "(?P<n1>.+)" node "(?P<n2>.+)" and int "(?P<len>\d+)"')
def step_impl(context, n1, n2, len):
    result = context.results["as_string"]
    given = tck_util.single_result_to_values(result)
    expected = tck_util.node_node_int_to_values(n1, n2, len)
    if given != expected:
        raise Exception("Mixed response does not match given: %s expected: %s" % (given, expected))


@when('running "(?P<parameters>.+)": (?P<statement>.+);')
def step_impl(context, parameters, statement):
    parameters = json.loads(parameters)
    context.results = {"as_string": tck_util.send_parameters(statement, parameters)}
