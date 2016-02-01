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

import random
import string

from behave import *

from neo4j.v1 import GraphDatabase
from test.tck.tck_util import to_unicode, Type, send_string, send_parameters, string_to_type

from neo4j.v1 import compat
use_step_matcher("re")


@given("A running database")
def step_impl(context):
    send_string("RETURN 1")


@given("a value (?P<input>.+) of type (?P<bolt_type>.+)")
def step_impl(context, input, bolt_type):
    context.expected = get_bolt_value(string_to_type(bolt_type), input)


@given("a value  of type (?P<bolt_type>.+)")
def step_impl(context, bolt_type):
    context.expected = get_bolt_value(string_to_type(bolt_type), u' ')


@given("a list value (?P<input>.+) of type (?P<bolt_type>.+)")
def step_impl(context, input, bolt_type):
    context.expected = get_list_from_feature_file(input, string_to_type(bolt_type))


@given("an empty list L")
def step_impl(context):
    context.L = []


@given("an empty map M")
def step_impl(context):
    context.M = {}


@given("a String of size (?P<size>\d+)")
def step_impl(context, size):
    context.expected = get_random_string(int(size))


@given("a List of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = get_list_of_random_type(int(size), string_to_type(type))


@given("a Map of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = get_dict_of_random_type(int(size), string_to_type(type))


@step("adding a table of lists to the list L")
def step_impl(context):
    for row in context.table:
        context.L.append(get_list_from_feature_file(row[1], row[0]))


@step("adding a table of values to the list L")
def step_impl(context):
    for row in context.table:
        context.L.append(get_bolt_value(row[0], row[1]))


@step("adding a table of values to the map M")
def step_impl(context):
    for row in context.table:
        context.M['a%d' % len(context.M)] = get_bolt_value(row[0], row[1])


@step("adding map M to list L")
def step_impl(context):
    context.L.append(context.M)


@when("adding a table of lists to the map M")
def step_impl(context):
    for row in context.table:
        context.M['a%d' % len(context.M)] = get_list_from_feature_file(row[1], row[0])


@step("adding a copy of map M to map M")
def step_impl(context):
    context.M['a%d' % len(context.M)] = context.M.copy()


@when("the driver asks the server to echo this value back")
def step_impl(context):
    context.results = {"as_string": send_string("RETURN " + as_cypher_text(context.expected)),
                       "as_parameters": send_parameters("RETURN {input}", {'input': context.expected})}


@when("the driver asks the server to echo this list back")
def step_impl(context):
    context.expected = context.L
    context.results = {"as_string": send_string("RETURN " + as_cypher_text(context.expected)),
                       "as_parameters": send_parameters("RETURN {input}", {'input': context.expected})}


@when("the driver asks the server to echo this map back")
def step_impl(context):
    context.expected = context.M
    context.results = {"as_string": send_string("RETURN " + as_cypher_text(context.expected)),
                       "as_parameters": send_parameters("RETURN {input}", {'input': context.expected})}


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


def get_bolt_value(type, value):
    if type == Type.INTEGER:
        return int(value)
    if type == Type.FLOAT:
        return float(value)
    if type == Type.STRING:
        return to_unicode(value)
    if type == Type.NULL:
        return None
    if type == Type.BOOLEAN:
        return bool(value)
    raise ValueError('No such type : %s' % type)


def as_cypher_text(expected):
    if expected is None:
        return "Null"
    if isinstance(expected, (str, compat.string)):
        return '"' + expected + '"'
    if isinstance(expected, float):
        return repr(expected).replace('+', '')
    if isinstance(expected, list):
        l = u'['
        for i, val in enumerate(expected):
            l += as_cypher_text(val)
            if i < len(expected) - 1:
                l += u','
        l += u']'
        return l
    if isinstance(expected, dict):
        d = u'{'
        for i, (key, val) in enumerate(expected.items()):
            d += to_unicode(key) + ':'
            d += as_cypher_text(val)
            if i < len(expected.items()) - 1:
                d += u','
        d += u'}'
        return d
    else:
        return to_unicode(expected)


def get_list_from_feature_file(string_list, bolt_type):
    inputs = string_list.strip('[]')
    inputs = inputs.split(',')
    list_to_return = []
    for value in inputs:
        list_to_return.append(get_bolt_value(bolt_type, value))
    return list_to_return


def get_random_string(size):
    return u''.join(
            random.SystemRandom().choice(list(string.ascii_uppercase + string.digits + string.ascii_lowercase)) for _ in
            range(size))


def get_random_bool():
    return bool(random.randint(0, 1))


def _get_random_func(type):
    def get_none():
        return None

    if type == Type.INTEGER:
        fu = random.randint
        args = [-9223372036854775808, 9223372036854775808]
    elif type == Type.FLOAT:
        fu = random.random
        args = []
    elif type == Type.STRING:
        fu = get_random_string
        args = [3]
    elif type == Type.NULL:
        fu = get_none
        args = []
    elif type == Type.BOOLEAN:
        fu = get_random_bool
        args = []
    else:
        raise ValueError('No such type : %s' % type)
    return (fu, args)


def get_list_of_random_type(size, type):
    fu, args = _get_random_func(type)
    return [fu(*args) for _ in range(size)]


def get_dict_of_random_type(size, type):
    fu, args = _get_random_func(type)
    return {'a%d' % i: fu(*args) for i in range(size)}
