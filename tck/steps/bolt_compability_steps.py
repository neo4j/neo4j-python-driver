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

import copy
import random
import string

from behave import *
from tck.resultparser import parse_values

from neo4j.v1 import compat
from tck import tck_util
from tck.tck_util import to_unicode, Type, string_to_type

use_step_matcher("re")


@given("A running database")
def step_impl(context):
    session = tck_util.driver.session()
    session.run("RETURN 1")
    session.close()


@given("a value (?P<input>.+)")
def step_impl(context, input):
    context.expected = parse_values(input)


@given("a list containing")
def step_impl(context):
    context.expected = [parse_values(row[0]) for row in context.table.rows]


@step("adding this list to itself")
def step_impl(context):
    clone = context.expected[:]
    context.expected.append(clone)


@given("a map containing")
def step_impl(context):
    context.expected = {parse_values(row[0]): parse_values(row[1]) for row in context.table.rows}


@step('adding this map to itself with key "(?P<key>.+)"')
def step_impl(context, key):
    clone = copy.deepcopy(context.expected)
    context.expected[key] = clone


@given("a String of size (?P<size>\d+)")
def step_impl(context, size):
    context.expected = get_random_string(int(size))


@given("a List of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = get_list_of_random_type(int(size), string_to_type(type))


@given("a Map of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = get_dict_of_random_type(int(size), string_to_type(type))


@when("the driver asks the server to echo this (?P<unused>.+) back")
def step_impl(context, unused):
    str_runner = tck_util.Runner("RETURN " + as_cypher_text(context.expected)).run()
    param_runner = tck_util.Runner("RETURN {input}", {'input': context.expected}).run()
    context.runners += [str_runner, param_runner]
    context.results = [str_runner.result, param_runner.result]


@step("the value given in the result should be the same as what was sent")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results:
        records = list(result)
        assert len(records) == 1
        assert len(records[0].values()) == 1
        result_value = records[0].values()[0]
        assert result_value == context.expected


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
