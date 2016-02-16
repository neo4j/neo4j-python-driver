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

from test.tck.tck_util import TestValue, send_string, send_parameters
from test.tck.resultparser import parse_values, parse_values_to_comparable

use_step_matcher("re")


@given("init: (?P<statement>.+)")
def step_impl(context, statement):
    send_string(statement)


@when("running: (?P<statement>.+)")
def step_impl(context, statement):
    context.results = {"as_string": send_string(statement)}


@then("result")
def step_impl(context):
    result = context.results["as_string"]
    given = driver_result_to_comparable_result(result)
    expected = table_to_comparable_result(context.table)
    if not unordered_equal(given, expected):
        raise Exception("Does not match given: \n%s expected: \n%s" % (given, expected))


@when('running parametrized: (?P<statement>.+)')
def step_impl(context, statement):
    assert len(context.table.rows) == 1
    keys = context.table.headings
    values = context.table.rows[0]
    parameters = {keys[i]: parse_values(values[i]) for i in range(len(keys))}

    context.results = {"as_string": send_parameters(statement, parameters)}


def _driver_value_to_comparable(val):
    if isinstance(val, list):
        l = [_driver_value_to_comparable(v) for v in val]
        return l
    else:
        return TestValue(val)


def table_to_comparable_result(table):
    result = []
    keys = table.headings
    for row in table:
        result.append(
                {keys[i]: parse_values_to_comparable(row[i]) for i in range(len(row))})
    return result


def driver_result_to_comparable_result(result):
    records = []
    for record in result:
        records.append({key: _driver_value_to_comparable(record[key]) for key in record})
    return records


def unordered_equal(given, expected):
    l1 = given[:]
    l2 = expected[:]
    assert isinstance(l1, list)
    assert isinstance(l2, list)
    assert len(l1) == len(l2)
    for d1 in l1:
        size = len(l2)
        for d2 in l2:
            if d1 == d2:
                l2.remove(d2)
                break
        if size == len(l2):
            return False
    return True
