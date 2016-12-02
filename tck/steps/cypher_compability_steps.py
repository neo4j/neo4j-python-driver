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
from tck.resultparser import parse_values

from tck import tck_util

use_step_matcher("re")


@given("init: (?P<statement>.+)")
def step_impl(context, statement):
    session = tck_util.driver.session()
    session.run(statement)
    session.close()


@step("running: (?P<statement>.+)")
def step_impl(context, statement):
    runner = tck_util.Runner(statement).run()
    context.runners.append(runner)
    context.results = [runner.result]


@step('running parametrized: (?P<statement>.+)')
def step_impl(context, statement):
    assert len(context.table.rows) == 1
    keys = context.table.headings
    values = context.table.rows[0]
    parameters = {keys[i]: parse_values(values[i]) for i in range(len(keys))}
    runner = tck_util.Runner(statement, parameters).run()
    context.runners.append(runner)
    context.results = [runner.result]


@then("result")
def step_impl(context):
    expected = tck_util.table_to_comparable_result(context.table)
    assert(len(context.results) > 0)
    for result in context.results:
        records = list(result)
        given = tck_util.driver_result_to_comparable_result(records)
        if not tck_util.unordered_equal(given, expected):
            raise Exception("Does not match given: \n%s expected: \n%s" % (given, expected))
