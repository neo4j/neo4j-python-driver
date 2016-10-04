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
from tck.resultparser import parse_values_to_comparable

from neo4j.v1 import Record, ResultSummary
from neo4j.v1.exceptions import ResultError
from tck import tck_util

use_step_matcher("re")


@step("using `Single` on `Statement Result` gives a `Record` containing")
def step_impl(context):
    expected = tck_util.table_to_comparable_result(context.table)
    for result in context.results:
        given = tck_util.driver_single_result_to_comparable(result.single())
        assert len(given) == 1
        assert tck_util.unordered_equal(given, expected)


@step("using `Single` on `Statement Result` throws exception")
def step_impl(context):
    for result in context.results:
        try:
            result.single()
            assert False, "Expected an error"
        except ResultError as e:
            assert str(e).startswith(context.table.rows[0][0])


@step("using `Next` on `Statement Result` gives a `Record`")
def step_impl(context):
    for result in context.results:
        record = next(iter(result))
        assert isinstance(record, Record)


@step("iterating through the `Statement Result` should follow the native code pattern")
def step_impl(context):
    for result in context.results:
        for rec in result:
            assert isinstance(rec, Record)


@step("using `Peek` on `Statement Result` gives a `Record` containing")
def step_impl(context):
    expected = tck_util.table_to_comparable_result(context.table)
    for result in context.results:
        given = tck_util.driver_single_result_to_comparable(result.peek())
        assert len(given) == 1
        assert tck_util.unordered_equal(given, expected)


@step("using `Next` on `Statement Result` gives a `Record` containing")
def step_impl(context):
    expected = tck_util.table_to_comparable_result(context.table)
    for result in context.results:
        given = tck_util.driver_single_result_to_comparable(next(iter(result)))
        assert len(given) == 1
        assert tck_util.unordered_equal(given, expected)


@step("using `Peek` on `Statement Result` fails")
def step_impl(context):
    for result in context.results:
        try:
            result.peek()
            assert False, "Expected an error"
        except ResultError as e:
            pass


@step("using `Next` on `Statement Result` fails")
def step_impl(context):
    for result in context.results:
        try:
            next(iter(result))
            assert False, "Expected an error"
        except StopIteration as e:
            pass


@step("it is not possible to go back")
def step_impl(context):
    for result in context.results:
        r1 = iter(result)
        r2 = iter(result)
        rec1 = next(r1)
        rec2 = next(r2)
        assert rec2 != rec1


@step("using `Keys` on `Statement Result` gives")
def step_impl(context):
    expected = [row[0] for row in context.table.rows]
    for result in context.results:
        given = result.keys()
        assert tuple(expected) == given


@step("using `List` on `Statement Result` gives")
def step_impl(context):
    expected = tck_util.table_to_comparable_result(context.table)
    for result in context.results:
        given = tck_util.driver_result_to_comparable_result(result)
        assert tck_util.unordered_equal(given, expected)


@step("using `List` on `Statement Result` gives a list of size 7, the previous records are lost")
def step_impl(context):
    for result in context.results:
        assert len(list(result)) == 7


@step("using `Consume` on `StatementResult` gives `ResultSummary`")
def step_impl(context):
    for result in context.results:
        assert isinstance(result.consume(), ResultSummary)


@step("using `Consume` on `StatementResult` multiple times gives the same `ResultSummary` each time")
def step_impl(context):
    for result in context.results:
        rs = result.consume()
        assert rs.counters == result.consume().counters


@step("using `Keys` on the single record gives")
def step_impl(context):
    expected = [row[0] for row in context.table.rows]
    for result in context.results:
        given = result.single().keys()
        assert tuple(expected) == given


@step("using `Values` on the single record gives")
def step_impl(context):
    expected = [parse_values_to_comparable(val[0]) for val in context.table.rows]
    for result in context.results:
        given = [tck_util.driver_value_to_comparable(val) for val in result.single().values()]
        assert expected == given


@step("using `Get` with index (?P<index>\d+) on the single record gives")
def step_impl(context, index):
    expected = parse_values_to_comparable(context.table.rows[0][0])
    for result in context.results:
        given = tck_util.driver_value_to_comparable(result.single()[int(index)])
        assert expected == given


@step("using `Get` with key `(?P<key>.+)` on the single record gives")
def step_impl(context, key):
    expected = parse_values_to_comparable(context.table.rows[0][0])
    for result in context.results:
        given = tck_util.driver_value_to_comparable(result.single()[key])
        assert expected == given