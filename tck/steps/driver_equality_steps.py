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

from tck.tck_util import send_string

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
