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

from neo4j.v1 import GraphDatabase
from neo4j.v1.exceptions import ProtocolError, CypherError
from tck import tck_util

use_step_matcher("re")


@given("I have a driver")
def step_impl(context):
    context.driver = tck_util.driver


@step("I start a `Transaction` through a session")
def step_impl(context):
    context.session = context.driver.session()
    context.session.begin_transaction()


@step("`run` a query with that same session without closing the transaction first")
def step_impl(context):
    try:
        context.session.run("CREATE (:n)")
    except Exception as e:
        context.exception = e
    finally:
        context.session.close()


@step("I start a new `Transaction` with the same session before closing the previous")
def step_impl(context):
    try:
        context.session.begin_transaction()
    except Exception as e:
        context.exception = e
    finally:
        context.session.close()


@step("I run a non valid cypher statement")
def step_impl(context):
    try:
        s = context.driver.session()
        print(s.transaction)
        s.run("NOT VALID").consume()
    except Exception as e:
        context.exception = e


@step("I set up a driver to an incorrect port")
def step_impl(context):
    try:
        context.driver = GraphDatabase.driver("bolt://localhost:7777")
        context.driver.session()
    except Exception as e:
        context.exception = e


@step("I set up a driver with wrong scheme")
def step_impl(context):
    try:
        context.driver = GraphDatabase.driver("wrong://localhost")
        context.driver.session()
    except Exception as e:
        context.exception = e


@step("it throws a `ClientException`")
def step_impl(context):
    print(context.exception)
    assert context.exception is not None
    assert type(context.exception) == ProtocolError or type(context.exception) == CypherError
    assert isinstance(context.exception, ProtocolError) or isinstance(context.exception, CypherError)
    assert str(context.exception).startswith(context.table.rows[0][0])
