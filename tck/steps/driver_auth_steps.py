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

from neo4j.v1 import GraphDatabase, basic_auth, exceptions
from tck.tck_util import BOLT_URI, AUTH_TOKEN


@given("a driver configured with auth disabled")
def step_impl(context):
    context.driver = GraphDatabase.driver(BOLT_URI, encrypted=False)


@given("a driver is configured with auth enabled and correct password is provided")
def step_impl(context):
    context.driver = GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN, encrypted=False)


@given("a driver is configured with auth enabled and the wrong password is provided")
def step_impl(context):
    context.driver = GraphDatabase.driver(BOLT_URI, auth=basic_auth("neo4j", "wrong"), encrypted=False)


@step("reading and writing to the database should be possible")
def step_impl(context):
    session = context.driver.session()
    session.run("CREATE (:label1)")
    assert len(list(session.run("MATCH (n:label1) RETURN n"))) == 1
    session.close()


@step("reading and writing to the database should not be possible")
def step_impl(context):
    try:
        session = context.driver.session()
        session.run("CREATE (:label1)")
        session.close()
        assert False
    except exceptions.ProtocolError as e:
        pass


@step("a `Protocol Error` is raised")
def step_impl(context):
    pass