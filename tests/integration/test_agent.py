#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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


from neo4j import Agent


def test_default_router():
    agent = Agent()
    assert agent.routers == [("localhost", 7687)]


def test_single_router_host_only():
    agent = Agent("example.com")
    assert agent.routers == [("example.com", 7687)]


def test_single_router_port_only():
    agent = Agent(":9999")
    assert agent.routers == [("localhost", 9999)]


def test_single_router_host_and_port():
    agent = Agent("x.com:9999")
    assert agent.routers == [("x.com", 9999)]


def test_multiple_routers_port_only():
    agent = Agent(":9001 :9002 :9003")
    assert agent.routers == [("localhost", 9001), ("localhost", 9002), ("localhost", 9003)]


def test_multiple_routers_host_and_port():
    agent = Agent("a.com:9001 b.com:9002 c.com:9003")
    assert agent.routers == [("a.com", 9001), ("b.com", 9002), ("c.com", 9003)]
