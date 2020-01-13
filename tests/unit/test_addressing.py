#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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


from unittest import TestCase

from neo4j import GraphDatabase
from neo4j.addressing import Address


class RoutingTableParseAddressTestCase(TestCase):

    def test_should_parse_ipv4_address_and_port(self):
        parsed = Address.parse("127.0.0.1:7687")
        self.assertEqual(parsed, ("127.0.0.1", 7687))

    def test_should_parse_ipv6_address_and_port(self):
        parsed = Address.parse("[::1]:7687")
        self.assertEqual(parsed, ("::1", 7687, 0, 0))

    def test_should_parse_host_name_and_port(self):
        parsed = Address.parse("localhost:7687")
        self.assertEqual(parsed, ("localhost", 7687))

    def verify_routing_context(self, expected, query):
        context = GraphDatabase._parse_routing_context(query)
        self.assertEqual(context, expected)

    def test_parse_routing_context(self):
        self.verify_routing_context({"name": "molly", "color": "white"}, "name=molly&color=white")
        self.verify_routing_context({"name": "molly", "color": "white"}, "name=molly&color=white")
        self.verify_routing_context({"name": "molly", "color": "white"}, "name=molly&color=white")

    def test_should_error_when_value_missing(self):
        with self.assertRaises(ValueError):
            GraphDatabase._parse_routing_context("name=&color=white")

    def test_should_error_when_key_duplicate(self):
        with self.assertRaises(ValueError):
            GraphDatabase._parse_routing_context("name=molly&name=white")
