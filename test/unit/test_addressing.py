#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
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


from unittest import TestCase

from neo4j.addressing import SocketAddress


class RoutingTableParseAddressTestCase(TestCase):

    def test_should_parse_ipv4_address_and_port(self):
        parsed = SocketAddress.parse("127.0.0.1:7687")
        assert parsed == ("127.0.0.1", 7687)

    def test_should_parse_ipv6_address_and_port(self):
        parsed = SocketAddress.parse("[::1]:7687")
        assert parsed == ("::1", 7687, 0, 0)

    def test_should_parse_host_name_and_port(self):
        parsed = SocketAddress.parse("localhost:7687")
        assert parsed == ("localhost", 7687)

    def test_should_fail_on_non_numeric_port(self):
        with self.assertRaises(ValueError):
            _ = SocketAddress.parse("127.0.0.1:X")

    def test_parse_empty_routing_context(self):
        verify_routing_context({}, "bolt+routing://127.0.0.1/cat?")
        verify_routing_context({}, "bolt+routing://127.0.0.1/cat")
        verify_routing_context({}, "bolt+routing://127.0.0.1/?")
        verify_routing_context({}, "bolt+routing://127.0.0.1/")
        verify_routing_context({}, "bolt+routing://127.0.0.1?")
        verify_routing_context({}, "bolt+routing://127.0.0.1")

    def test_parse_routing_context(self):
        verify_routing_context({"name": "molly", "color": "white"}, "bolt+routing://127.0.0.1/cat?name=molly&color=white")
        verify_routing_context({"name": "molly", "color": "white"}, "bolt+routing://127.0.0.1/?name=molly&color=white")
        verify_routing_context({"name": "molly", "color": "white"}, "bolt+routing://127.0.0.1?name=molly&color=white")

    def test_should_error_when_value_missing(self):
        with self.assertRaises(ValueError):
            SocketAddress.parse_routing_context("bolt+routing://127.0.0.1/?name=&color=white")

    def test_should_error_when_key_duplicate(self):
        with self.assertRaises(ValueError):
            SocketAddress.parse_routing_context("bolt+routing://127.0.0.1/?name=molly&name=white")


def verify_routing_context(expected, uri):
    context = SocketAddress.parse_routing_context(uri)
    assert context == expected
