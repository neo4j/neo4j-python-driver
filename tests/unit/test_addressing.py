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


import pytest
import unittest.mock as mock
from socket import (
    AF_INET,
    AF_INET6,
)

from neo4j.addressing import (
    Address,
    IPv4Address,
    IPv6Address,
)
from neo4j import GraphDatabase

mock_socket_ipv4 = mock.Mock()
mock_socket_ipv4.getpeername = lambda: ("127.0.0.1", 7687)  # (address, port)

mock_socket_ipv6 = mock.Mock()
mock_socket_ipv6.getpeername = lambda: ("[::1]", 7687, 0, 0)  # (address, port, flow info, scope id)

# python -m pytest tests/unit/test_addressing.py -s


def test_address_initialization():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_initialization

    address = Address(("127.0.0.1", 7687))
    assert address.family == AF_INET
    assert address.host == "127.0.0.1"
    assert address.port == 7687

    assert str(address) == "127.0.0.1:7687"
    assert repr(address) == "IPv4Address(('127.0.0.1', 7687))"

    address = Address(("localhost", 7687))
    assert address.family == AF_INET

    address = Address((None, None))
    assert address.family == AF_INET

    address = Address(("[::1]", 7687, 0, 0))
    assert address.family == AF_INET6

    assert str(address) == "[[::1]]:7687"
    assert repr(address) == "IPv6Address(('[::1]', 7687, 0, 0))"

    address = Address((None, None, None, None))
    assert address.family == AF_INET6

    iterable_address = Address(("127.0.0.1", 7687))

    address = Address(iterable_address)
    assert address.family == AF_INET
    assert address is not iterable_address

    with pytest.raises(ValueError):
        address = Address(("127.0.0.1",))

    with pytest.raises(ValueError):
        address = Address(("127.0.0.1", 7687, 0))

    with pytest.raises(ValueError):
        address = Address(("[::1]", 7687, 0))

    with pytest.raises(ValueError):
        address = Address(("[::1]", 7687, 0, 0, 0))


def test_address_from_socket():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_from_socket

    address = Address.from_socket(mock_socket_ipv4)
    assert address == ("127.0.0.1", 7687)

    address = Address.from_socket(mock_socket_ipv6)
    assert address == ("[::1]", 7687, 0, 0)

    with pytest.raises(AttributeError):
        address = Address.from_socket(None)


def test_address_should_parse_ipv4_host_and_port():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_should_parse_ipv4_host_and_port

    parsed = Address.parse("127.0.0.1:7687")
    assert parsed == ("127.0.0.1", 7687)

    parsed = Address.parse("localhost:7687")
    assert parsed == ("localhost", 7687)

    parsed = Address.parse(":7687")
    assert parsed == ("localhost", 7687)

    parsed = Address.parse(":")
    assert parsed == ("localhost", 0)

    parsed = Address.parse("")
    assert parsed == ("localhost", 0)

    parsed = Address.parse(":abcd")
    assert parsed == ("localhost", "abcd")

    parsed = Address.parse(" ")
    assert parsed == (" ", 0)

    with pytest.raises(TypeError):
        parsed = Address.parse(None)

    with pytest.raises(TypeError):
        parsed = Address.parse(123)

    with pytest.raises(TypeError):
        parsed = Address.parse(("127.0.0.1", 7687))


def test_address_should_parse_ipv6_host_and_port():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_should_parse_ipv6_host_and_port

    parsed = Address.parse("[::1]:7687")
    assert parsed == ("::1", 7687, 0, 0)

    parsed = Address.parse("[::1]:abcd")
    assert parsed == ("::1", "abcd", 0, 0)


def test_address_parse_list():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_parse_list

    addresses = Address.parse_list("localhost:7687 [::1]:7687")
    assert len(addresses) == 2

    addresses = Address.parse_list("localhost:7687", "[::1]:7687")
    assert len(addresses) == 2

    with pytest.raises(TypeError):
        addresses = Address.parse_list("localhost:7687", None)


def test_address_resolve():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_resolve

    address = Address(("127.0.0.1", 7687))
    resolved = address.resolve()
    assert isinstance(resolved, Address) is False
    assert isinstance(resolved, list) is True
    assert len(resolved) == 1
    assert resolved[0] == IPv4Address(('127.0.0.1', 7687))


def test_graphdatabase_parse_routing_context():
    # python -m pytest tests/unit/test_addressing.py -s -k test_graphdatabase_parse_routing_context

    context = GraphDatabase._parse_routing_context(query="name=molly&color=white")
    assert context == {"name": "molly", "color": "white"}


def test_graphdatabase_parse_routing_context_should_error_when_value_missing():
    # python -m pytest tests/unit/test_addressing.py -s -k test_graphdatabase_parse_routing_context_should_error_when_value_missing

    with pytest.raises(ValueError):
        GraphDatabase._parse_routing_context("name=&color=white")


def test_graphdatabase_parse_routing_context_should_error_when_key_duplicate():
    # python -m pytest tests/unit/test_addressing.py -s -k test_graphdatabase_parse_routing_context_should_error_when_key_duplicate

    with pytest.raises(ValueError):
        GraphDatabase._parse_routing_context("name=molly&name=white")
