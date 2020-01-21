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


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (("127.0.0.1", 7687), {"family": AF_INET, "host": "127.0.0.1", "port": 7687, "str": "127.0.0.1:7687", "repr": "IPv4Address(('127.0.0.1', 7687))"}),
        (("localhost", 7687), {"family": AF_INET, "host": "localhost", "port": 7687, "str": "localhost:7687", "repr": "IPv4Address(('localhost', 7687))"}),
        ((None, None), {"family": AF_INET, "host": None, "port": None, "str": "None:None", "repr": "IPv4Address((None, None))"}),
        (("::1", 7687), {"family": AF_INET, "host": "::1", "port": 7687, "str": "::1:7687", "repr": "IPv4Address(('::1', 7687))"}),
        (("::1", 7687, 0, 0), {"family": AF_INET6, "host": "::1", "port": 7687, "str": "[::1]:7687", "repr": "IPv6Address(('::1', 7687, 0, 0))"}),
        (("::1", 7687, 1, 2), {"family": AF_INET6, "host": "::1", "port": 7687, "str": "[::1]:7687", "repr": "IPv6Address(('::1', 7687, 1, 2))"}),
        ((None, None, None, None), {"family": AF_INET6, "host": None, "port": None, "str": "[None]:None", "repr": "IPv6Address((None, None, None, None))"}),
        (Address(("127.0.0.1", 7687)), {"family": AF_INET, "host": "127.0.0.1", "port": 7687, "str": "127.0.0.1:7687", "repr": "IPv4Address(('127.0.0.1', 7687))"}),
        (Address(("::1", 7687, 1, 2)), {"family": AF_INET6, "host": "::1", "port": 7687, "str": "[::1]:7687", "repr": "IPv6Address(('::1', 7687, 1, 2))"}),
    ]
)
def test_address_initialization(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_initialization
    address = Address(test_input)
    assert address.family == expected["family"]
    assert address.host == expected["host"]
    assert address.port == expected["port"]
    assert str(address) == expected["str"]
    assert repr(address) == expected["repr"]


@pytest.mark.parametrize(
    "test_input",
    [
        Address(("127.0.0.1", 7687)),
        Address(("127.0.0.1", 7687, 1, 2)),
    ]
)
def test_address_init_with_address_object_returns_same_instance(test_input):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_init_with_address_object_returns_same_instance
    address = Address(test_input)
    assert address is test_input
    assert id(address) == id(test_input)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (("127.0.0.1",), ValueError),
        (("127.0.0.1", 7687, 0), ValueError),
        (("[::1]", 7687, 0), ValueError),
        (("[::1]", 7687, 0, 0, 0), ValueError),
    ]
)
def test_address_initialization_with_incorrect_input(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_initialization_with_incorrect_input
    with pytest.raises(expected):
        address = Address(test_input)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (mock_socket_ipv4, ("127.0.0.1", 7687)),
        (mock_socket_ipv6, ("[::1]", 7687, 0, 0))
    ]
)
def test_address_from_socket(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_from_socket

    address = Address.from_socket(test_input)
    assert address == expected


def test_address_from_socket_with_none():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_from_socket_with_none
    with pytest.raises(AttributeError):
        address = Address.from_socket(None)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("127.0.0.1:7687", ("127.0.0.1", 7687)),
        ("localhost:7687", ("localhost", 7687)),
        (":7687", ("localhost", 7687)),
        (":", ("localhost", 0)),
        ("", ("localhost", 0)),
        (":abcd", ("localhost", "abcd")),
        (" ", (" ", 0)),
    ]
)
def test_address_parse_with_ipv4(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_parse_with_ipv4
    parsed = Address.parse(test_input)
    assert parsed == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("[::1]:7687", ("::1", 7687, 0, 0)),
        ("[::1]:abcd", ("::1", "abcd", 0, 0)),
        ("[::1]:", ("::1", 0, 0, 0)),
        ("[::1]", ("::1", 0, 0, 0)),
    ]
)
def test_address_should_parse_ipv6(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_should_parse_ipv6
    parsed = Address.parse(test_input)
    assert parsed == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (None, TypeError),
        (123, TypeError),
        (("127.0.0.1", 7687), TypeError),
        (("[::1]", 7687, 1, 2), TypeError),
        (Address(("127.0.0.1", 7687)), TypeError),
    ]
)
def test_address_parse_with_invalid_input(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_parse_with_invalid_input
    with pytest.raises(expected):
        parsed = Address.parse(test_input)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (("localhost:7687 [::1]:7687",), 2),
        (("localhost:7687", "[::1]:7687"), 2),
        (("localhost:7687 localhost:7688", "[::1]:7687"), 3),
        (("localhost:7687 localhost:7687", "[::1]:7687"), 3),
    ]
)
def test_address_parse_list(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_parse_list
    addresses = Address.parse_list(*test_input)
    assert len(addresses) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (("localhost:7687", None), TypeError),
        (("localhost:7687", 123), TypeError),
        (("localhost:7687", ("127.0.0.1", 7687)), TypeError),
        (("localhost:7687", ("[::1]", 7687, 1, 2)), TypeError),
        (("localhost:7687", Address(("127.0.0.1", 7687))), TypeError),
    ]
)
def test_address_parse_list_with_invalid_input(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_parse_list_with_invalid_input
    with pytest.raises(TypeError):
        addresses = Address.parse_list(*test_input)


def test_address_resolve():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_resolve
    address = Address(("127.0.0.1", 7687))
    resolved = address.resolve()
    assert isinstance(resolved, Address) is False
    assert isinstance(resolved, list) is True
    assert len(resolved) == 1
    assert resolved[0] == IPv4Address(('127.0.0.1', 7687))


def test_address_resolve_with_custom_resolver_none():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_resolve_with_custom_resolver_none
    address = Address(("127.0.0.1", 7687))
    resolved = address.resolve(resolver=None)
    assert isinstance(resolved, Address) is False
    assert isinstance(resolved, list) is True
    assert len(resolved) == 1
    assert resolved[0] == IPv4Address(('127.0.0.1', 7687))


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (Address(("127.0.0.1", "abcd")), ValueError),
        (Address((None, None)), ValueError),
    ]
)
def test_address_resolve_with_unresolvable_address(test_input, expected):
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_resolve_with_unresolvable_address
    with pytest.raises(expected):
        test_input.resolve(resolver=None)


def test_address_resolve_with_custom_resolver():
    # python -m pytest tests/unit/test_addressing.py -s -k test_address_resolve_with_custom_resolver
    custom_resolver = lambda a: [("127.0.0.1", 7687), ("localhost", 1234)]

    address = Address(("127.0.0.1", 7687))
    resolved = address.resolve(resolver=custom_resolver)
    assert isinstance(resolved, Address) is False
    assert isinstance(resolved, list) is True
    assert len(resolved) == 3
    assert resolved[0] == IPv4Address(('127.0.0.1', 7687))
    assert resolved[1] == IPv6Address(('::1', 1234, 0, 0))
    assert resolved[2] == IPv4Address(('127.0.0.1', 1234))


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
