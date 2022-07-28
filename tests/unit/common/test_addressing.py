# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import typing as t
from socket import (
    AF_INET,
    AF_INET6,
)
from unittest import mock

import pytest

from neo4j import (
    Address,
    IPv4Address,
)


mock_socket_ipv4 = mock.Mock()
mock_socket_ipv4.getpeername = lambda: ("127.0.0.1", 7687)  # (address, port)

mock_socket_ipv6 = mock.Mock()
mock_socket_ipv6.getpeername = lambda: ("[::1]", 7687, 0, 0)  # (address, port, flow info, scope id)


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
def test_address_initialization(
    test_input: t.Union[tuple, Address], expected: dict
) -> None:
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
def test_address_init_with_address_object_returns_same_instance(
    test_input: Address
) -> None:
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
def test_address_initialization_with_incorrect_input(
    test_input: tuple, expected
) -> None:
    with pytest.raises(expected):
        _ = Address(test_input)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (mock_socket_ipv4, ("127.0.0.1", 7687)),
        (mock_socket_ipv6, ("[::1]", 7687, 0, 0))
    ]
)
def test_address_from_socket(test_input: mock.Mock, expected: tuple) -> None:
    _ = Address.from_socket(mock_socket_ipv4)
    address = Address.from_socket(test_input)
    assert address == expected


def test_address_from_socket_with_none() -> None:
    with pytest.raises(AttributeError):
        _ = Address.from_socket(None)  # type: ignore[arg-type]


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
def test_address_parse_with_ipv4(test_input: str, expected: tuple) -> None:
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
def test_address_should_parse_ipv6(test_input: str, expected: tuple) -> None:
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
def test_address_parse_with_invalid_input(test_input, expected) -> None:
    with pytest.raises(expected):
        _ = Address.parse(test_input)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (("localhost:7687 [::1]:7687",), 2),
        (("localhost:7687", "[::1]:7687"), 2),
        (("localhost:7687 localhost:7688", "[::1]:7687"), 3),
        (("localhost:7687 localhost:7687", "[::1]:7687"), 3),
    ]
)
def test_address_parse_list(test_input: tuple, expected: int) -> None:
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
def test_address_parse_list_with_invalid_input(
    test_input: tuple, expected
) -> None:
    with pytest.raises(TypeError):
        _ = Address.parse_list(*test_input)
