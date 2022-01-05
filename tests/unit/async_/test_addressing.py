# Copyright (c) "Neo4j"
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


from socket import (
    AF_INET,
    AF_INET6,
)
import unittest.mock as mock

import pytest

from neo4j import (
    Address,
    IPv4Address,
)
from neo4j._async_compat.network import AsyncNetworkUtil
from neo4j._async_compat.util import AsyncUtil

from ..._async_compat import mark_async_test


mock_socket_ipv4 = mock.Mock()
mock_socket_ipv4.getpeername = lambda: ("127.0.0.1", 7687)  # (address, port)

mock_socket_ipv6 = mock.Mock()
mock_socket_ipv6.getpeername = lambda: ("[::1]", 7687, 0, 0)  # (address, port, flow info, scope id)


@mark_async_test
async def test_address_resolve():
    address = Address(("127.0.0.1", 7687))
    resolved = AsyncNetworkUtil.resolve_address(address)
    resolved = await AsyncUtil.list(resolved)
    assert isinstance(resolved, Address) is False
    assert isinstance(resolved, list) is True
    assert len(resolved) == 1
    assert resolved[0] == IPv4Address(('127.0.0.1', 7687))


@mark_async_test
async def test_address_resolve_with_custom_resolver_none():
    address = Address(("127.0.0.1", 7687))
    resolved = AsyncNetworkUtil.resolve_address(address, resolver=None)
    resolved = await AsyncUtil.list(resolved)
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
@mark_async_test
async def test_address_resolve_with_unresolvable_address(test_input, expected):
    with pytest.raises(expected):
        await AsyncUtil.list(
            AsyncNetworkUtil.resolve_address(test_input, resolver=None)
        )


@mark_async_test
@pytest.mark.parametrize("resolver_type", ("sync", "async"))
async def test_address_resolve_with_custom_resolver(resolver_type):
    def custom_resolver_sync(_):
        return [("127.0.0.1", 7687), ("localhost", 1234)]

    async def custom_resolver_async(_):
        return [("127.0.0.1", 7687), ("localhost", 1234)]

    if resolver_type == "sync":
        custom_resolver = custom_resolver_sync
    else:
        custom_resolver = custom_resolver_async

    address = Address(("127.0.0.1", 7687))
    resolved = AsyncNetworkUtil.resolve_address(
        address, family=AF_INET, resolver=custom_resolver
    )
    resolved = await AsyncUtil.list(resolved)
    assert isinstance(resolved, Address) is False
    assert isinstance(resolved, list) is True
    assert len(resolved) == 2  # IPv4 only
    assert resolved[0] == IPv4Address(('127.0.0.1', 7687))
    assert resolved[1] == IPv4Address(('127.0.0.1', 1234))


@mark_async_test
async def test_address_unresolve():
    custom_resolved = [("127.0.0.1", 7687), ("localhost", 4321)]
    custom_resolver = lambda _: custom_resolved

    address = Address(("foobar", 1234))
    unresolved = address.unresolved
    assert address.__class__ == unresolved.__class__
    assert address == unresolved
    resolved = AsyncNetworkUtil.resolve_address(
        address, family=AF_INET, resolver=custom_resolver
    )
    resolved = await AsyncUtil.list(resolved)
    custom_resolved = sorted(Address(a) for a in custom_resolved)
    unresolved = sorted(a.unresolved for a in resolved)
    assert custom_resolved == unresolved
    assert (list(map(lambda a: a.__class__, custom_resolved))
            == list(map(lambda a: a.__class__, unresolved)))
