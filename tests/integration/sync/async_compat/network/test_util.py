# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

import socket

import pytest

from neo4j._addressing import (
    ResolvedAddress,
    ResolvedIPv4Address,
    ResolvedIPv6Address,
)
from neo4j._async_compat.network import NetworkUtil
from neo4j.addressing import Address
from neo4j.exceptions import ServiceUnavailable

from ....._async_compat import mark_sync_test


@mark_sync_test
def test_resolve_address():
    resolved = [
        addr
        for addr in NetworkUtil.resolve_address(
            Address(("localhost", 1234)),
        )
    ]
    assert all(isinstance(addr, ResolvedAddress) for addr in resolved)
    for addr in resolved:
        if isinstance(addr, ResolvedIPv4Address):
            assert len(addr) == 2
            assert addr[0].startswith("127.0.0.")
            assert addr[1] == 1234
        elif isinstance(addr, ResolvedIPv6Address):
            assert len(addr) == 4
            assert addr[:2] == ("::1", 1234)


@mark_sync_test
def test_resolve_invalid_address():
    with pytest.raises(ServiceUnavailable) as exc:
        next(
            NetworkUtil.resolve_address(
                Address(("example.invalid", 1234)),
            )
        )
    cause = exc.value.__cause__
    assert isinstance(cause, socket.gaierror)
    assert cause.errno, socket.EAI_NONAME
