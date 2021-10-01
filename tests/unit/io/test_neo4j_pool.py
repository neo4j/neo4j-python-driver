#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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

import inspect
from unittest.mock import Mock

import pytest

from ..work import FakeConnection

from neo4j import (
    READ_ACCESS,
    WRITE_ACCESS,
)
from neo4j.addressing import ResolvedAddress
from neo4j.conf import (
    PoolConfig,
    WorkspaceConfig
)
from neo4j.io import Neo4jPool


ROUTER_ADDRESS = ResolvedAddress(("1.2.3.1", 9001), host_name="host")
READER_ADDRESS = ResolvedAddress(("1.2.3.1", 9002), host_name="host")
WRITER_ADDRESS = ResolvedAddress(("1.2.3.1", 9003), host_name="host")


@pytest.fixture()
def opener():
    def open_(addr, timeout):
        connection = FakeConnection()
        connection.addr = addr
        connection.timeout = timeout
        route_mock = Mock()
        route_mock.return_value = [{
            "ttl": 1000,
            "servers": [
                {"addresses": [str(ROUTER_ADDRESS)], "role": "ROUTE"},
                {"addresses": [str(READER_ADDRESS)], "role": "READ"},
                {"addresses": [str(WRITER_ADDRESS)], "role": "WRITE"},
            ],
        }]
        connection.attach_mock(route_mock, "route")
        opener_.connections.append(connection)
        return connection

    opener_ = Mock()
    opener_.connections = []
    opener_.side_effect = open_
    return opener_


@pytest.mark.parametrize("type_", ("r", "w"))
def test_chooses_right_connection_type(opener, type_):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS if type_ == "r" else WRITE_ACCESS,
                       30, "test_db", None)
    pool.release(cx1)
    if type_  == "r":
        assert cx1.addr == READER_ADDRESS
    else:
        assert cx1.addr == WRITER_ADDRESS


def test_reuses_connection(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx1)
    cx2 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    assert cx1 is cx2


@pytest.mark.parametrize("break_on_close", (True, False))
def test_closes_stale_connections(opener, break_on_close):
    def break_connection():
        pool.deactivate(cx1.addr)

        if cx_close_mock_side_effect:
            cx_close_mock_side_effect()

    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx1)
    assert cx1 in pool.connections[cx1.addr]
    # simulate connection going stale (e.g. exceeding) and than breaking when
    # the pool tries to close the connection
    cx1.stale.return_value = True
    cx_close_mock = cx1.close
    if break_on_close:
        cx_close_mock_side_effect = cx_close_mock.side_effect
        cx_close_mock.side_effect = break_connection
    cx2 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx2)
    assert cx1.close.called_once()
    assert cx2 is not cx1
    assert cx2.addr == cx1.addr
    assert cx1 not in pool.connections[cx1.addr]
    assert cx2 in pool.connections[cx2.addr]
