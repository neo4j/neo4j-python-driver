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
    RoutingConfig,
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


def test_acquires_new_routing_table_if_deleted(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db")

    del pool.routing_tables["test_db"]

    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db")


def test_acquires_new_routing_table_if_stale(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db")

    old_value = pool.routing_tables["test_db"].last_updated_time
    pool.routing_tables["test_db"].ttl = 0

    cx = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx)
    assert pool.routing_tables["test_db"].last_updated_time > old_value


def test_removes_old_routing_table(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx = pool.acquire(READ_ACCESS, 30, "test_db1", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db1")
    cx = pool.acquire(READ_ACCESS, 30, "test_db2", None)
    pool.release(cx)
    assert pool.routing_tables.get("test_db2")

    old_value = pool.routing_tables["test_db1"].last_updated_time
    pool.routing_tables["test_db1"].ttl = 0
    pool.routing_tables["test_db2"].ttl = \
        -RoutingConfig.routing_table_purge_delay

    cx = pool.acquire(READ_ACCESS, 30, "test_db1", None)
    pool.release(cx)
    assert pool.routing_tables["test_db1"].last_updated_time > old_value
    assert "test_db2" not in pool.routing_tables


@pytest.mark.parametrize("type_", ("r", "w"))
def test_chooses_right_connection_type(opener, type_):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS if type_ == "r" else WRITE_ACCESS,
                       30, "test_db", None)
    pool.release(cx1)
    if type_ == "r":
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
    # simulate connection going stale (e.g. exceeding) and then breaking when
    # the pool tries to close the connection
    cx1.stale.return_value = True
    cx_close_mock = cx1.close
    if break_on_close:
        cx_close_mock_side_effect = cx_close_mock.side_effect
        cx_close_mock.side_effect = break_connection
    cx2 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx2)
    if break_on_close:
        cx1.close.assert_called()
    else:
        cx1.close.assert_called_once()
    assert cx2 is not cx1
    assert cx2.addr == cx1.addr
    assert cx1 not in pool.connections[cx1.addr]
    assert cx2 in pool.connections[cx2.addr]


def test_does_not_close_stale_connections_in_use(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    assert cx1 in pool.connections[cx1.addr]
    # simulate connection going stale (e.g. exceeding) while being in use
    cx1.stale.return_value = True
    cx2 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx2)
    cx1.close.assert_not_called()
    assert cx2 is not cx1
    assert cx2.addr == cx1.addr
    assert cx1 in pool.connections[cx1.addr]
    assert cx2 in pool.connections[cx2.addr]

    pool.release(cx1)
    # now that cx1 is back in the pool and still stale,
    # it should be closed when trying to acquire the next connection
    cx1.close.assert_not_called()

    cx3 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    pool.release(cx3)
    cx1.close.assert_called_once()
    assert cx2 is cx3
    assert cx3.addr == cx1.addr
    assert cx1 not in pool.connections[cx1.addr]
    assert cx3 in pool.connections[cx2.addr]


def test_release_resets_connections(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    cx1.is_reset_mock.return_value = False
    cx1.is_reset_mock.reset_mock()
    pool.release(cx1)
    cx1.is_reset_mock.assert_called_once()
    cx1.reset.assert_called_once()


def test_release_does_not_resets_closed_connections(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    cx1.closed.return_value = True
    cx1.closed.reset_mock()
    cx1.is_reset_mock.reset_mock()
    pool.release(cx1)
    cx1.closed.assert_called_once()
    cx1.is_reset_mock.asset_not_called()
    cx1.reset.asset_not_called()


def test_release_does_not_resets_defunct_connections(opener):
    pool = Neo4jPool(opener, PoolConfig(), WorkspaceConfig(), ROUTER_ADDRESS)
    cx1 = pool.acquire(READ_ACCESS, 30, "test_db", None)
    cx1.defunct.return_value = True
    cx1.defunct.reset_mock()
    cx1.is_reset_mock.reset_mock()
    pool.release(cx1)
    cx1.defunct.assert_called_once()
    cx1.is_reset_mock.asset_not_called()
    cx1.reset.asset_not_called()
